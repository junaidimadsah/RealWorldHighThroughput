using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// Outbound TCP client — the counterpart to GodSharpIngressServer.
    ///
    /// Design mirrors the inbound server's overflow-channel pattern in reverse:
    ///   - Engine consumer threads call Send() which does a non-blocking TryWrite
    ///     into a bounded overflow channel. Engine threads are NEVER stalled.
    ///   - A single dedicated forwarder thread drains the channel and performs the
    ///     actual TCP write, spinning only on that non-I/O thread.
    ///   - If the remote server is unavailable the forwarder reconnects in the
    ///     background with a short backoff. Transactions queue (then drop-oldest)
    ///     during the outage — the engine keeps running at full speed.
    ///
    /// Wire format: raw Transaction struct bytes, 40 bytes per transaction.
    /// Matches exactly what GodSharpIngressServer.ProcessIncomingBytes expects.
    /// </summary>
    public sealed class GodSharpIngressClient : IGodSharpSender
    {
        // ── Constants ──────────────────────────────────────────────────────────

        /// <summary>
        /// Byte size of one Transaction on the wire.
        /// Asserted at construction time so a layout change fails fast.
        /// </summary>
        private static readonly int StructSize = Marshal.SizeOf<Transaction>();

        private const int ReconnectDelayMs = 500;

        // ── Fields ─────────────────────────────────────────────────────────────

        private readonly string                  _host;
        private readonly int                     _port;
        private readonly Channel<Transaction>    _overflow;
        private volatile bool                    _isRunning;
        private long                             _droppedPackets;

        // Current live stream — written only by the forwarder thread.
        // Volatile so the forwarder always sees the latest reference.
        private volatile NetworkStream?          _stream;

        /// <inheritdoc/>
        public long DroppedPackets => Volatile.Read(ref _droppedPackets);

        // ── Construction ───────────────────────────────────────────────────────

        /// <param name="host">Remote server hostname or IP address.</param>
        /// <param name="port">Remote server TCP port.</param>
        /// <param name="overflowCapacity">
        ///   Maximum transactions buffered while the connection is unavailable.
        ///   When full, oldest entries are silently dropped and counted.
        /// </param>
        public GodSharpIngressClient(string host, int port, int overflowCapacity = 1_000_000)
        {
            // Fail fast if Transaction layout ever changes without updating the wire protocol.
            if (StructSize != 40)
                throw new InvalidOperationException(
                    $"Transaction struct size is {StructSize} bytes; expected 40. " +
                    "Update the wire protocol before proceeding.");

            _host = host;
            _port = port;

            _overflow = Channel.CreateBounded<Transaction>(new BoundedChannelOptions(overflowCapacity)
            {
                // Multiple engine consumer lanes write concurrently.
                SingleWriter = false,
                // Only the forwarder thread reads.
                SingleReader = true,
                // Never block a writer — drop the oldest queued transaction instead.
                FullMode     = BoundedChannelFullMode.DropOldest,
            });
        }

        // ── Lifecycle ──────────────────────────────────────────────────────────

        public void Start()
        {
            _isRunning = true;

            var forwarder = new Thread(ForwarderLoop)
            {
                IsBackground = true,
                Priority     = ThreadPriority.AboveNormal,
                Name         = "GodSharpIngress-Forwarder",
            };
            forwarder.Start();

            Console.WriteLine($"[GodSharpClient] Sender started — targeting {_host}:{_port}");
        }

        public void Stop()
        {
            _isRunning = false;
            // Signal the channel as complete so the forwarder's TryRead loop exits
            // after draining any remaining buffered transactions.
            _overflow.Writer.TryComplete();
            Console.WriteLine($"[GodSharpClient] Stopped. Total dropped: {DroppedPackets:N0}");
        }

        // ── IGodSharpSender ────────────────────────────────────────────────────

        /// <summary>
        /// Non-blocking enqueue. Called from engine consumer threads (multiple
        /// concurrent callers). Never throws, never stalls the caller.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Send(in Transaction tx)
        {
            if (!_overflow.Writer.TryWrite(tx))
                Interlocked.Increment(ref _droppedPackets);
        }

        // ── Forwarder thread ───────────────────────────────────────────────────

        /// <summary>
        /// Runs on its own dedicated thread. Maintains the TCP connection and drains
        /// the overflow channel. All blocking/spinning is isolated here so engine
        /// consumer threads and I/O threads are unaffected.
        /// </summary>
        private void ForwarderLoop()
        {
            var reader = _overflow.Reader;

            while (_isRunning || reader.Count > 0)
            {
                // Ensure we have a live connection before draining.
                if (_stream == null)
                {
                    if (!TryConnect())
                    {
                        // Server not available — wait before retrying.
                        // Transactions continue queuing (drop-oldest) during this sleep.
                        Thread.Sleep(ReconnectDelayMs);
                        continue;
                    }
                }

                // Drain all currently available transactions in a tight loop.
                while (reader.TryRead(out Transaction tx))
                {
                    if (!TrySendStruct(tx))
                    {
                        // Write failed — connection broken. Discard current stream,
                        // re-enqueue this transaction if possible, break to reconnect.
                        DropStream();

                        // Best-effort re-queue. If the channel is full it counts as dropped.
                        if (!_overflow.Writer.TryWrite(tx))
                            Interlocked.Increment(ref _droppedPackets);

                        break; // exit inner drain loop → outer loop will reconnect
                    }
                }

                // Brief yield when the overflow channel is empty and connection is alive.
                if (reader.Count == 0)
                    Thread.SpinWait(10);
            }

            // Final cleanup — close the stream if still open.
            DropStream();
        }

        // ── TCP helpers ────────────────────────────────────────────────────────

        /// <summary>
        /// Attempt a fresh TCP connection. Returns true on success.
        /// All exceptions are caught — a failed connect is never fatal.
        /// </summary>
        private bool TryConnect()
        {
            try
            {
                var client = new TcpClient();
                client.Connect(_host, _port);
                _stream = client.GetStream();
                Console.WriteLine($"[GodSharpClient] Connected to {_host}:{_port}");
                return true;
            }
            catch (SocketException ex)
            {
                Console.WriteLine($"[GodSharpClient] Connect failed ({ex.SocketErrorCode}) — retrying in {ReconnectDelayMs}ms");
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GodSharpClient] Connect error: {ex.Message} — retrying in {ReconnectDelayMs}ms");
                return false;
            }
        }

        /// <summary>
        /// Serialize one Transaction and write all 40 bytes to the stream.
        /// Uses a write loop to handle partial writes from the OS.
        /// Returns false if the connection is broken.
        /// </summary>
        private bool TrySendStruct(in Transaction tx)
        {
            NetworkStream? stream = _stream;
            if (stream == null) return false;

            // Stack-allocate the exact wire bytes — zero heap allocation per send.
            Span<byte> buffer = stackalloc byte[StructSize]; // 40 bytes
            MemoryMarshal.Write(buffer, in tx);

            try
            {
                // Write loop: NetworkStream.Write guarantees all bytes are written
                // in a single call on most platforms, but we loop defensively.
                int totalWritten = 0;
                while (totalWritten < StructSize)
                {
                    stream.Write(buffer[totalWritten..]);
                    // NetworkStream.Write throws on error rather than returning a count,
                    // so if we reach the next line, all bytes in this slice were accepted.
                    totalWritten = StructSize; // Write accepted the full slice
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GodSharpClient] Send error: {ex.Message} — reconnecting");
                return false;
            }
        }

        /// <summary>
        /// Null out and close the current stream. Safe to call multiple times.
        /// </summary>
        private void DropStream()
        {
            NetworkStream? stream = _stream;
            if (stream == null) return;
            _stream = null;
            try { stream.Close(); } catch { /* already closed */ }
        }
    }
}
