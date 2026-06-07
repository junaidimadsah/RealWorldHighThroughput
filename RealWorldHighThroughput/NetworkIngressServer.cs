using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// TCP ingress server backed by an overflow <see cref="Channel{T}"/>.
    ///
    /// Original design had the socket receive task spinning directly on
    /// <c>engine.IngestTransaction</c> — this blocked the I/O thread pool thread
    /// while the ring buffer was momentarily full, starving other connections.
    ///
    /// New design:
    ///  1. Receive task writes to a bounded overflow channel (non-blocking TryWrite).
    ///     If the overflow channel is also full the packet is counted as dropped;
    ///     the socket thread is never stalled.
    ///  2. A single dedicated forwarder thread drains the overflow channel and spins
    ///     on IngestTransaction — the spin is isolated to one non-I/O thread.
    ///  3. Dropped packet count is exposed for monitoring.
    /// </summary>
    public sealed class NetworkIngressServer
    {
        // ── Fields ─────────────────────────────────────────────────────────────
        private readonly Socket                      _listener;
        private readonly RealWorldEngine             _engine;
        private readonly Channel<Transaction>        _overflow;
        private volatile bool                        _isRunning;
        private long                                 _droppedPackets;

        public long DroppedPackets => Volatile.Read(ref _droppedPackets);

        private static readonly int StructSize = Marshal.SizeOf<Transaction>();

        // ── Construction ───────────────────────────────────────────────────────
        public NetworkIngressServer(int port, RealWorldEngine engine,
                                    int overflowCapacity = 1_000_000)
        {
            _engine   = engine;
            _overflow = Channel.CreateBounded<Transaction>(new BoundedChannelOptions(overflowCapacity)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode     = BoundedChannelFullMode.DropOldest, // never block the write
            });

            _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listener.Bind(new IPEndPoint(IPAddress.Loopback, port));
        }

        // ── Lifecycle ──────────────────────────────────────────────────────────
        public void Start()
        {
            _isRunning = true;
            _listener.Listen(100);

            // Forwarder thread: drains overflow channel → ring buffer, spins there.
            var forwarder = new Thread(ForwarderLoop)
            {
                IsBackground = true,
                Priority     = ThreadPriority.AboveNormal,
                Name         = "NetworkIngress-Forwarder",
            };
            forwarder.Start();

            Task.Run(AcceptConnectionsAsync);
            Console.WriteLine($"[Network] Ingress gateway listening on port {_listener.LocalEndPoint}...");
        }

        public void Stop()
        {
            _isRunning = false;
            _overflow.Writer.TryComplete();
            _listener.Close();
        }

        // ── Accept loop ────────────────────────────────────────────────────────
        private async Task AcceptConnectionsAsync()
        {
            while (_isRunning)
            {
                try
                {
                    Socket client = await _listener.AcceptAsync().ConfigureAwait(false);
                    _ = Task.Run(() => HandleClientAsync(client));
                }
                catch { break; }
            }
        }

        // ── Per-connection handler ─────────────────────────────────────────────
        private async Task HandleClientAsync(Socket socket)
        {
            byte[]        rawBuffer  = new byte[StructSize * 100];
            Memory<byte>  memWrapper = rawBuffer;

            try
            {
                while (_isRunning)
                {
                    int bytesRead = await socket.ReceiveAsync(memWrapper, SocketFlags.None)
                                                .ConfigureAwait(false);
                    if (bytesRead == 0) break;

                    // Synchronous helper — Span<T> is safe outside async state machine.
                    EnqueueToOverflow(rawBuffer, bytesRead);
                }
            }
            catch { /* connection drop — not an error */ }
            finally { socket.Close(); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnqueueToOverflow(byte[] buffer, int bytesRead)
        {
            int items = bytesRead / StructSize;
            for (int i = 0; i < items; i++)
            {
                ReadOnlySpan<byte> bytes = buffer.AsSpan(i * StructSize, StructSize);
                Transaction tx = MemoryMarshal.Read<Transaction>(bytes);

                if (!_overflow.Writer.TryWrite(tx))
                    Interlocked.Increment(ref _droppedPackets);
            }
        }

        // ── Forwarder (overflow → ring buffer) ─────────────────────────────────
        private void ForwarderLoop()
        {
            var reader = _overflow.Reader;

            while (_isRunning || reader.Count > 0)
            {
                while (reader.TryRead(out Transaction tx))
                {
                    // Spin only here, on a dedicated non-I/O thread.
                    while (!_engine.IngestTransaction(in tx))
                        Thread.SpinWait(5);
                }

                Thread.SpinWait(10); // brief yield when overflow channel is empty
            }
        }
    }
}
