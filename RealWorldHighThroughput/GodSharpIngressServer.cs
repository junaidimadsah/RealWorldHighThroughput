using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using GodSharp.Sockets;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// GodSharp TCP ingress server — overflow-channel variant.
    ///
    /// The original design spun directly in the OnReceived event handler
    /// (a GodSharp I/O callback thread) whenever the ring buffer was full.
    /// This stalled all other connections sharing that I/O thread.
    ///
    /// New design mirrors <see cref="NetworkIngressServer"/>:
    ///  - OnReceived handler writes to a bounded overflow channel (non-blocking).
    ///  - A dedicated forwarder thread drains the channel and spins on IngestTransaction.
    /// </summary>
    public sealed class GodSharpIngressServer
    {
        // ── Fields ─────────────────────────────────────────────────────────────
        private readonly TcpServer                   _server;
        private readonly RealWorldEngine             _engine;
        private readonly Channel<Transaction>        _overflow;
        private volatile bool                        _isRunning;
        private long                                 _droppedPackets;

        public long DroppedPackets => Volatile.Read(ref _droppedPackets);

        private static readonly int StructSize = Marshal.SizeOf<Transaction>();

        // ── Construction ───────────────────────────────────────────────────────
        public GodSharpIngressServer(int port, RealWorldEngine engine,
                                     int overflowCapacity = 1_000_000)
        {
            _engine   = engine;
            _overflow = Channel.CreateBounded<Transaction>(new BoundedChannelOptions(overflowCapacity)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode     = BoundedChannelFullMode.DropOldest,
            });

            _server = new TcpServer(port);

            _server.OnReceived += (e) =>
            {
                if (e?.Buffers == null || e.Buffers.Length == 0) return;
                EnqueueToOverflow(e.Buffers, 0, e.Buffers.Length);
            };

            _server.OnStarted += (_) =>
                Console.WriteLine($"[GodSharp] Server started on port {port}.");

            _server.OnException += (_) =>
                Console.WriteLine("[GodSharp] Transport exception or connection drop.");
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

            try { _server.Start(); }
            catch (Exception ex)
            {
                Console.WriteLine($"[GodSharp] Startup aborted: {ex.Message}");
            }
        }

        public void Stop()
        {
            _isRunning = false;
            _overflow.Writer.TryComplete();
            _server.Stop();
        }

        // ── Overflow enqueue (called on GodSharp I/O thread) ──────────────────
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnqueueToOverflow(byte[] buffer, int offset, int count)
        {
            int items = count / StructSize;
            for (int i = 0; i < items; i++)
            {
                ReadOnlySpan<byte> bytes = buffer.AsSpan(offset + (i * StructSize), StructSize);
                Transaction tx = MemoryMarshal.Read<Transaction>(bytes);

                if (!_overflow.Writer.TryWrite(tx))
                    Interlocked.Increment(ref _droppedPackets);
            }
        }

        // ── Forwarder (overflow → ring buffer, dedicated thread) ──────────────
        private void ForwarderLoop()
        {
            var reader = _overflow.Reader;

            while (_isRunning || reader.Count > 0)
            {
                while (reader.TryRead(out Transaction tx))
                {
                    while (!_engine.IngestTransaction(in tx))
                        Thread.SpinWait(5);
                }

                Thread.SpinWait(10);
            }
        }
    }
}
