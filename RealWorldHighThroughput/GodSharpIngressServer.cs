using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using GodSharp.Sockets;

namespace RealWorldHighThroughput
{
    public class GodSharpIngressServer
    {
        private readonly TcpServer _server;
        private readonly RealWorldEngine _engine;

        public GodSharpIngressServer(int port, RealWorldEngine engine)
        {
            _engine = engine;

            // Initialize the GodSharp TcpServer instance
            _server = new TcpServer(port);

            _server.OnReceived += (e) =>
            {
                if (e == null || e.Buffers == null || e.Buffers.Length == 0) return;

                // Process using the event's internal buffer array
                ProcessIncomingBytes(e.Buffers, 0, e.Buffers.Length);
            };

            _server.OnStarted += (e) =>
            {
                Console.WriteLine($"[GodSharp] Server started listening cleanly on port {port}...");
            };

            _server.OnException += (e) =>
            {
                Console.WriteLine("[GodSharp] Transport layer exception or connection drop encountered.");
            };
        }

        public void Start()
        {
            try
            {
                _server.Start();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GodSharp] Startup aborted by OS: {ex.Message}");
            }
        }

        public void Stop()
        {
            _server.Stop();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessIncomingBytes(byte[] buffer, int offset, int count)
        {
            int structSize = Marshal.SizeOf<Transaction>();
            int itemsCount = count / structSize;

            for (int i = 0; i < itemsCount; i++)
            {
                ReadOnlySpan<byte> structBytes = buffer.AsSpan(offset + (i * structSize), structSize);
                Transaction tx = MemoryMarshal.Read<Transaction>(structBytes);

                while (!_engine.IngestTransaction(tx))
                {
                    System.Threading.Thread.SpinWait(5);
                }
            }
        }
    }
}
