using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    public class NetworkIngressServer
    {
        private readonly Socket _listenerSocket;
        private readonly RealWorldEngine _engine;
        private bool _isRunning;

        public NetworkIngressServer(int port, RealWorldEngine engine)
        {
            _engine = engine;

            // 1. Initialize the socket object instance
            _listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            // 2. Bind strictly to Loopback (localhost) on the designated port
            _listenerSocket.Bind(new IPEndPoint(IPAddress.Loopback, port));
        }

        public void Start()
        {
            _isRunning = true;
            _listenerSocket.Listen(100);
            Task.Run(AcceptConnectionsAsync);
            Console.WriteLine("Ingress Network Gateway Listening on Port 8080...");
        }

        private async Task AcceptConnectionsAsync()
        {
            while (_isRunning)
            {
                try
                {
                    Socket clientSocket = await _listenerSocket.AcceptAsync();
                    _ = Task.Run(() => HandleClientTrafficAsync(clientSocket));
                }
                catch { break; }
            }
        }

        private async Task HandleClientTrafficAsync(Socket socket)
        {
            byte[] memoryBuffer = new byte[Marshal.SizeOf<Transaction>() * 100];
            Memory<byte> memoryWrapper = memoryBuffer;

            try
            {
                while (_isRunning)
                {
                    // Async state machine operates comfortably here
                    int bytesRead = await socket.ReceiveAsync(memoryWrapper, SocketFlags.None);
                    if (bytesRead == 0) break;

                    // Pass to a synchronous method where Span<T> and unsafe casting are fully permitted
                    ProcessIncomingBytes(memoryBuffer, bytesRead);
                }
            }
            catch { }
            finally { socket.Close(); }
        }

        // A regular method has no async state machine, meaning we can use ref structs safely
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessIncomingBytes(byte[] buffer, int bytesRead)
        {
            int structSize = Marshal.SizeOf<Transaction>();
            int itemsCount = bytesRead / structSize;

            for (int i = 0; i < itemsCount; i++)
            {
                // This is now perfectly legal in C# 12
                ReadOnlySpan<byte> structBytes = buffer.AsSpan(i * structSize, structSize);
                Transaction tx = MemoryMarshal.Read<Transaction>(structBytes);

                // Spin fast on the thread pool thread if the ring buffer is temporarily full
                while (!_engine.IngestTransaction(tx))
                {
                    System.Threading.Thread.SpinWait(5);
                }
            }
        }
    }
}
