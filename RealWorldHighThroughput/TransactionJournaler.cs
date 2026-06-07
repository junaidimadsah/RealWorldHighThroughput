using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    public class TransactionJournaler
    {
        private readonly Channel<string> _logChannel;
        private readonly string _logPath = "transaction_wal.log";
        private bool _isRunning;

        public TransactionJournaler()
        {
            _logChannel = Channel.CreateBounded<string>(new BoundedChannelOptions(500_000)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        public void Start()
        {
            _isRunning = true;
            Task.Run(WriteLoopAsync);
        }

        public void QueueLog(long txId, bool success)
        {
            _logChannel.Writer.TryWrite($"{txId},{success},{DateTime.UtcNow.Ticks}\n");
        }

        private async Task WriteLoopAsync()
        {
            // Create options structure explicitly to satisfy the compiler signature rules
            var fileOptions = new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                Share = FileShare.Read,
                BufferSize = 262144,
                Options = FileOptions.SequentialScan | FileOptions.WriteThrough
            };

            using var fileStream = new FileStream(_logPath, fileOptions);
            using var writer = new StreamWriter(fileStream, Encoding.UTF8);

            var reader = _logChannel.Reader;
            int batchCount = 0;

            while (await reader.WaitToReadAsync() && _isRunning)
            {
                while (reader.TryRead(out var logLine))
                {
                    await writer.WriteAsync(logLine);
                    batchCount++;

                    if (batchCount >= 10000)
                    {
                        await writer.FlushAsync();
                        batchCount = 0;
                    }
                }
            }
        }

        public void Stop() => _isRunning = false;
    }
}
