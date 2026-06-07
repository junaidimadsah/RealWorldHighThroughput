using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    public class RealWorldEngine
    {
        private readonly UltraRingBuffer _ringBuffer;
        private readonly LedgerState _ledger;
        private readonly TransactionJournaler _journaler;
        private bool _isRunning;
        private long _processedCount;

        public long ProcessedCount => Volatile.Read(ref _processedCount);

        public RealWorldEngine(int capacity, LedgerState ledger, TransactionJournaler journaler)
        {
            _ringBuffer = new UltraRingBuffer(capacity);
            _ledger = ledger;
            _journaler = journaler;
        }

        public void Start()
        {
            _isRunning = true;
            Thread consumerThread = new Thread(ConsumerLoop)
            {
                IsBackground = true,
                Priority = ThreadPriority.Highest,
                Name = "CoreLedgerProcessor"
            };
            consumerThread.Start();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private void ConsumerLoop()
        {
            var buffer = _ringBuffer;
            const int batchSize = 1024;
            Span<Transaction> batch = stackalloc Transaction[batchSize];
            int batchIndex = 0;

            while (_isRunning)
            {
                while (batchIndex < batchSize && buffer.TryDequeue(out Transaction tx))
                {
                    batch[batchIndex++] = tx;
                }

                if (batchIndex > 0)
                {
                    // Process business validations & balance mutations
                    for (int i = 0; i < batchIndex; i++)
                    {
                        ref readonly var tx = ref batch[i];

                        // Execute state mutation in-memory (Blazing Fast)
                        bool success = _ledger.ProcessTransfer(tx.SourceAccountId, tx.DestinationAccountId, (decimal)tx.Amount);

                        // Hand off to the asynchronous storage journaling queue
                        _journaler.QueueLog(tx.TransactionId, success);
                    }

                    Interlocked.Add(ref _processedCount, batchIndex);
                    batchIndex = 0;
                }
                else
                {
                    Thread.SpinWait(15); // Dynamic backoff spin-wait
                }
            }
        }

        public bool IngestTransaction(in Transaction tx) => _ringBuffer.TryEnqueue(tx);
        public void Stop() => _isRunning = false;
    }
}
