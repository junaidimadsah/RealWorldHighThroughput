using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// Core processing engine.
    ///
    /// Changes from original:
    ///  1. Uses <see cref="PartitionedRingBuffer"/> with N lanes (default: logical CPU
    ///     count, capped at 16) so transactions are processed in parallel.
    ///  2. Each lane runs a dedicated consumer thread pinned to high priority.
    ///  3. LedgerState.ProcessTransfer now receives a long (minor units) instead of
    ///     a decimal cast from double — eliminates the cast on the hot path.
    ///  4. TimestampTicks is forwarded to QueueLog instead of calling DateTime.UtcNow.
    /// </summary>
    public sealed class RealWorldEngine
    {
        // ── Configuration ──────────────────────────────────────────────────────
        // Lane count defaults to the number of physical/logical processors, capped
        // so we never spawn more threads than there are cores to run them.
        private static int DefaultLaneCount()
        {
            int cores = Environment.ProcessorCount;
            // Round down to next power of two so the lane mask trick works.
            int lanes = 1;
            while (lanes * 2 <= cores && lanes < 16) lanes *= 2;
            return lanes;
        }

        private const int BatchSize = 1024;

        // ── Fields ─────────────────────────────────────────────────────────────
        private readonly PartitionedRingBuffer _partitioned;
        private readonly LedgerState           _ledger;
        private readonly TransactionJournaler  _journaler;
        private readonly int                   _laneCount;
        private volatile bool                  _isRunning;
        private long                           _processedCount;

        public long ProcessedCount => Volatile.Read(ref _processedCount);

        // ── Construction ───────────────────────────────────────────────────────
        /// <param name="laneCapacity">Per-lane ring buffer size (power of two).</param>
        /// <param name="laneCount">
        ///   Number of consumer threads / ring-buffer lanes.
        ///   Pass 0 to auto-detect from CPU count.
        /// </param>
        public RealWorldEngine(int laneCapacity, LedgerState ledger, TransactionJournaler journaler,
                               int laneCount = 0)
        {
            _laneCount   = laneCount > 0 ? laneCount : DefaultLaneCount();
            _ledger      = ledger;
            _journaler   = journaler;
            _partitioned = new PartitionedRingBuffer(_laneCount, laneCapacity);
        }

        // ── Lifecycle ──────────────────────────────────────────────────────────
        public void Start()
        {
            _isRunning = true;
            for (int i = 0; i < _laneCount; i++)
            {
                int laneIndex = i; // capture for closure
                var thread = new Thread(() => ConsumerLoop(laneIndex))
                {
                    IsBackground = true,
                    Priority     = ThreadPriority.Highest,
                    Name         = $"LedgerProcessor-Lane{laneIndex}",
                };
                thread.Start();
            }
        }

        public void Stop() => _isRunning = false;

        // ── Hot path ───────────────────────────────────────────────────────────

        /// <summary>
        /// Ingest a transaction from any producer thread (network ingress, benchmark loop, etc.).
        /// Routes to the correct lane via SourceAccountId and returns false if that lane is full.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IngestTransaction(in Transaction tx) => _partitioned.TryEnqueue(in tx);

        // ── Consumer loop (one instance per lane) ──────────────────────────────
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private void ConsumerLoop(int laneIndex)
        {
            UltraRingBuffer lane = _partitioned.GetLane(laneIndex);

            // Stack-allocated batch buffer — lives on the thread stack, no GC.
            Span<Transaction> batch = stackalloc Transaction[BatchSize];
            int batchIndex = 0;

            while (_isRunning)
            {
                // Drain up to BatchSize items from this lane's ring buffer.
                while (batchIndex < BatchSize && lane.TryDequeue(out Transaction tx))
                    batch[batchIndex++] = tx;

                if (batchIndex > 0)
                {
                    for (int i = 0; i < batchIndex; i++)
                    {
                        ref readonly Transaction tx = ref batch[i];

                        // Amount is already in minor units (long) — no cast needed.
                        bool success = _ledger.ProcessTransfer(
                            tx.SourceAccountId,
                            tx.DestinationAccountId,
                            tx.Amount);

                        // Pass the transaction's own timestamp — no syscall here.
                        _journaler.QueueLog(tx.TransactionId, success, tx.TimestampTicks);
                    }

                    Interlocked.Add(ref _processedCount, batchIndex);
                    batchIndex = 0;
                }
                else
                {
                    // Brief spin-wait before re-checking — avoids burning a full core
                    // while keeping latency low when new work arrives.
                    Thread.SpinWait(15);
                }
            }
        }
    }
}
