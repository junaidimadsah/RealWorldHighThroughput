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
    ///  5. Optional IGodSharpSender hook: after journaling, each processed transaction
    ///     is forwarded to the sender (e.g. GodSharpIngressClient). The hook costs one
    ///     null check per transaction on the hot path when no sender is configured.
    /// </summary>
    public sealed class RealWorldEngine
    {
        // ── Configuration ──────────────────────────────────────────────────────
        private static int DefaultLaneCount()
        {
            int cores = Environment.ProcessorCount;
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

        // Optional outbound sender. Null = no forwarding (zero overhead beyond a
        // null check). Set via SetSender() before calling Start().
        private IGodSharpSender?               _sender;

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

        // ── Sender wiring ──────────────────────────────────────────────────────

        /// <summary>
        /// Attach an outbound sender. Must be called before <see cref="Start"/>.
        /// Passing null removes any previously attached sender.
        /// </summary>
        public void SetSender(IGodSharpSender? sender) => _sender = sender;

        // ── Lifecycle ──────────────────────────────────────────────────────────
        public void Start()
        {
            _isRunning = true;
            for (int i = 0; i < _laneCount; i++)
            {
                int laneIndex = i;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IngestTransaction(in Transaction tx) => _partitioned.TryEnqueue(in tx);

        // ── Consumer loop (one instance per lane) ──────────────────────────────
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private void ConsumerLoop(int laneIndex)
        {
            UltraRingBuffer   lane   = _partitioned.GetLane(laneIndex);
            IGodSharpSender?  sender = _sender; // local copy — avoids volatile read per tx

            Span<Transaction> batch      = stackalloc Transaction[BatchSize];
            int               batchIndex = 0;

            while (_isRunning)
            {
                while (batchIndex < BatchSize && lane.TryDequeue(out Transaction tx))
                    batch[batchIndex++] = tx;

                if (batchIndex > 0)
                {
                    for (int i = 0; i < batchIndex; i++)
                    {
                        ref readonly Transaction tx = ref batch[i];

                        bool success = _ledger.ProcessTransfer(
                            tx.SourceAccountId,
                            tx.DestinationAccountId,
                            tx.Amount);

                        _journaler.QueueLog(tx.TransactionId, success, tx.TimestampTicks);

                        // Forward to outbound sender if one is configured.
                        // One null check per transaction — negligible cost.
                        sender?.Send(in tx);
                    }

                    Interlocked.Add(ref _processedCount, batchIndex);
                    batchIndex = 0;
                }
                else
                {
                    Thread.SpinWait(15);
                }
            }
        }
    }
}
