using System;
using System.Runtime.CompilerServices;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// Wraps N independent <see cref="UltraRingBuffer"/> lanes. Each lane is drained
    /// by exactly one dedicated consumer thread, giving true parallel processing.
    ///
    /// Routing key: <c>SourceAccountId % LaneCount</c>
    ///
    /// Routing by source account (not transaction ID) guarantees that all transfers
    /// from the same account always land on the same lane. This preserves per-account
    /// ordering so the ledger never sees an account's balance mutated concurrently
    /// by two consumer threads.
    ///
    /// Note: a transfer between account A (lane 0) and account B (lane 1) is still
    /// safe because LedgerState.ProcessTransfer only reads/writes array slots by
    /// index and we do not hold locks — the only invariant we need is that no two
    /// threads touch the same source account simultaneously, which the routing
    /// guarantees.
    /// </summary>
    public sealed class PartitionedRingBuffer
    {
        public readonly int LaneCount;
        private readonly UltraRingBuffer[] _lanes;
        private readonly int _laneMask; // valid only when LaneCount is a power of two

        /// <param name="laneCount">Number of lanes. Must be a power of two.</param>
        /// <param name="laneCapacity">Per-lane capacity. Must be a power of two.</param>
        public PartitionedRingBuffer(int laneCount, int laneCapacity)
        {
            if (laneCount <= 0 || (laneCount & (laneCount - 1)) != 0)
                throw new ArgumentException("laneCount must be a positive power of two.", nameof(laneCount));

            LaneCount  = laneCount;
            _laneMask  = laneCount - 1;
            _lanes     = new UltraRingBuffer[laneCount];

            for (int i = 0; i < laneCount; i++)
                _lanes[i] = new UltraRingBuffer(laneCapacity);
        }

        /// <summary>
        /// Route the transaction to the lane owned by its source account and attempt
        /// a non-blocking enqueue. Returns false when that lane is full.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnqueue(in Transaction tx)
        {
            int lane = (int)(tx.SourceAccountId & _laneMask);
            return _lanes[lane].TryEnqueue(in tx);
        }

        /// <summary>Returns the ring buffer for a specific lane (used by consumer threads).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public UltraRingBuffer GetLane(int index) => _lanes[index];
    }
}
