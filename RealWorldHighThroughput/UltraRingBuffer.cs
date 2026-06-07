using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// A lock-free, MPSC (multi-producer, single-consumer) ring buffer using per-slot
    /// sequence numbers. Each slot carries an explicit sequence state so that producers
    /// atomically claim a unique slot before writing, eliminating the TOCTOU race in the
    /// original tail-only design.
    ///
    /// Slot lifecycle (producer side):
    ///   sequence == slotIndex          → slot is empty and claimable
    ///   sequence == slotIndex + 1      → slot is fully written and ready to consume
    ///
    /// Slot lifecycle (consumer side):
    ///   sequence == slotIndex + 1      → slot is ready; read it
    ///   after reading, write sequence  → slotIndex + capacity  (recycles the slot)
    /// </summary>
    public sealed class UltraRingBuffer
    {
        // Each slot bundles the payload with its own sequence counter so they share a
        // cache line and a single load fetches both.
        private struct Slot
        {
            public long   Sequence;   // explicit slot state (see lifecycle above)
            public Transaction Data;
        }

        private readonly Slot[] _slots;
        private readonly int    _mask;
        private readonly int    _capacity;

        // Producer cursor — multiple threads compete to advance this via CAS.
        // Padded to its own cache line to avoid false sharing with the consumer cursor.
        [ThreadStatic]
        private static long _tsPad; // keeps the field below from sharing a line with _slots header

        private PaddedSequence _producerCursor;
        private PaddedSequence _consumerCursor;

        public UltraRingBuffer(int powerOfTwoCapacity)
        {
            if (powerOfTwoCapacity <= 0 || (powerOfTwoCapacity & (powerOfTwoCapacity - 1)) != 0)
                throw new ArgumentException("Capacity must be a positive power of two.", nameof(powerOfTwoCapacity));

            _capacity = powerOfTwoCapacity;
            _mask     = powerOfTwoCapacity - 1;
            _slots    = new Slot[powerOfTwoCapacity];

            // Pre-initialise every slot sequence to its own index so the first producer
            // to claim slot[0] sees sequence == 0 == slotIndex and proceeds.
            for (int i = 0; i < powerOfTwoCapacity; i++)
                Volatile.Write(ref _slots[i].Sequence, (long)i);

            _producerCursor = new PaddedSequence { Value = 0 };
            _consumerCursor = new PaddedSequence { Value = 0 };
        }

        /// <summary>
        /// Try to enqueue a transaction. Thread-safe for multiple concurrent producers.
        /// Returns false immediately (no spin) when the buffer is full — callers spin
        /// externally so this stays pure non-blocking.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnqueue(in Transaction tx)
        {
            while (true)
            {
                long   pos      = Volatile.Read(ref _producerCursor.Value);
                ref Slot slot   = ref _slots[pos & _mask];
                long   seq      = Volatile.Read(ref slot.Sequence);
                long   diff     = seq - pos;

                if (diff == 0)
                {
                    // Slot is empty and pos is the expected sequence — try to claim it.
                    if (Interlocked.CompareExchange(ref _producerCursor.Value, pos + 1, pos) == pos)
                    {
                        // We own this slot. Write data then publish by advancing sequence.
                        slot.Data = tx;
                        Volatile.Write(ref slot.Sequence, pos + 1);
                        return true;
                    }
                    // Another producer claimed it first — retry the outer loop.
                }
                else if (diff < 0)
                {
                    // diff < 0 means the consumer has not yet recycled this slot from a
                    // previous lap — buffer is full from this producer's perspective.
                    return false;
                }
                // diff > 0: another producer already claimed pos and moved the cursor
                // forward; re-read _producerCursor and retry.
            }
        }

        /// <summary>
        /// Try to dequeue a transaction. Designed for a SINGLE consumer thread.
        /// Returns false when the buffer is empty.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out Transaction tx)
        {
            long pos        = Volatile.Read(ref _consumerCursor.Value);
            ref Slot slot   = ref _slots[pos & _mask];
            long seq        = Volatile.Read(ref slot.Sequence);
            long diff       = seq - (pos + 1);

            if (diff == 0)
            {
                // Slot is ready — read it and recycle.
                tx = slot.Data;
                // Recycle: advance sequence by capacity so the slot is claimable again
                // on the next lap (its slotIndex will equal pos + capacity).
                Volatile.Write(ref slot.Sequence, pos + _capacity);
                Volatile.Write(ref _consumerCursor.Value, pos + 1);
                return true;
            }

            tx = default;
            return false;
        }
    }
}
