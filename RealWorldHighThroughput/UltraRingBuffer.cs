using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    public class UltraRingBuffer
    {
        private readonly Transaction[] _buffer;
        private readonly int _mask;

        // Correct C# way to force fields onto completely different cache lines
        private PaddedSequence _headSequence;
        private PaddedSequence _tailSequence;

        public UltraRingBuffer(int powerOfTwoCapacity)
        {
            if ((powerOfTwoCapacity & (powerOfTwoCapacity - 1)) != 0)
                throw new ArgumentException("Capacity must be a power of two.");

            _buffer = new Transaction[powerOfTwoCapacity];
            _mask = powerOfTwoCapacity - 1;
            _headSequence = new PaddedSequence { Value = 0 };
            _tailSequence = new PaddedSequence { Value = 0 };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnqueue(in Transaction tx)
        {
            long currentTail = Volatile.Read(ref _tailSequence.Value);
            long currentHead = Volatile.Read(ref _headSequence.Value);

            if (currentTail - currentHead >= _buffer.Length)
            {
                return false;
            }

            _buffer[currentTail & _mask] = tx;
            Volatile.Write(ref _tailSequence.Value, currentTail + 1);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out Transaction tx)
        {
            long currentHead = Volatile.Read(ref _headSequence.Value);
            long currentTail = Volatile.Read(ref _tailSequence.Value);

            if (currentHead == currentTail)
            {
                tx = default;
                return false;
            }

            tx = _buffer[currentHead & _mask];
            Volatile.Write(ref _headSequence.Value, currentHead + 1);
            return true;
        }
    }
}
