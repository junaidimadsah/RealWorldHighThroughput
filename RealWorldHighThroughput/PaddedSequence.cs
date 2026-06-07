using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    public struct PaddedSequence
    {
        [FieldOffset(0)]
        public long Value;
    }
}
