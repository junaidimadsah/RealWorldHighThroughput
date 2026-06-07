using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    public struct Transaction
    {
        public long TransactionId;
        public long SourceAccountId;
        public long DestinationAccountId;
        public double Amount;
        public long TimestampTicks;
    }
}
