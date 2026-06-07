using System.Runtime.InteropServices;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// Wire-format transaction struct. Amount is stored as a fixed-point long in
    /// minor units with 4 decimal places of precision (1 unit = 0.0001 currency).
    /// Example: $150.75 is encoded as 1_507_500.
    ///
    /// Using long instead of double/decimal keeps all arithmetic in native 64-bit
    /// integer instructions — ~10-20x faster than decimal on the hot consumer path.
    /// Struct size and field order are identical to the previous layout so that
    /// existing wire-protocol clients need only change how they encode Amount.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    public struct Transaction
    {
        public long TransactionId;
        public long SourceAccountId;
        public long DestinationAccountId;

        /// <summary>Amount in minor units (×10,000). $1.00 == 10_000.</summary>
        public long Amount;

        public long TimestampTicks;

        // ── Convenience helpers ────────────────────────────────────────────────

        private const long Scale = 10_000L;

        /// <summary>Encode a decimal currency value into minor units.</summary>
        public static long ToMinorUnits(decimal value) => (long)(value * Scale);

        /// <summary>Decode minor units back to a decimal currency value.</summary>
        public static decimal FromMinorUnits(long minorUnits) => (decimal)minorUnits / Scale;
    }
}
