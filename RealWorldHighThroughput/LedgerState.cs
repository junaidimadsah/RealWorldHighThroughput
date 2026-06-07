using System.Runtime.CompilerServices;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// In-memory ledger. Balances are stored as fixed-point longs in the same minor-unit
    /// scale as <see cref="Transaction.Amount"/> (×10,000 — i.e. $1.00 == 10_000).
    ///
    /// Replacing decimal with long means every balance mutation is a native 64-bit
    /// integer subtract/add — no software-emulated decimal arithmetic on the hot path.
    /// </summary>
    public sealed class LedgerState
    {
        private readonly long[] _balances;

        // $10,000,000.00 expressed in minor units (×10,000)
        private const long InitialBalance = 10_000_000_00_00L; // 100_000_000_000

        public LedgerState(int maxAccounts)
        {
            _balances = new long[maxAccounts];
            for (int i = 0; i < maxAccounts; i++)
                _balances[i] = InitialBalance;
        }

        /// <summary>
        /// Apply a transfer. Both srcAcc and destAcc must be valid indices.
        /// Returns false (insufficient funds) without mutating state when the source
        /// balance would go negative.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProcessTransfer(long srcAcc, long destAcc, long amountMinorUnits)
        {
            if (_balances[srcAcc] < amountMinorUnits)
                return false; // Insufficient funds — no mutation

            _balances[srcAcc]  -= amountMinorUnits;
            _balances[destAcc] += amountMinorUnits;
            return true;
        }

        /// <summary>Returns the balance in minor units.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetBalance(long accountId) => _balances[accountId];

        /// <summary>Returns the balance as a human-readable decimal value.</summary>
        public decimal GetBalanceDecimal(long accountId) =>
            Transaction.FromMinorUnits(_balances[accountId]);
    }
}
