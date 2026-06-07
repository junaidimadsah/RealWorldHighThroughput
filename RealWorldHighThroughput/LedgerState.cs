using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    public class LedgerState
    {
        // Pre-allocate arrays for O(1) index-based lookups to avoid dictionary collisions
        private readonly decimal[] _balances;

        public LedgerState(int maxAccounts)
        {
            _balances = new decimal[maxAccounts];
            // Initialize dummy accounts with starting balances
            for (int i = 0; i < maxAccounts; i++)
            {
                _balances[i] = 10_000_000.00m;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProcessTransfer(long srcAcc, long destAcc, decimal amount)
        {
            // Real-world business logic check
            if (_balances[srcAcc] < amount)
                return false; // Insufficient Funds

            _balances[srcAcc] -= amount;
            _balances[destAcc] += amount;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public decimal GetBalance(long accountId) => _balances[accountId];
    }
}
