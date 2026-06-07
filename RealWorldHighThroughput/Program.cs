using System;
using System.Diagnostics;
using System.Threading;

namespace RealWorldHighThroughput
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== Starting High-Throughput Memory Core Pipeline ===");
            Console.WriteLine($"    Logical CPUs detected : {Environment.ProcessorCount}");

            // ── Component initialisation ───────────────────────────────────────
            var ledger    = new LedgerState(maxAccounts: 1_000_000);
            var journaler = new TransactionJournaler();

            // laneCapacity: total ring buffer capacity spread across N lanes.
            // With 4 lanes each gets 1M slots; with 8 lanes each gets 512K slots.
            // Using 0 for laneCount lets the engine auto-detect from CPU count.
            var engine = new RealWorldEngine(
                laneCapacity : 1_048_576,   // 1M slots per lane (power of two)
                ledger       : ledger,
                journaler    : journaler,
                laneCount    : 0);          // auto-detect

            var server = new GodSharpIngressServer(port: 9095, engine);

            journaler.Start();
            engine.Start();
            server.Start();

            // ── Benchmark transaction ──────────────────────────────────────────
            // Amount: $150.75 encoded as minor units (×10,000).
            Transaction tx = new Transaction
            {
                SourceAccountId      = 500_021,
                DestinationAccountId = 900_042,
                Amount               = Transaction.ToMinorUnits(150.75m), // 1_507_500
                TimestampTicks       = DateTime.UtcNow.Ticks,
            };

            const long TotalSubmissions = 10_000_000;

            Console.WriteLine($"    Submitting            : {TotalSubmissions:N0} transactions");
            Console.WriteLine($"    Engine lanes          : {engine /* expose via property if needed */}");
            Console.WriteLine();

            var watch = Stopwatch.StartNew();

            // ── Producer loop ─────────────────────────────────────────────────
            for (long i = 0; i < TotalSubmissions; i++)
            {
                tx.TransactionId  = i;
                tx.TimestampTicks = DateTime.UtcNow.Ticks; // update per-tx timestamp

                while (!engine.IngestTransaction(in tx))
                    Thread.SpinWait(5);
            }

            // ── Wait for consumer threads to drain ────────────────────────────
            while (engine.ProcessedCount < TotalSubmissions)
                Thread.Sleep(10);

            watch.Stop();
            double seconds = watch.Elapsed.TotalSeconds;

            // ── Results ───────────────────────────────────────────────────────
            Console.WriteLine("======================= RESULTS =======================");
            Console.WriteLine($"Successfully Processed : {engine.ProcessedCount:N0} transactions");
            Console.WriteLine($"Execution Time         : {seconds:F4} seconds");
            Console.WriteLine($"Throughput             : {engine.ProcessedCount / seconds:N0} TPS");
            Console.WriteLine($"Journaler drops        : {journaler.DroppedCount:N0}");
            Console.WriteLine($"Network drops          : {server.DroppedPackets:N0}");
            Console.WriteLine("=======================================================");

            engine.Stop();
            journaler.Stop();
            server.Stop();

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
