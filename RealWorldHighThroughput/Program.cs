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

            // ── Parse optional host/port from command line ─────────────────────
            // Usage: RealWorldHighThroughput [host] [port]
            // Defaults: 127.0.0.1  9095
            string host = args.Length > 0 ? args[0] : "127.0.0.1";
            int    port = args.Length > 1 && int.TryParse(args[1], out int p) ? p : 9095;

            Console.WriteLine($"    Outbound target       : {host}:{port}");

            // ── Component initialisation ───────────────────────────────────────
            var ledger    = new LedgerState(maxAccounts: 1_000_000);
            var journaler = new TransactionJournaler();

            var engine = new RealWorldEngine(
                laneCapacity : 1_048_576,
                ledger       : ledger,
                journaler    : journaler,
                laneCount    : 0);          // auto-detect from CPU count

            // GodSharpIngressClient replaces GodSharpIngressServer.
            // It forwards every processed transaction to the remote server over TCP.
            // If the server is unavailable it queues and retries — the engine is unaffected.
            var client = new GodSharpIngressClient(host, port);
            engine.SetSender(client);

            // Start order: journaler → client → engine
            // Engine must start last so consumer threads see a fully wired sender.
            journaler.Start();
            client.Start();
            engine.Start();

            // ── Benchmark transaction ──────────────────────────────────────────
            Transaction tx = new Transaction
            {
                SourceAccountId      = 500_021,
                DestinationAccountId = 900_042,
                Amount               = Transaction.ToMinorUnits(150.75m), // 1_507_500 minor units
                TimestampTicks       = DateTime.UtcNow.Ticks,
            };

            const long TotalSubmissions = 10_000_000;

            Console.WriteLine($"    Submitting            : {TotalSubmissions:N0} transactions");
            Console.WriteLine();

            var watch = Stopwatch.StartNew();

            // ── Producer loop ─────────────────────────────────────────────────
            for (long i = 0; i < TotalSubmissions; i++)
            {
                tx.TransactionId  = i;
                tx.TimestampTicks = DateTime.UtcNow.Ticks;

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
            Console.WriteLine($"Client send drops      : {client.DroppedPackets:N0}");
            Console.WriteLine("=======================================================");

            // ── Shutdown order: engine → journaler → client ───────────────────
            // Stop engine first so no new transactions are handed to the client.
            // Stop client last so it can flush any remaining queued transactions.
            engine.Stop();
            journaler.Stop();
            client.Stop();

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
