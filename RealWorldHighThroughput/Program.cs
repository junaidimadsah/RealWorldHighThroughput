using System.Diagnostics;

namespace RealWorldHighThroughput
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== Starting High-Throughput Memory Core Pipeline ===");

            // Initialize components
            var ledger = new LedgerState(maxAccounts: 1_000_000);
            var journaler = new TransactionJournaler();
            var engine = new RealWorldEngine(capacity: 4194304, ledger, journaler);

            var server = new GodSharpIngressServer(port: 9095, engine);

            journaler.Start();
            engine.Start();
            server.Start();

            Transaction tx = new Transaction
            {
                SourceAccountId = 500021,
                DestinationAccountId = 900042,
                Amount = 150.75,
                TimestampTicks = DateTime.UtcNow.Ticks
            };

            var watch = Stopwatch.StartNew();
            long totalSubmissions = 10_000_000; // 10 Million Transactions

            // Producer Loop pushing transactions as fast as hardware allows
            for (long i = 0; i < totalSubmissions; i++)
            {
                tx.TransactionId = i;

                // Spin until buffer accepts payload (backpressure handling)
                while (!engine.IngestTransaction(in tx))
                {
                    Thread.SpinWait(5);
                }
            }

            // FIX CS0206: Read the value normally without using the 'ref' keyword on a property
            while (engine.ProcessedCount < totalSubmissions)
            {
                Thread.Sleep(10); // Check every 10ms until the engine flushes the queue
            }

            watch.Stop();
            double totalSeconds = watch.Elapsed.TotalSeconds;

            Console.WriteLine("\n======================= RESULTS =======================");
            Console.WriteLine($"Successfully Processed: {engine.ProcessedCount:N0} Transactions.");
            Console.WriteLine($"Execution Time: {totalSeconds:F4} seconds");
            Console.WriteLine($"Throughput Performance: {engine.ProcessedCount / totalSeconds:N0} TPS");
            Console.WriteLine("=======================================================");

            engine.Stop();
            journaler.Stop();
            server.Stop();

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
