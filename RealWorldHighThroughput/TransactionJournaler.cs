using System;
using System.Buffers;
using System.Buffers.Text;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RealWorldHighThroughput
{
    /// <summary>
    /// Asynchronous write-ahead logger.
    ///
    /// Hot-path changes vs the original:
    ///  1. QueueLog accepts the transaction's already-captured TimestampTicks instead
    ///     of calling DateTime.UtcNow on every transaction (eliminates a syscall on
    ///     the consumer hot path).
    ///  2. Entry formatting uses Utf8Formatter into a pooled ArrayPool<byte> buffer —
    ///     zero managed string allocations per entry.
    ///  3. The span/formatting work lives in a plain synchronous helper (FormatEntry)
    ///     so it is legal under C# 12 / net8.0, where ref structs are not allowed
    ///     directly inside async state machines (CS9202).
    /// </summary>
    public sealed class TransactionJournaler
    {
        // ── Log entry ──────────────────────────────────────────────────────────
        private readonly struct LogEntry
        {
            public readonly long TransactionId;
            public readonly long TimestampTicks;
            public readonly bool Success;

            public LogEntry(long txId, bool success, long timestampTicks)
            {
                TransactionId  = txId;
                Success        = success;
                TimestampTicks = timestampTicks;
            }
        }

        // ── Fields ─────────────────────────────────────────────────────────────
        private readonly Channel<LogEntry> _channel;
        private readonly string            _logPath = "transaction_wal.log";
        private volatile bool              _isRunning;
        private long                       _droppedCount;

        public long DroppedCount => Volatile.Read(ref _droppedCount);

        // ── Construction ───────────────────────────────────────────────────────
        public TransactionJournaler()
        {
            _channel = Channel.CreateBounded<LogEntry>(new BoundedChannelOptions(500_000)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode     = BoundedChannelFullMode.Wait,
            });
        }

        // ── Public API ─────────────────────────────────────────────────────────
        public void Start()
        {
            _isRunning = true;
            Task.Run(WriteLoopAsync);
        }

        /// <summary>
        /// Enqueue a log entry. Pass <paramref name="timestampTicks"/> from the
        /// transaction struct — avoids a DateTime.UtcNow syscall on this thread.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void QueueLog(long txId, bool success, long timestampTicks)
        {
            if (!_channel.Writer.TryWrite(new LogEntry(txId, success, timestampTicks)))
                Interlocked.Increment(ref _droppedCount);
        }

        public void Stop()
        {
            _isRunning = false;
            _channel.Writer.TryComplete();
        }

        // ── Synchronous formatter — NO async state machine, so Span<T> is legal ──
        // Each entry: "<txId>,<true|false>,<ticks>\n"
        // Max bytes:  20 + 1 + 5 + 1 + 20 + 1 = 48  →  64 gives comfortable headroom.
        private const int EntryMaxBytes = 64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FormatEntry(LogEntry entry, byte[] dest, int offset)
        {
            Span<byte> span    = dest.AsSpan(offset);
            int        written = 0;

            Utf8Formatter.TryFormat(entry.TransactionId, span[written..], out int w);
            written += w;

            span[written++] = (byte)',';

            if (entry.Success)
            {
                span[written++] = (byte)'t';
                span[written++] = (byte)'r';
                span[written++] = (byte)'u';
                span[written++] = (byte)'e';
            }
            else
            {
                span[written++] = (byte)'f';
                span[written++] = (byte)'a';
                span[written++] = (byte)'l';
                span[written++] = (byte)'s';
                span[written++] = (byte)'e';
            }

            span[written++] = (byte)',';

            Utf8Formatter.TryFormat(entry.TimestampTicks, span[written..], out w);
            written += w;

            span[written++] = (byte)'\n';

            return written;
        }

        // ── Write loop ─────────────────────────────────────────────────────────
        private async Task WriteLoopAsync()
        {
            var fileOptions = new FileStreamOptions
            {
                Mode       = FileMode.Create,
                Access     = FileAccess.Write,
                Share      = FileShare.Read,
                BufferSize = 1 << 18, // 256 KiB OS buffer
                Options    = FileOptions.SequentialScan | FileOptions.WriteThrough,
            };

            await using var fileStream = new FileStream(_logPath, fileOptions);

            const int batchSize    = 10_000;
            int       bufferBytes  = batchSize * EntryMaxBytes;
            var       rentedBuffer = ArrayPool<byte>.Shared.Rent(bufferBytes);

            try
            {
                var reader     = _channel.Reader;
                int bufferPos  = 0;
                int batchCount = 0;

                while (await reader.WaitToReadAsync().ConfigureAwait(false) && _isRunning)
                {
                    while (reader.TryRead(out LogEntry entry))
                    {
                        // Flush before the buffer runs out of room for one more entry.
                        if (bufferPos + EntryMaxBytes > rentedBuffer.Length)
                        {
                            // Capture locals for the await — no spans cross the await boundary.
                            int flushLen = bufferPos;
                            await fileStream.WriteAsync(rentedBuffer.AsMemory(0, flushLen))
                                            .ConfigureAwait(false);
                            bufferPos = 0;
                        }

                        // FormatEntry is synchronous — all Span<T> work happens here,
                        // outside the async state machine. This is the CS9202 fix.
                        bufferPos  += FormatEntry(entry, rentedBuffer, bufferPos);
                        batchCount++;

                        if (batchCount >= batchSize)
                        {
                            int flushLen = bufferPos;
                            await fileStream.WriteAsync(rentedBuffer.AsMemory(0, flushLen))
                                            .ConfigureAwait(false);
                            await fileStream.FlushAsync().ConfigureAwait(false);
                            bufferPos  = 0;
                            batchCount = 0;
                        }
                    }
                }

                // Final flush when the channel closes.
                if (bufferPos > 0)
                {
                    await fileStream.WriteAsync(rentedBuffer.AsMemory(0, bufferPos))
                                    .ConfigureAwait(false);
                    await fileStream.FlushAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedBuffer);
            }
        }
    }
}
