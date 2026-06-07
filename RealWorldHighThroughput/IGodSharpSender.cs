namespace RealWorldHighThroughput
{
    /// <summary>
    /// Abstraction over any outbound transaction sender.
    /// RealWorldEngine depends on this interface — not on the concrete TCP client —
    /// so the sender can be swapped or mocked in tests without touching the engine.
    /// </summary>
    public interface IGodSharpSender
    {
        /// <summary>
        /// Enqueue a transaction for outbound delivery. Must be non-blocking and
        /// thread-safe: multiple engine consumer threads call this concurrently.
        /// Implementations should never throw; drop and count instead.
        /// </summary>
        void Send(in Transaction tx);

        /// <summary>Number of transactions dropped due to overflow or disconnection.</summary>
        long DroppedPackets { get; }
    }
}
