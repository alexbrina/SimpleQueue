using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Concurrent;

namespace SimpleQueue.InMemory
{
    internal class ConsumerContext<T>
        where T : ISimpleQueueWorker
    {
        public BlockingCollection<Work> Queue { get; private set; }
        public int MaxAttempts { get; private set; }
        public int ConsumerId { get; private set; }
        public Action<T> ConfigureWorker { get; private set; }

        public ConsumerContext(
            BlockingCollection<Work> queue,
            int maxAttempts,
            int consumerId,
            Action<T> configureWorker)
        {
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            MaxAttempts = maxAttempts;
            ConsumerId = consumerId;
            ConfigureWorker = configureWorker;
        }
    }
}
