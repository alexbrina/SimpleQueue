using Microsoft.Extensions.DependencyInjection;
using SimpleQueue.Abstractions;
using System;

namespace SimpleQueue.InMemory
{
    internal class ConsumerContext<T>
        where T : ISimpleQueueWorker
    {
        public InMemoryQueue Queue { get; private set; }
        public int MaxAttempts { get; private set; }
        public int ConsumerId { get; private set; }
        public IServiceScope Scope { get; private set; }

        public ConsumerContext(
            InMemoryQueue queue,
            int maxAttempts,
            int consumerId,
            IServiceScope scope)
        {
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            MaxAttempts = maxAttempts;
            ConsumerId = consumerId;
            Scope = scope;
        }
    }
}
