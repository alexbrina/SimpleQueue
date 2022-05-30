using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Concurrent;

namespace SimpleQueue.InMemory
{
    internal class WorkerContext
    {
        public BlockingCollection<Work> Queue { get; private set; }
        public ISimpleQueueWorker Worker { get; private set; }
        public int MaxAttempts { get; private set; }

        public WorkerContext(
            BlockingCollection<Work> queue,
            ISimpleQueueWorker worker,
            int maxAttempts)
        {
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            Worker = worker ?? throw new ArgumentNullException(nameof(worker));
            MaxAttempts = maxAttempts;
        }
    }
}
