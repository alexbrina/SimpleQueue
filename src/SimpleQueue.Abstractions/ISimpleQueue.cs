using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Generic;
using System.Threading;

namespace SimpleQueue.Abstractions
{
    public interface ISimpleQueue : IDisposable
    {
        void Add(Work work);

        void Requeue(IEnumerable<Work> works);

        void Consume(ISimpleQueueWorker worker, int maxAttempts,
            CancellationToken cancellationToken);

        void Consume(ISimpleQueueWorker worker, int maxAttempts, int workerReplicas,
            CancellationToken cancellationToken);

        int Count { get; }
    }
}
