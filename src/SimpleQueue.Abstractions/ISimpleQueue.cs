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

        void Consume<T>(
            int maxAttempts,
            Action<T> configureWorker,
            CancellationToken cancellationToken)
            where T : ISimpleQueueWorker;

        int Count { get; }
    }
}
