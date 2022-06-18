using Microsoft.Extensions.DependencyInjection;
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
            IServiceScope scope = null,
            CancellationToken cancellationToken = default)
            where T : ISimpleQueueWorker;

        /// <summary>
        /// Works in queue
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Returns true if there is at least one worker processing a work
        /// </summary>
        bool IsWorking { get; }
    }
}
