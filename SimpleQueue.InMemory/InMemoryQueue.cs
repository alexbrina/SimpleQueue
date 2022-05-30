using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleQueue.InMemory
{
    public sealed class InMemoryQueue : ISimpleQueue
    {
        private readonly IServiceProvider serviceProvider;
        private readonly BlockingCollection<Work> queue;
        private readonly ILogger<InMemoryQueue> logger;
        private int workers = 0;
        private int requeuing = 0;

        public InMemoryQueue(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider
                ?? throw new ArgumentNullException(nameof(serviceProvider));

            logger = serviceProvider.GetRequiredService<ILogger<InMemoryQueue>>();
            queue = new BlockingCollection<Work>();
        }

        public void Add(Work work) => queue.Add(work);

        public void Requeue(IEnumerable<Work> works)
        {
            // if already requeuing returns
            if (1 == Interlocked.Exchange(ref requeuing, 1))
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("Requeue operation already in progress");

                return;
            }

            var added = 0;
            foreach (var work in works)
            {
                // we check if work is already in queue but we won't bother
                // to prevent that some pending work was done between
                // retrieving and requeuing, this should be checked in workers
                // before work execution
                if (!queue.Any(i => i.Id == work.Id))
                {
                    queue.Add(work);
                    added++;
                }
            }

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Requeue operation put {added} pending works" +
                    " back in queue");

            // release lock
            Interlocked.Exchange(ref requeuing, 0);
        }

        public void Consume(ISimpleQueueWorker worker, int maxAttempts,
            CancellationToken cancellationToken)
        {
            var id = Interlocked.Increment(ref workers);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"{nameof(InMemoryQueue)} is starting worker {id}.");

            // one separated task for each consumer
            Task.Factory.StartNew(async () =>
            {
                await serviceProvider.GetRequiredService<Consumer>()
                    .Execute(id, new WorkerContext(queue, worker, maxAttempts), cancellationToken);
            }, cancellationToken);
        }

        public void Consume(ISimpleQueueWorker worker, int maxAttempts, int workerReplicas,
            CancellationToken cancellationToken)
        {
            for (int i = 0; i < workerReplicas; i++)
            {
                Consume(worker, maxAttempts, cancellationToken);
            }
        }

        public int Count => queue.Count;

        public void Dispose()
        {
            queue?.Dispose();
        }
    }
}
