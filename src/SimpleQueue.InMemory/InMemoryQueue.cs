using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace SimpleQueue.InMemory
{
    public sealed class InMemoryQueue : ISimpleQueue, IDisposable
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ILogger<InMemoryQueue> logger;
        private readonly BlockingCollection<Work> queue;

        private int consumers = 0;
        private int requeuing = 0;
        private int working = 0;

        public InMemoryQueue(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider
                ?? throw new ArgumentNullException(nameof(serviceProvider));

            logger = serviceProvider.GetRequiredService<ILogger<InMemoryQueue>>();

            queue = new BlockingCollection<Work>();
        }

        public void Add(Work work) => queue.Add(work);

        /// <inheritdoc />
        /// <remarks>
        /// We check if work is already in queue before requeuing.
        /// Notice that this doesn't prevent all duplications cenarios, some
        /// pending work may not be in queue anymore but is still being
        /// processed. When required, workers should implement deduplication
        /// logic before work processing.
        /// </remarks>
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

        /// <summary>
        /// Starts an independent consuming thread each time this method is called
        /// </summary>
        /// <typeparam name="T">Worker type</typeparam>
        /// <param name="maxAttempts">Max attempts a work will be attempted before discarded</param>
        /// <param name="scope">Injected scope where workers will be instantiated.<br />
        /// This parameter is rather relevant for workers dependencies, as it provides
        /// a way to externally controll them.<br />
        /// Workers themselves should probably be registered with transient lifetime.<br />
        /// If not provided, each work processing will have its own scope.<br />
        /// For independent instances of dependencies for each worker, register
        /// dependencies as transient or scoped and pass a distinct scope for
        /// each consumer, these dependencies will live as long as its scope is
        /// valid.
        /// </param>
        /// <param name="cancellationToken">Allows consumer cancellation</param>
        public void Consume<T>(
            int maxAttempts,
            IServiceScope scope = null,
            CancellationToken cancellationToken = default)
            where T : ISimpleQueueWorker
        {
            var id = Interlocked.Increment(ref consumers);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"{nameof(InMemoryQueue)} is starting consumer {id}.");

            // We start a dedicated background thread for each consumer.
            var thread = new Thread(async () =>
            {
                try
                {
                    var consumer = serviceProvider.GetRequiredService<Consumer>();
                    var context = new ConsumerContext<T>(this, maxAttempts, id, scope);
                    await consumer.Execute<T>(context, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // if cancelled we simply log and let this thread end quietly
                    if (logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug($"Consumer {id} operation canceled");
                }
                catch (Exception ex)
                {
                    // exception handling is made inside Consumer
                    // this captures exception if Consumer handling fails
                    logger.LogError(ex, $"Consumer {id} stopped due to unhandled exception.");

                    // note that consumer dedicated thread dies if an exception reaches here
                    throw;
                }
            })
            {
                IsBackground = true
            };
            thread.Start();
        }

        public int Count => queue.Count;

        public bool IsWorking { get => Interlocked.CompareExchange(ref working, 0, 0) > 0; }

        internal int IncrementWorking() => Interlocked.Increment(ref working);

        internal int DecrementWorking() => Interlocked.Decrement(ref working);

        internal IEnumerable<Work> KeepReading(CancellationToken cancellationToken)
            => queue.GetConsumingEnumerable(cancellationToken);

        public void Dispose()
        {
            queue?.Dispose();
        }
    }
}
