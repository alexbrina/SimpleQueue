using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Exceptions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleQueue.InMemory
{
    internal class Consumer
    {
        private readonly ILogger<Consumer> logger;
        private int consumerId;

        public Consumer(ILogger<Consumer> logger)
        {
            this.logger = logger
                ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task Execute<T>(
            ConsumerContext<T> context,
            IServiceProvider serviceProvider,
            CancellationToken cancellationToken)
            where T : ISimpleQueueWorker
        {
            this.consumerId = context.ConsumerId;

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} has started");

            foreach (var work in context.Queue.GetConsumingEnumerable(cancellationToken))
            {
                // When some work is available, we create a new scope and ask
                // service provider for a Worker instance (and its dependencies).
                // This scope is valid for this work execution only.
                using var scope = serviceProvider.CreateScope();
                var worker = scope.ServiceProvider.GetRequiredService<T>();
                context.ConfigureWorker?.Invoke(worker);
                await Execute(work, worker, context.Queue, context.MaxAttempts, cancellationToken);
            }

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} has exited");
        }

        private async Task Execute(
            Work work,
            ISimpleQueueWorker worker,
            BlockingCollection<Work> queue,
            int maxAttempts,
            CancellationToken cancellationToken)
        {
            // We catch whatever exception occured while attempting to
            // execute work and handle it. Note that consumer dedicated thread
            // dies if an exception bubbles up.
            try
            {
                await Attempt(work, worker, queue, cancellationToken);
            }
            catch (WorkIsMissingException ex)
            {
                logger.LogError(ex.Message);
            }
            catch (WorkCompletedException ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex.Message);
            }
            catch (Exception ex)
            {
                HandleFailure(work, queue, maxAttempts, ex);
            }
        }

        private async Task Attempt(
            Work work,
            ISimpleQueueWorker worker,
            BlockingCollection<Work> queue,
            CancellationToken cancellationToken)
        {
            var action = work.Attempted ? "retrying" : "processing";

            work.IncreaseAttempts();

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} is {action} Work {work.Id}" +
                    $" with data {work.Data}, attempt {work.Attempts}. " +
                    $"{queue.Count} work(s) remaining in queue.");

            work.SetCompleted();

            await worker.Execute(work, cancellationToken);
        }

        private void HandleFailure(
            Work work,
            BlockingCollection<Work> queue,
            int maxAttempts,
            Exception ex)
        {
            logger.LogError(ex, $"Consumer {consumerId} failed to " +
                $"process work {work.Id} with data {work.Data}!");

            if (work.Attempts < maxAttempts)
            {
                // put it back in queue
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug($"Consumer {consumerId} is putting work {work.Id}" +
                        $" back in queue for retry");

                work.SetPending();

                queue.Add(work);
            }
            else
            {
                // too many attempts, work is discarded
                logger.LogWarning($"Work {work.Id} with data {work.Data} has " +
                    $"reached max of {maxAttempts} " +
                    $"failed attempts and was discarded.");
            }
        }
    }
}
