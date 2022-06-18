using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Exceptions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleQueue.InMemory
{
    internal class Consumer
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ILogger<Consumer> logger;
        private int consumerId;

        public Consumer(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider
                ?? throw new ArgumentNullException(nameof(serviceProvider));

            logger = serviceProvider.GetRequiredService<ILogger<Consumer>>();
        }

        public async Task Execute<T>(
            ConsumerContext<T> context,
            CancellationToken cancellationToken)
            where T : ISimpleQueueWorker
        {
            consumerId = context.ConsumerId;

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} has started");

            foreach (var work in context.Queue.KeepReading(cancellationToken))
            {
                try
                {
                    context.Queue.IncrementWorking();
                    using var scope = serviceProvider.CreateScope();
                    var worker = (context.Scope ?? scope).ServiceProvider.GetRequiredService<T>();
                    await Execute(work, worker, context.Queue, context.MaxAttempts, cancellationToken);
                }
                finally
                {
                    context.Queue.DecrementWorking();
                }
            }

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} has exited");
        }

        private async Task Execute(
            Work work,
            ISimpleQueueWorker worker,
            InMemoryQueue queue,
            int maxAttempts,
            CancellationToken cancellationToken)
        {
            // We catch whatever exception occured while attempting to
            // execute work and handle it.
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
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                HandleFailure(work, queue, maxAttempts, ex);
            }
        }

        private async Task Attempt(
            Work work,
            ISimpleQueueWorker worker,
            InMemoryQueue queue,
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
            InMemoryQueue queue,
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
