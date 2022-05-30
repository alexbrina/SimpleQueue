using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions.Exceptions;
using SimpleQueue.Abstractions.Models;
using System;
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

        public async Task Execute(int consumerId, WorkerContext context,
            CancellationToken cancellationToken)
        {
            this.consumerId = consumerId;

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} is starting");

            foreach (var work in context.Queue.GetConsumingEnumerable(cancellationToken))
            {
                await Execute(work, context, cancellationToken);
            }

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} has exited");
        }

        private async Task Execute(Work work, WorkerContext context,
            CancellationToken cancellationToken)
        {
            // WARNING: consumer thread dies without notice if an exception bubbles up
            try
            {
                await Attempt(work, context, cancellationToken);
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
                HandleFailure(work, context, ex);
            }
        }

        private async Task Attempt(Work work, WorkerContext context,
            CancellationToken cancellationToken)
        {
            var action = work.Attempted ? "retrying" : "processing";

            work.IncreaseAttempts();

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Consumer {consumerId} is {action} Work {work.Id}" +
                    $" with data {work.Data}, attempt {work.Attempts}. " +
                    $"{context.Queue.Count} work(s) remaining in queue.");

            work.SetCompleted();
            await context.Worker.Execute(work, cancellationToken);
        }

        private void HandleFailure(Work work, WorkerContext context, Exception ex)
        {
            logger.LogError(ex, $"Consumer {consumerId} failed to " +
                $"process work {work.Id} with data {work.Data}!");

            if (work.Attempts < context.MaxAttempts)
            {
                // put it back in queue
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug($"Consumer {consumerId} is putting work {work.Id}" +
                        $" back in queue for retry");

                work.SetPending();
                context.Queue.Add(work);
            }
            else
            {
                // too many attempts, work was discarded
                logger.LogWarning($"Work {work.Id} with data {work.Data} has " +
                    $"reached max of {context.MaxAttempts} " +
                    $"failed attempts and was discarded.");
            }
        }
    }
}
