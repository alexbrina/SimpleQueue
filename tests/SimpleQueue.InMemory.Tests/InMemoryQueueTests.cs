using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace SimpleQueue.InMemory.Tests
{
    public class InMemoryQueueTests
    {
        private ITestOutputHelper OutputHelper { get; }
        private readonly IServiceProvider serviceProvider;

        private const int WORK_EXECUTION_DELAY = 1000;

        // unique id generation
        private static string Id => $"{DateTime.Now:yyMMddHHmmss}" +
            $"{Interlocked.Increment(ref sequence):00000000}";
        private static int sequence = 0;

        public InMemoryQueueTests(ITestOutputHelper output)
        {
            OutputHelper = output;

            IServiceCollection services = new ServiceCollection();
            serviceProvider = services
                .AddLogging(b => b.AddXUnit(OutputHelper).SetMinimumLevel(LogLevel.Trace))
                .AddSimpleQueue()
                .BuildServiceProvider();
        }

        [Fact]
        public async Task Simple_worker()
        {
            var done = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(GetWorker(done), 3, CancellationToken.None);
            queue.Add(new Work(Id, "work"));
            await ForQueueToFinish(queue);

            // assert
            Assert.Single(done);
        }

        [Fact]
        public async Task Parallel_worker()
        {
            // arrange
            var done = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(GetWorker(done), 3, 3, CancellationToken.None);

            for (int i = 0; i < 10; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }
            await ForQueueToFinish(queue);

            // assert
            Assert.Equal(10, done.Count);
        }


        [Fact]
        public async Task Multiple_workers()
        {
            // arrange
            var done1 = new List<Work>();
            var done2 = new List<Work>();
            var done3 = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(GetWorker(done1), 3, CancellationToken.None);
            queue.Consume(GetWorker(done2), 3, CancellationToken.None);
            queue.Consume(GetWorker(done3), 3, CancellationToken.None);

            for (int i = 0; i < 30; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }
            await ForQueueToFinish(queue);

            // assert
            Assert.Equal(30, done1.Count + done2.Count + done3.Count);
        }

        [Fact]
        public async Task Max_attempts()
        {
            var done = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Add(new Work(Id, "work-1"));
            queue.Add(new Work(Id, "work-2"));

            var cursed = new Work(Id, "cursed");
            queue.Add(cursed);

            queue.Consume(GetWorker(done, cursed), 3, 3, CancellationToken.None);

            await ForQueueToFinish(queue);
            // wait a little more so retries can finish
            await Task.Delay(2 * WORK_EXECUTION_DELAY);

            // assert
            Assert.Equal(2, done.Count);
            Assert.Equal(0, queue.Count);
        }

        [Fact]
        public async Task Cancelled_worker()
        {
            var done = new List<Work>();
            var cts = new CancellationTokenSource();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(GetWorker(done), 3, cts.Token);
            cts.Cancel();
            queue.Add(new Work(Id, "work"));
            await Task.Delay(WORK_EXECUTION_DELAY + 100);

            // assert
            Assert.Empty(done);
            Assert.Equal(1, queue.Count);
        }

        [Fact]
        public async Task Cancelled_parallel_worker()
        {
            var done = new List<Work>();
            var cts = new CancellationTokenSource();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(GetWorker(done), 3, 3, cts.Token);

            for (int i = 0; i < 10; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }

            // wait to finish 3 works and start 3 others
            while (queue.Count > 4)
            {
                await Task.Delay(100);
            }
            cts.Cancel();

            // assert
            Assert.Equal(3, done.Count); // 3 finished
            Assert.True(5 > queue.Count); // 3 in exection and 4 remaining
        }

        [Fact]
        public async Task Multiple_queues()
        {
            // arrange
            var done1 = new List<Work>();
            var done2 = new List<Work>();
            var done3 = new List<Work>();
            using var queue1 = serviceProvider.GetRequiredService<ISimpleQueue>();
            using var queue2 = serviceProvider.GetRequiredService<ISimpleQueue>();
            using var queue3 = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue1.Consume(GetWorker(done1), 3, 3, CancellationToken.None);
            queue2.Consume(GetWorker(done2), 3, 3, CancellationToken.None);
            queue3.Consume(GetWorker(done3), 3, 3, CancellationToken.None);

            for (int i = 0; i < 10; i++)
            {
                queue1.Add(new Work(Id, $"q1-work-{i}"));
                queue2.Add(new Work(Id, $"q2-work-{i}"));
                queue3.Add(new Work(Id, $"q3-work-{i}"));
            }
            await ForQueueToFinish(queue1);
            await ForQueueToFinish(queue2);
            await ForQueueToFinish(queue3);

            // assert
            Assert.Equal(10, done1.Count);
            Assert.Equal(10, done2.Count);
            Assert.Equal(10, done3.Count);
        }

        [Fact]
        public async Task Multiple_producers()
        {
            // arrange
            var done = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var random = new Random();

            // act
            queue.Consume(GetWorker(done), 3, 3, CancellationToken.None);

            var producers = Enumerable.Range(1, 3)
                .Select(id =>
                {
                    return Task.Factory.StartNew(async () =>
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            await Task.Delay((int)random.NextDouble() * 100);
                            queue.Add(new Work(Id, $"p{id}-work-{i}"));
                        }
                    });
                }).ToArray();

            await Task.WhenAll(producers);
            await ForQueueToFinish(queue);

            // assert
            Assert.Equal(30, done.Count);
        }

        [Fact]
        public void Requeue()
        {
            // arrange
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            var works = new List<Work>();
            for (int i = 0; i < 10; i++)
            {
                var work = new Work(Id, $"work-{i}");
                works.Add(work);
                queue.Add(work);
            }

            // act
            queue.Requeue(works);

            // assert
            Assert.Equal(10, queue.Count);
        }

        [Fact]
        public async Task Multiple_requeues()
        {
            // arrange
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            var works = new List<Work>();
            for (int i = 0; i < 10; i++)
            {
                var work = new Work(Id, $"work-{i}");
                works.Add(work);
                queue.Add(work);
            }

            // act
            var requeuing = Enumerable.Range(1, 20)
                .Select(id =>
                {
                    return Task.Factory.StartNew(() =>
                    {
                        queue.Requeue(works);
                    });
                }).ToArray();
            await Task.WhenAll(requeuing);

            // assert
            Assert.Equal(10, queue.Count);
        }

        private static ISimpleQueueWorker GetWorker(
            ICollection<Work> works, Work cursed = null) => new Worker
            {
                Works = works,
                Cursed = cursed,
                ExecutionDelay = WORK_EXECUTION_DELAY,
            };

        private static async Task ForQueueToFinish(ISimpleQueue queue)
        {
            while (queue.Count > 0)
            {
                await Task.Delay(100);
            }
            await Task.Delay(WORK_EXECUTION_DELAY);
        }
    }

    public class Worker : ISimpleQueueWorker
    {
        public ICollection<Work> Works { get; init; }
        public Work Cursed { get; init; }
        public int ExecutionDelay { get; init; }
        private readonly object _lock = new();

        public async Task Execute(Work work, CancellationToken cancellationToken)
        {
            await Task.Delay(ExecutionDelay, cancellationToken);
            lock (_lock)
            {
                if (work.Id == Cursed?.Id)
                {
                    throw new InvalidOperationException($"{work.Id} failed!");
                }
                Works.Add(work);
            }
        }
    }
}
