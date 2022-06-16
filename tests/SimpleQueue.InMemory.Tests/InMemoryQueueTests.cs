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

namespace SimpleQueue.InMemory.Tests
{
    public class InMemoryQueueTests
    {
        private readonly IServiceProvider serviceProvider;

        private const int WORK_EXECUTION_DELAY = 20;
        private const int WORK_EXECUTION_DELAY_HALF = 10;
        private const int WORK_EXECUTION_DELAY_FINISH = 2000;

        // unique id generation
        private static string Id => $"{DateTime.Now:yyMMddHHmmss}" +
            $"{Interlocked.Increment(ref sequence):00000000}";
        private static int sequence = 0;

        public InMemoryQueueTests()
        {
            IServiceCollection services = new ServiceCollection();
            serviceProvider = services
                .AddLogging(b => b.AddDebug().SetMinimumLevel(LogLevel.Trace))
                .AddSimpleQueue()
                .AddTransient<Worker>()
                .BuildServiceProvider();
        }

        [Fact]
        public async Task Simple_worker()
        {
            // arrange
            var done = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(3, Configure(done), CancellationToken.None);
            queue.Add(new Work(Id, "work"));
            await ForQueueToFinish(queue);

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Single(done);
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
            queue.Consume(3, Configure(done1), CancellationToken.None);
            queue.Consume(3, Configure(done2), CancellationToken.None);
            queue.Consume(3, Configure(done3), CancellationToken.None);

            for (int i = 0; i < 30; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }

            await ForQueueToFinish(queue);

            await Task.WhenAll(
                WorkIsDone(done1, 10),
                WorkIsDone(done2, 10),
                WorkIsDone(done3, 10)
            );

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(30, done1.Count + done2.Count + done3.Count);
        }

        [Fact]
        public async Task Max_attempts()
        {
            // arrange
            var done = new SynchronizedCollection<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var cursed = new Work(Id, "cursed");

            // act
            queue.Add(new Work(Id, "work-1"));
            queue.Add(new Work(Id, "work-2"));
            queue.Add(cursed);

            var configure = Configure(done, cursed);
            for (int i = 0; i < 3; i++)
            {
                queue.Consume(3, configure, CancellationToken.None);
            }

            await ForQueueToFinish(queue);

            // wait a little more so retries can finish
            await Task.Delay(2 * WORK_EXECUTION_DELAY);

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(2, done.Count);
        }

        [Fact]
        public async Task Cancelled_worker()
        {
            // arrange
            var done = new List<Work>();
            var cts = new CancellationTokenSource();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume(3, Configure(done), cts.Token);
            // cancel consumer even before sending a work
            cts.Cancel();
            queue.Add(new Work(Id, "work"));
            await Task.Delay(WORK_EXECUTION_DELAY);

            // assert
            Assert.Equal(1, queue.Count);
            Assert.Empty(done);
        }

        [Fact]
        public async Task Cancelled_parallel_worker()
        {
            // arrange
            var done = new SynchronizedCollection<Work>();
            var cts = new CancellationTokenSource();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var configure = Configure(done);

            // act
            for (int i = 0; i < 3; i++)
            {
                queue.Consume(3, Configure(done), cts.Token);
            }

            for (int i = 0; i < 10; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }

            // wait to finish 3 works and start 3 others
            while (queue.Count > 4)
            {
                await Task.Delay(WORK_EXECUTION_DELAY_HALF);
            }
            cts.Cancel();

            // assert
            Assert.True(5 > queue.Count); // 3 in exection and 4 remaining
            Assert.Equal(3, done.Count); // 3 finished
        }

        [Fact]
        public async Task Multiple_producers()
        {
            // arrange
            var done = new SynchronizedCollection<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var random = new Random();
            var configure = Configure(done);

            // act
            for (int i = 0; i < 3; i++)
            {
                queue.Consume(3, configure, CancellationToken.None);
            }

            var producers = Enumerable.Range(1, 3)
                .Select(id =>
                {
                    return Task.Factory.StartNew(async () =>
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var randomDelay = (int)random.NextDouble()
                                * WORK_EXECUTION_DELAY_HALF;
                            await Task.Delay(randomDelay);
                            queue.Add(new Work(Id, $"p{id}-work-{i}"));
                        }
                    });
                }).ToArray();

            await Task.WhenAll(producers);

            await ForQueueToFinish(queue);

            await WorkIsDone(done, 30);

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(30, done.Count);
        }

        [Fact]
        public async Task Parallel_worker_single_queue()
        {
            // arrange
            // we need a thread-safe collection<T> since we have one single
            // worker instance being shared by different consumer threads
            var done = new SynchronizedCollection<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var configure = Configure(done);

            // act
            for (int i = 0; i < 3; i++)
            {
                queue.Consume(3, configure, CancellationToken.None);
            }

            for (int i = 0; i < 10; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }

            await ForQueueToFinish(queue);

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(10, done.Count);
        }

        [Fact]
        public async Task Parallel_workers_multiple_queues()
        {
            // arrange
            var done1 = new SynchronizedCollection<Work>();
            var done2 = new SynchronizedCollection<Work>();
            var done3 = new SynchronizedCollection<Work>();
            using var queue1 = serviceProvider.GetRequiredService<ISimpleQueue>();
            using var queue2 = serviceProvider.GetRequiredService<ISimpleQueue>();
            using var queue3 = serviceProvider.GetRequiredService<ISimpleQueue>();
            var configure1 = Configure(done1);
            var configure2 = Configure(done2);
            var configure3 = Configure(done3);

            // act
            for (int i = 0; i < 10; i++)
            {
                queue1.Add(new Work(Id, $"q1-work-{i}"));
                queue2.Add(new Work(Id, $"q2-work-{i}"));
                queue3.Add(new Work(Id, $"q3-work-{i}"));
            }

            for (int i = 0; i < 3; i++)
            {
                queue1.Consume(3, configure1, CancellationToken.None);
                queue2.Consume(3, configure2, CancellationToken.None);
                queue3.Consume(3, configure3, CancellationToken.None);
            }

            await Task.WhenAll(
                ForQueueToFinish(queue1),
                ForQueueToFinish(queue2),
                ForQueueToFinish(queue3)
            );

            await Task.WhenAll(
                WorkIsDone(done1, 10),
                WorkIsDone(done2, 10),
                WorkIsDone(done3, 10)
            );

            // assert
            Assert.Equal(0, queue1.Count);
            Assert.Equal(10, done1.Count);
            Assert.Equal(0, queue2.Count);
            Assert.Equal(10, done2.Count);
            Assert.Equal(0, queue3.Count);
            Assert.Equal(10, done3.Count);
        }

        [Fact]
        public async Task Requeue_single()
        {
            // arrange
            var works = new List<Work>();
            var done = new List<Work>();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            for (int i = 0; i < 10; i++)
            {
                var work = new Work(Id, $"work-{i}");
                works.Add(work);
                queue.Add(work);
            }
            queue.Consume(3, Configure(done), CancellationToken.None);

            await ForQueueToFinish(queue);

            queue.Requeue(works);

            await ForQueueToFinish(queue);

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(20, done.Count);
        }

        [Fact]
        public async Task Requeue_multiple()
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
                        // as all works are already queued, they won't be requeued
                        queue.Requeue(works);
                    });
                }).ToArray();
            await Task.WhenAll(requeuing);

            // assert
            Assert.Equal(10, queue.Count);
        }

        private static Action<Worker> Configure(
            ICollection<Work> works, Work cursed = null)
        {
            void configure(Worker worker)
            {
                worker.Works = works;
                worker.Cursed = cursed;
                worker.ExecutionDelay = WORK_EXECUTION_DELAY;
            };
            return configure;
        }

        private static async Task ForQueueToFinish(ISimpleQueue queue)
        {
            while (queue.Count > 0)
            {
                await Task.Delay(WORK_EXECUTION_DELAY_HALF);
            }
            await Task.Delay(WORK_EXECUTION_DELAY_FINISH);
        }

        // this is nasty! we never know how long we must wait
        // until remaining works are all done after queues are empty.
        // this kind of invalidate assertions on done collections.
        private static async Task WorkIsDone(ICollection<Work> collection, int total)
        {
            var maxWaitCount = 1000;
            int count = 0;
            while (collection.Count < total)
            {
                if (count++ > maxWaitCount)
                {
                    throw new InvalidOperationException("Waited too mutch");
                }
                await Task.Delay(WORK_EXECUTION_DELAY);
            }
        }
    }

    public class Worker : ISimpleQueueWorker
    {
        public Work Cursed { get; set; }
        public int ExecutionDelay { get; set; }
        public bool Executing { get; private set; } = false;

        private readonly object _lock = new();
        private ICollection<Work> works;
        public ICollection<Work> Works
        {
            get => works;
            set
            {
                lock (_lock)
                {
                    works = value;
                }
            }
        }

        public async Task Execute(Work work, CancellationToken cancellationToken)
        {
            await Task.Delay(ExecutionDelay, cancellationToken);
            if (work.Id == Cursed?.Id)
            {
                throw new InvalidOperationException($"{work.Id} failed!");
            }
            lock (_lock)
            {
                works.Add(work);
            }
        }

    }
}
