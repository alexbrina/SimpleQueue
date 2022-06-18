using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SimpleQueue.Abstractions;
using SimpleQueue.Abstractions.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace SimpleQueue.InMemory.Tests
{
    public sealed class InMemoryQueueTests : IClassFixture<DefaultFixture>
    {
        private readonly DefaultFixture fixture;
        private readonly IServiceProvider serviceProvider;
        private const int EXECUTION_DELAY = 40;
        private const int EXECUTION_DELAY_FRACTION = 5;
        private const string CURSED = "cursed";

        // unique id generation
        private static string Id => $"{DateTime.Now:yyMMddHHmmss}" +
            $"{Interlocked.Increment(ref sequence):00000000}";
        private static int sequence = 0;

        public InMemoryQueueTests(DefaultFixture fixture)
        {
            this.fixture = fixture;

            IServiceCollection services = new ServiceCollection();
            serviceProvider = services
                .AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Trace))
                .AddSimpleQueue()
                .AddScoped<List<Work>>()
                .AddScoped<SynchronizedCollection<Work>>()
                .AddTransient(s => new SimpleWorker(s.GetService<List<Work>>(), EXECUTION_DELAY))
                .AddTransient(s => new CursedWorker(s.GetService<List<Work>>(), EXECUTION_DELAY, CURSED))
                .AddTransient(s => new ConcurrentWorker(s.GetService<SynchronizedCollection<Work>>(), EXECUTION_DELAY, CURSED))
                .BuildServiceProvider();
        }

        private void LogTest(string test)
        {
            fixture.LogWriter.WriteLine($"{new string('*', 40)} {test} {new string('*', 40)}" );
        }

        [Fact]
        public async Task Simple_worker()
        {
            // arrange
            LogTest(nameof(Simple_worker));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume<SimpleWorker>(3, scope);
            queue.Add(new Work(Id, "work"));
            await ForQueueToFinish(queue);
            var done = scope.ServiceProvider.GetService<List<Work>>();

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Single(done);
        }

        [Fact]
        public async Task Max_attempts()
        {
            // arrange
            LogTest(nameof(Max_attempts));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Add(new Work(Id, "work-1"));
            queue.Add(new Work(Id, "work-2"));
            queue.Add(new Work(Id, CURSED));

            for (int i = 0; i < 3; i++)
            {
                queue.Consume<ConcurrentWorker>(3, scope);
            }

            await ForQueueToFinish(queue);
            var done = scope.ServiceProvider.GetService<SynchronizedCollection<Work>>();

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(2, done.Count);
        }

        [Fact]
        public async Task Cancelled_worker()
        {
            // arrange
            LogTest(nameof(Cancelled_worker));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var cts = new CancellationTokenSource();

            // act
            queue.Consume<SimpleWorker>(3, scope, cts.Token);
            await Task.Delay(EXECUTION_DELAY);
            cts.Cancel(); // cancel consumer even before sending a work
            await Task.Delay(EXECUTION_DELAY);
            queue.Add(new Work(Id, "work"));
            await Task.Delay(EXECUTION_DELAY);
            var done = scope.ServiceProvider.GetService<List<Work>>();

            // assert
            Assert.Equal(1, queue.Count);
            Assert.Empty(done);
        }

        [Fact]
        public async Task Cancelled_multiple_workers()
        {
            // arrange
            LogTest(nameof(Cancelled_multiple_workers));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var cts = new CancellationTokenSource();

            // act
            for (int i = 0; i < 3; i++)
            {
                queue.Consume<ConcurrentWorker>(3, scope, cts.Token);
            }
            for (int i = 0; i < 10; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }
            // wait to finish 3 works and start 3 others
            while (queue.Count > 4)
            {
                await Task.Delay(EXECUTION_DELAY_FRACTION);
            }
            cts.Cancel();
            await Task.Delay(EXECUTION_DELAY);
            var done = scope.ServiceProvider.GetService<SynchronizedCollection<Work>>();

            // assert
            Assert.Equal(4, queue.Count); // 4 remaining
            Assert.Equal(3, done.Count); // 3 finished
        }

        [Fact]
        public async Task Multiple_producers()
        {
            // arrange
            LogTest(nameof(Multiple_producers));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var random = new Random();

            // act
            for (int i = 0; i < 3; i++)
            {
                queue.Consume<ConcurrentWorker>(3, scope);
            }

            var producers = Enumerable.Range(1, 3)
                .Select(id =>
                {
                    return Task.Factory.StartNew(async () =>
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var randomDelay = (int)random.NextDouble()
                                * EXECUTION_DELAY_FRACTION;
                            await Task.Delay(randomDelay);
                            queue.Add(new Work(Id, $"p{id}-work-{i}"));
                        }
                    });
                }).ToArray();

            await Task.WhenAll(producers);
            await ForQueueToFinish(queue);
            var done = scope.ServiceProvider.GetService<SynchronizedCollection<Work>>();

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(30, done.Count);
        }

        [Fact]
        public async Task Multiple_workers_single_queue()
        {
            // arrange
            LogTest(nameof(Multiple_workers_single_queue));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            queue.Consume<ConcurrentWorker>(3, scope);
            for (int i = 0; i < 30; i++)
            {
                queue.Add(new Work(Id, $"work-{i}"));
            }
            await ForQueueToFinish(queue);
            var done = scope.ServiceProvider.GetService<SynchronizedCollection<Work>>();

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(30, done.Count);
        }

        [Fact]
        public async Task Multiple_workers_and_queues()
        {
            // arrange
            LogTest(nameof(Multiple_workers_and_queues));
            using var scope1 = serviceProvider.CreateScope();
            using var scope2 = serviceProvider.CreateScope();
            using var queue1 = serviceProvider.GetRequiredService<ISimpleQueue>();
            using var queue2 = serviceProvider.GetRequiredService<ISimpleQueue>();

            // act
            for (int i = 0; i < 10; i++)
            {
                queue1.Add(new Work(Id, $"q1-work-{i}"));
                queue2.Add(new Work(Id, $"q2-work-{i}"));
            }

            for (int i = 0; i < 3; i++)
            {
                queue1.Consume<ConcurrentWorker>(3, scope1);
                queue2.Consume<ConcurrentWorker>(3, scope2);
            }

            await Task.WhenAll(
                ForQueueToFinish(queue1),
                ForQueueToFinish(queue2)
            );
            var done1 = scope1.ServiceProvider.GetService<SynchronizedCollection<Work>>();
            var done2 = scope2.ServiceProvider.GetService<SynchronizedCollection<Work>>();

            // assert
            Assert.Equal(0, queue1.Count);
            Assert.Equal(10, done1.Count);
            Assert.Equal(0, queue2.Count);
            Assert.Equal(10, done2.Count);
        }

        [Fact]
        public async Task Requeue_single()
        {
            // arrange
            LogTest(nameof(Requeue_single));
            using var scope = serviceProvider.CreateScope();
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();
            var works = new List<Work>();

            // act
            for (int i = 0; i < 10; i++)
            {
                var work = new Work(Id, $"work-{i}");
                works.Add(work);
                queue.Add(work);
            }
            queue.Consume<SimpleWorker>(3, scope);
            await ForQueueToFinish(queue);
            queue.Requeue(works);
            await ForQueueToFinish(queue);
            var done = scope.ServiceProvider.GetService<List<Work>>();

            // assert
            Assert.Equal(0, queue.Count);
            Assert.Equal(20, done.Count);
        }

        [Fact]
        public async Task Requeue_multiple()
        {
            // arrange
            LogTest(nameof(Requeue_multiple));
            using var queue = serviceProvider.GetRequiredService<ISimpleQueue>();

            var works = new List<Work>();
            for (int i = 0; i < 10; i++)
            {
                works.Add(new Work(Id, $"work-{i}"));
            }

            // act
            var requeuing = Enumerable.Range(1, 10)
                .Select(_ => Task.Factory.StartNew(() =>
                    {
                        // 1st requeue will put works in queue
                        queue.Requeue(works);
                    })).ToArray();
            await Task.WhenAll(requeuing);

            // assert
            Assert.Equal(10, queue.Count);
        }

        private static async Task ForQueueToFinish(ISimpleQueue queue)
        {
            while (queue.Count > 0 || queue.IsWorking)
            {
                await Task.Delay(EXECUTION_DELAY);
            }
            await Task.Delay(EXECUTION_DELAY);
        }
    }

    public class SimpleWorker : ISimpleQueueWorker
    {
        protected readonly ICollection<Work> done;
        protected readonly int delay;

        public SimpleWorker(ICollection<Work> done, int delay)
        {
            this.done = done;
            this.delay = delay;
        }

        public virtual async Task Execute(Work work, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            done.Add(work);
        }
    }

    public class CursedWorker : SimpleWorker
    {
        private readonly string cursed;

        public CursedWorker(ICollection<Work> done, int delay, string cursed)
            : base(done, delay)
        {
            this.cursed = cursed;
        }

        public override async Task Execute(Work work, CancellationToken cancellationToken)
        {
            if (work.Data == cursed)
            {
                throw new InvalidOperationException($"{work.Id} failed!");
            }
            await base.Execute(work, cancellationToken);
        }
    }

    public class ConcurrentWorker : CursedWorker
    {
        public ConcurrentWorker(ICollection<Work> done, int delay, string cursed)
            : base(done, delay, cursed)
        {
        }
    }
}
