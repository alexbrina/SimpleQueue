using System;
using System.Threading;

namespace SimpleQueue.Abstractions.Models
{
    public class Work
    {
        public string Id { get; private set; }
        public string Data { get; private set; }
        public DateTimeOffset RequestedAt { get; private set; }
        public DateTimeOffset CompletedAt { get; private set; }
        public int Attempts { get; private set; }

        public Work(string id, string data)
        {
            Id = id;
            Data = data;
            RequestedAt = DateTimeOffset.Now;
        }

        public Work(string id, string data, string requestedAt, int? attempts)
        {
            Id = id;
            Data = data;
            RequestedAt = DateTimeOffset.Parse(requestedAt);
            Attempts = attempts.GetValueOrDefault();
        }

        public bool Attempted => Attempts > 0;

        public void SetCompleted()
        {
            CompletedAt = DateTimeOffset.Now;
        }

        public void SetPending()
        {
            CompletedAt = default;
        }

        public void IncreaseAttempts()
        {
            Attempts++;
        }
    }
}
