using System;
using System.Runtime.Serialization;

namespace SimpleQueue.Abstractions.Exceptions
{
    [Serializable]
    public class WorkCompletedException : Exception
    {
        public WorkCompletedException(string id, string completedAt)
            : base($"Work {id} was already completed at {completedAt} and won't be executed again")
        {
        }

        protected WorkCompletedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
