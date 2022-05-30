using System;
using System.Runtime.Serialization;

namespace SimpleQueue.Abstractions.Exceptions
{
    [Serializable]
    public class WorkIsMissingException : Exception
    {
        public WorkIsMissingException(string id)
            : base($"Work {id} is missing from storage and won't be executed")
        {
        }

        protected WorkIsMissingException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
