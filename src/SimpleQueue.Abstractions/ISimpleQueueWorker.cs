using SimpleQueue.Abstractions.Models;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleQueue.Abstractions
{
    public interface ISimpleQueueWorker
    {
        Task Execute(Work work, CancellationToken cancellationToken);
    }
}
