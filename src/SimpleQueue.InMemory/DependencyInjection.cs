using Microsoft.Extensions.DependencyInjection.Extensions;
using SimpleQueue.Abstractions;
using SimpleQueue.InMemory;
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddSimpleQueue(
            this IServiceCollection services)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            services.AddTransient<Consumer>();

            // this allows client applications to register ISimpleQueue
            // with different life time
            services.TryAddTransient<ISimpleQueue, InMemoryQueue>();

            return services;
        }
    }
}
