using System;
using System.Threading;
using System.Threading.Tasks;

namespace Buildersoft.Messaging.Abstraction
{
    public interface IMessagingProducer<T>
    {
        delegate void StatusChangedHandler(object sender, string status);
        event StatusChangedHandler StatusChanged;

        Task<Guid> SendAsync(T tEntity, string key = "");
        CancellationTokenSource GetCancellationTokenSource();
    }
}
