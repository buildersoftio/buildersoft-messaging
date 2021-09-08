using Buildersoft.Messaging.Abstraction;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Buildersoft.Messaging.App.Pulsar
{
    public class PulsarProducer<T> : IMessagingProducer<T>
    {
        public event IMessagingProducer<T>.StatusChangedHandler StatusChanged;

        public CancellationTokenSource GetCancellationTokenSource()
        {
            throw new NotImplementedException();
        }

        public Task<Guid> SendAsync(T tEntity, string key = "")
        {
            throw new NotImplementedException();
        }
    }
}
