using Buildersoft.Messaging.Abstraction;
using System.Threading;

namespace Buildersoft.Messaging.App.Andy
{
    public class AndyConsumer<T> : IMessagingConsumer<T>
    {
        public event IMessagingConsumer<T>.MessageReceivedHandler MessageReceived;
        public event IMessagingConsumer<T>.StatusChangedHandler StatusChanged;

        public CancellationTokenSource GetCancellationTokenSource()
        {
            throw new System.NotImplementedException();
        }

        public string GetConsumerState()
        {
            throw new System.NotImplementedException();
        }

        public string GetInitializedConsumerName()
        {
            throw new System.NotImplementedException();
        }

        public void StartConsuming()
        {
            throw new System.NotImplementedException();
        }

        public void StopConsuming()
        {
            throw new System.NotImplementedException();
        }
    }
}
