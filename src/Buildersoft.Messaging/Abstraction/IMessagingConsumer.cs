using Buildersoft.Messaging.Domain.Message;
using System.Threading;

namespace Buildersoft.Messaging.Abstraction
{
    public interface IMessagingConsumer<T>
    {
        delegate bool MessageReceivedHandler(object sender, MessageReceivedArgs<T> messageReceivedArgs);
        event MessageReceivedHandler MessageReceived;

        delegate void StatusChangedHandler(object sender, string status);
        event StatusChangedHandler StatusChanged;

        void StopConsuming();
        void StartConsuming();
        string GetInitializedConsumerName();
        string GetConsumerState();
        CancellationTokenSource GetCancellationTokenSource();
    }
}
