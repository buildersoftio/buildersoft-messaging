using Buildersoft.Messaging.Configuration;

namespace Buildersoft.Messaging.Builder
{
    public class MessagingConsumerBuilder<T>
    {
        public MessagingConsumerConfiguration<T> MessagingConsumerConfiguration { get; private set; }

        public MessagingConsumerBuilder(MessagingConsumerConfiguration<T> messagingConsumerConfiguration)
        {
            MessagingConsumerConfiguration = messagingConsumerConfiguration;
        }
    }
}
