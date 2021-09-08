using Buildersoft.Messaging.Configuration;

namespace Buildersoft.Messaging.Builder
{
    public class MessagingProducerBuilder<T>
    {
        public MessagingProducerConfiguration<T> MessagingProducerConfiguration { get; private set; }

        public MessagingProducerBuilder(MessagingProducerConfiguration<T> messagingProducerConfiguration)
        {
            MessagingProducerConfiguration = messagingProducerConfiguration;
        }
    }
}
