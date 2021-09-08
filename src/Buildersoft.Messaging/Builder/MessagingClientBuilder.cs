using Buildersoft.Messaging.Configuration;

namespace Buildersoft.Messaging.Builder
{
    public class MessagingClientBuilder
    {
        public MessagingClientConfiguration MessagingConfiguration { get; private set; }

        public MessagingClientBuilder(MessagingClientConfiguration messagingConfiguration)
        {
            MessagingConfiguration = messagingConfiguration;
        }
    }
}
