using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Builder;
using Microsoft.Extensions.Logging;

namespace Buildersoft.Messaging.App.Kafka
{
    public class KafkaClient : IMessagingClient
    {
        public IMessagingClient Build()
        {
            throw new System.NotImplementedException();
        }

        public MessagingClientBuilder GetBuilder()
        {
            throw new System.NotImplementedException();
        }

        public IMessagingClient GetClient()
        {
            throw new System.NotImplementedException();
        }

        public ILoggerFactory GetLoggerFactory()
        {
            throw new System.NotImplementedException();
        }
    }
}
