using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Builder;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Buildersoft.Messaging.App.Kafka
{
    public class KafkaClient : IMessagingClient
    {
        private readonly MessagingClientBuilder _messagingBuilder;
        private readonly ILogger<KafkaClient> _logger;
        private ClientConfig clientConfig;

        public KafkaClient(MessagingClientBuilder messagingBuilder)
        {
            _messagingBuilder = messagingBuilder;

            _logger = messagingBuilder
                 .MessagingConfiguration
                 .LoggerFactory
                 .CreateLogger<KafkaClient>();

            clientConfig = new ClientConfig()
            {
                BootstrapServers = messagingBuilder.MessagingConfiguration.GetBrokersAsCSV()
            };
        }

        public IMessagingClient Build()
        {
            // For Kafka we don't need to build and test connection with broker.
            return this;
        }

        public MessagingClientBuilder GetBuilder()
        {
            return _messagingBuilder;
        }

        public object GetClient()
        {
            return clientConfig;
        }

        public ILoggerFactory GetLoggerFactory()
        {
            return _messagingBuilder
                .MessagingConfiguration
                .LoggerFactory;
        }
    }
}
