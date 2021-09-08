using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Configuration;
using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Buildersoft.Messaging.App.Kafka
{
    public class KafkaProducer<T> : IMessagingProducer<T>
    {
        public event IMessagingProducer<T>.StatusChangedHandler StatusChanged;

        private readonly MessagingProducerConfiguration<T> _messagingProducerConfiguration;
        private readonly ClientConfig _clientConfig;
        private IProducer<Null, String> producer;

        public KafkaProducer(IMessagingClient messagingClient, MessagingProducerConfiguration<T> messagingProducerConfiguration)
        {
            _messagingProducerConfiguration = messagingProducerConfiguration;

            _clientConfig = messagingClient.GetClient() as ClientConfig;
            producer = new ProducerBuilder<Null, String>(_clientConfig)
                .Build();
        }

        public CancellationTokenSource GetCancellationTokenSource()
        {
            return null;
        }

        public async Task<Guid> SendAsync(T tEntity, string key = "")
        {
            try
            {
                var dr = await producer.ProduceAsync(_messagingProducerConfiguration.Topic, new Message<Null, String> { Value = tEntity.ToJson() });
                // return new MessageId(dr.Key, dr.TopicPartition.Partition.Value);
                return Guid.NewGuid();
            }
            catch (ProduceException<Null, String>)
            {
                return Guid.Empty;
            }
        }
    }
}
