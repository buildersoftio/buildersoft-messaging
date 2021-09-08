using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Configuration;
using Buildersoft.Messaging.Domain.Message;
using Buildersoft.Messaging.Utility.Extensions;
using Confluent.Kafka;
using System;
using System.Threading;

namespace Buildersoft.Messaging.App.Kafka
{
    public class KafkaConsumer<T> : IMessagingConsumer<T>
    {
        public event IMessagingConsumer<T>.MessageReceivedHandler MessageReceived;
        public event IMessagingConsumer<T>.StatusChangedHandler StatusChanged;

        private readonly MessagingConsumerConfiguration<T> _messagingConsumerConfiguration;
        private readonly ClientConfig clientConfig;
        private readonly IConsumer<Null, String> consumer;

        public KafkaConsumer(IMessagingClient messagingClient, MessagingConsumerConfiguration<T> messagingConsumerConfiguration)
        {
            _messagingConsumerConfiguration = messagingConsumerConfiguration;
            clientConfig = messagingClient.GetClient() as ClientConfig;

            var consumerConfig = new ConsumerConfig(clientConfig)
            {
                GroupId = messagingConsumerConfiguration.ConsumerName
            };

            // Later we can control Schemas if we will use Conflient SchemaRegistry for Conflient Kafka
            // For now we don't support schema control, we are storing the messages as Jsons;

            // Kafka doesn't support shared consumers, but it supports failover and exclusive (Both are the same).
            // The reason that Kafka uses PULL to fetch messages, it doesn't support shared type.

            if (messagingConsumerConfiguration.SubscriptionType == SubscriptionTypes.Shared)
            {
                throw new Exception("Kafka doesn's support Shared Subscription");
            }

            consumer = new ConsumerBuilder<Null, String>(consumerConfig)
                .Build();

            new Thread(() => SubscribeTopic())
                .Start();
        }

        private void SubscribeTopic()
        {
            consumer.Subscribe(_messagingConsumerConfiguration.Topic);
            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume();
                        bool? shouldAcknowledge = MessageReceived?.Invoke(this,
                            new MessageReceivedArgs<T>(
                                cr.Message.Value.ToTopicObject<T>(),
                                cr.Message.Value,
                                DateTime.Now,
                                // It's a bug here, we should get the date from producer
                                DateTime.Now));
                        if (shouldAcknowledge.HasValue)
                        {
                            if (shouldAcknowledge.Value == true)
                            {
                                consumer.Commit(cr);
                            }
                        }
                    }
                    catch (ConsumeException)
                    {
                        // TO DISCUSS: skip for now;
                        // TODO: We will log this lib soon.
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }

        public void StopConsuming()
        {
            consumer.Close();
        }

        public void StartConsuming()
        {
            new Thread(() => SubscribeTopic())
                           .Start();
        }

        public CancellationTokenSource GetCancellationTokenSource()
        {
            throw new NotImplementedException();
        }

        public string GetInitializedConsumerName()
        {
            throw new NotImplementedException();
        }

        public string GetConsumerState()
        {
            throw new NotImplementedException();
        }
    }
}
