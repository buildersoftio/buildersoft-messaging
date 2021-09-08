using Buildersoft.Messaging.Configuration.Parameters;
using Buildersoft.Messaging.Utility.Extensions;
using System;

namespace Buildersoft.Messaging.Configuration
{
    public class MessagingConsumerConfiguration<T>
    {
        /// <summary>
        /// Subscription Name
        /// </summary>
        public string SubscriptionName { get; set; }

        /// <summary>
        /// Subscription type, defaul is Exclusive; For kafka subscription type is not needed.
        /// </summary>
        public SubscriptionTypes SubscriptionType { get; set; }

        /// <summary>
        /// Consumer name
        /// </summary>
        public string ConsumerName { get; set; }

        /// <summary>
        /// Message protocol, default is persistent
        /// </summary>
        public MessageProtocol MessageProtocol { get; set; }

        //Tenant name
        public string Tenant { get; set; }

        /// <summary>
        /// Namespace name
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// Topic name
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// Schema type, not implemented yet for Pulsar.
        /// </summary>
        public T Schema { get; set; }

        /// <summary>
        /// Set the priority level for the shared subscription consumer. The default is 0.
        /// </summary>
        public int PriorityLevel { get; set; }

        /// <summary>
        /// Should resend Unacknowledged Messages, default is true.
        /// </summary>
        public bool ShouldResendUnacknowledgedMessages { get; set; }

        /// <summary>
        /// Number of messages that will be prefetched. The default is 1000.
        /// </summary>
        public uint MessagePrefetchCount { get; set; }

        /// <summary>
        /// Configure Initial Position of the coursor, defaukt us Earliest
        /// </summary>
        public SubscriptionInitialPositions SubscriptionInitialPosition { get; set; }

        public MessagingConsumerConfiguration()
        {
            MessageProtocol = MessageProtocol.Persistent;
            Tenant = "public";
            Namespace = "default";
            Topic = "";
            SubscriptionName = "default";
            ConsumerName = "default-consumer";
            SubscriptionType = SubscriptionTypes.Exclusive;
            PriorityLevel = 0;
            MessagePrefetchCount = 1000;
            ShouldResendUnacknowledgedMessages = true;
            SubscriptionInitialPosition = SubscriptionInitialPositions.Earliest;
        }

        /// <summary>
        /// Build Topic will generate a string of protocol://tenant/namespace/topic
        /// </summary>
        /// <returns></returns>
        public string BuildTopic(string environment)
        {
            if (Topic != "")
            {
                return $"{MessageProtocol.GetStringContent()}://{Tenant}/{Namespace}/{Topic}";
            }
            else
                throw new ArgumentNullException("Topic should not be null or empty");
        }
    }

    public enum SubscriptionTypes
    {
        /// <summary>
        /// There can be only 1 consumer on the same topic with the same subscription name.
        /// </summary>
        Exclusive,
        /// <summary>
        /// Multiple consumers will be able to use the same subscription name and the messages will be dispatched according to the key.
        /// </summary>
        Failover,
        /// <summary>
        /// Multiple consumers will be able to use the same subscription name and the messages will be dispatched according to a round-robin rotation.
        /// </summary>
        Shared
    }

    public enum SubscriptionInitialPositions
    {
        /// <summary>
        /// Consume from the earliest unacked message
        /// </summary>
        Earliest,
        /// <summary>
        /// Ignore the old messages, consume messages that are comming
        /// </summary>
        Latest
    }
}
