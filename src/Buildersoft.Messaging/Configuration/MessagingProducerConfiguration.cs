using Buildersoft.Messaging.Configuration.Parameters;
using Buildersoft.Messaging.Utility.Extensions;
using System;

namespace Buildersoft.Messaging.Configuration
{
    public class MessagingProducerConfiguration<T>
    {
        public string ProducerName { get; set; }
        public MessageProtocol MessageProtocol { get; set; }
        public string Tenant { get; set; }
        public string Namespace { get; set; }
        public string Topic { get; set; }
        public T Schema { get; set; }
        public bool ShowEnvironmentInTopic { get; set; }
        public bool IsAllowedToStoreInMemoryIfConnectionFailes { get; set; }

        public MessagingProducerConfiguration()
        {
            MessageProtocol = MessageProtocol.Persistent;
            ProducerName = "default-producer";
            Tenant = "public";
            Namespace = "default";
            Topic = "";
            ShowEnvironmentInTopic = true;
            IsAllowedToStoreInMemoryIfConnectionFailes = false;
        }

        public string BuildTopic(string environment)
        {
            if (Topic != "")
            {
                if (ShowEnvironmentInTopic != true)
                    return $"{MessageProtocol.GetStringContent()}://{Tenant}/{Namespace}/{Topic}";

                return $"{MessageProtocol.GetStringContent()}://{Tenant}/{Namespace}/{environment}-{Topic}";
            }
            else
                throw new ArgumentNullException("Topic should not be null or empty");
        }
    }

}
