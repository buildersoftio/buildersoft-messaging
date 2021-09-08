using Buildersoft.Messaging.Builder;
using System;

namespace Buildersoft.Messaging.Abstraction
{
    public interface IMessagingClient
    {
        Object GetClient();
        IMessagingClient Build();
        MessagingClientBuilder GetBuilder();
        ILoggerFactory GetLoggerFactory();
    }
}
