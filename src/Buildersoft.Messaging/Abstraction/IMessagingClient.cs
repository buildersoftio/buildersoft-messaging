using Buildersoft.Messaging.Builder;
using Microsoft.Extensions.Logging;
using System;

namespace Buildersoft.Messaging.Abstraction
{
    public interface IMessagingClient
    {
        IMessagingClient GetClient();
        IMessagingClient Build();
        MessagingClientBuilder GetBuilder();
        ILoggerFactory GetLoggerFactory();
    }
}
