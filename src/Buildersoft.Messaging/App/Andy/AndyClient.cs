﻿using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Builder;
using Microsoft.Extensions.Logging;

namespace Buildersoft.Messaging.App.Andy
{
    public class AndyClient : IMessagingClient
    {
        public IMessagingClient Build()
        {
            throw new System.NotImplementedException();
        }

        public MessagingClientBuilder GetBuilder()
        {
            throw new System.NotImplementedException();
        }

        public object GetClient()
        {
            throw new System.NotImplementedException();
        }

        public ILoggerFactory GetLoggerFactory()
        {
            throw new System.NotImplementedException();
        }
    }
}
