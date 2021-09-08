using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Builder;
using DotPulsar.Abstractions;
using Microsoft.Extensions.Logging;
using System;

namespace Buildersoft.Messaging.App.Pulsar
{
    public class PulsarClient : IMessagingClient
    {
        private readonly IPulsarClientBuilder _pulsarClientBuilder;
        private readonly MessagingClientBuilder _messagingBuilder;
        private readonly ILogger<PulsarClient> _logger;
        private IPulsarClient _pulsarClient = null;
        string brokerSelected;

        public PulsarClient(MessagingClientBuilder messagingBuilder)
        {
            _messagingBuilder = messagingBuilder;

            _logger = messagingBuilder
                .MessagingConfiguration
                .LoggerFactory
                .CreateLogger<PulsarClient>();


            if (messagingBuilder.MessagingConfiguration.GetBrokers().Count == 0)
            {
                _logger.LogDebug($"pulsar://connection : you should add broker_urls");
                throw new Exception("pulsar://connection : you should add broker_urls");
            }
            brokerSelected = messagingBuilder.MessagingConfiguration.GetBrokers()[0];
            _pulsarClientBuilder = DotPulsar.PulsarClient
                   .Builder()
                   .ServiceUrl(new Uri(brokerSelected))
                   .RetryInterval(messagingBuilder.MessagingConfiguration.RetryInterval);

            // We should build the pulsarClient when we initialize the Client;
            Build();
        }

        public PulsarClient(MessagingClientBuilder messagingBuilder, string brokerUrl)
        {

            _messagingBuilder = messagingBuilder;

            _logger = messagingBuilder
                .MessagingConfiguration
                .LoggerFactory
                .CreateLogger<PulsarClient>();


            if (messagingBuilder.MessagingConfiguration.GetBrokers().Count == 0 || brokerUrl == "")
            {
                _logger.LogDebug($"pulsar://connection : you should add broker_urls");
                throw new Exception("pulsar://connection : you should add broker_urls");
            }

            brokerSelected = brokerUrl;

            _pulsarClientBuilder = DotPulsar.PulsarClient
                   .Builder()
                   .ServiceUrl(new Uri(brokerSelected))
                   .RetryInterval(messagingBuilder.MessagingConfiguration.RetryInterval);

            // We should build the pulsarClient when we initialize the Client;
            Build();
        }

        /// <summary>
        /// Build PulsarClient and connect to Pulsar broker
        /// </summary>
        /// <returns>Bisko PulsarClient Instance</returns>
        public IMessagingClient Build()
        {
            try
            {
                _pulsarClient = _pulsarClientBuilder
                    .CloseInactiveConnectionsInterval(new TimeSpan(0, 0, 5))
                    .Build();

                _logger.LogInformation($"{brokerSelected}: trying to connect");
            }
            catch (Exception)
            {
                _logger.LogError($"{brokerSelected}: can not connect");
            }
            return this;
        }

        /// <summary>
        /// Return Pulsar Client Interface
        /// </summary>
        /// <returns>IPulsarClient instance</returns>
        public Object GetClient()
        {
            if (_pulsarClient != null)
                return _pulsarClient;
            else
            {
                _logger.LogError($"pulsar://connection : PulsarClient should be build before fetching it.");
                throw new ArgumentNullException("pulsar://connection : PulsarClient should be build before fetching it.");
            }
        }

        public MessagingClientBuilder GetBuilder()
        {
            return _messagingBuilder;
        }

        public ILoggerFactory GetLoggerFactory()
        {
            return _messagingBuilder
                .MessagingConfiguration
                .LoggerFactory;
        }
    }
}
