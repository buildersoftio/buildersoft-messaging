using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Configuration;
using Buildersoft.Messaging.Domain.Message;
using Buildersoft.Messaging.Utility.Extensions;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Buildersoft.Messaging.App.Pulsar
{
    public class PulsarConsumer<T> : IMessagingConsumer<T>
    {
        public event IMessagingConsumer<T>.MessageReceivedHandler MessageReceived;
        public event IMessagingConsumer<T>.StatusChangedHandler StatusChanged;

        private readonly ILogger<PulsarConsumer<T>> _logger;
        private readonly IMessagingClient _messagingClient;
        private IPulsarClient _pulsarClient;
        private readonly MessagingConsumerConfiguration<T> _messagingConsumerConfiguration;
        private readonly string _instanceNumber;
        private readonly string _instanceName;

        private ConsumerState consumerState;
        private DotPulsar.Abstractions.IConsumer<byte[]> consumer;
        private CancellationTokenSource cancellationToken;

        private DateTime todayDate;
        private double proccesedMessages = 0;
        private System.Timers.Timer LogMetricsTimer;

        private List<string> brokerUrls;
        private int brokerIndex = -1;
        private bool IsConsumerReading = false;

        public PulsarConsumer(IMessagingClient messagingClient, MessagingConsumerConfiguration<T> messagingConsumerConfiguration)
        {
            todayDate = DateTime.Now;
            _messagingClient = messagingClient;


            brokerUrls = messagingClient
                .GetBuilder()
                .MessagingConfiguration
                .GetBrokers();

            _instanceNumber = new Random().Next(0, 1000000).ToString("D6");
            _instanceName = $"consumer-{messagingConsumerConfiguration.ConsumerName}-{messagingConsumerConfiguration.Topic}-{_instanceNumber}";

            _logger = messagingClient
                .GetLoggerFactory()
                .CreateLogger<PulsarConsumer<T>>();

            _messagingConsumerConfiguration = messagingConsumerConfiguration;

            CreateNewConsumer();

            LogMetricsTimer = new System.Timers.Timer()
            {
                Interval = new TimeSpan(0, 5, 0).TotalMilliseconds,
                AutoReset = true
            };
            LogMetricsTimer.Elapsed += LogMetricsTimer_Elapsed;
            LogMetricsTimer.Start();

            CheckIfConsumerIsConnected(false);

        }

        private void CheckIfConsumerIsConnected(bool goFaster = false)
        {
            new Thread(async () =>
            {
                while (consumerState == ConsumerState.Disconnected || consumerState == ConsumerState.Closed || consumerState == ConsumerState.Faulted)
                {
                    if (IsConsumerReading == true)
                    {
                        await RetryConnection(goFaster);
                    }
                }
            }).Start();
        }

        private void CreateNewConsumer()
        {
            brokerIndex += 1;
            if (brokerUrls.Count() == brokerIndex)
                brokerIndex = 0;

            _logger.LogWarning($"bisko://consumers/{_messagingConsumerConfiguration.ConsumerName}-{_instanceNumber} : trying to connect to '{brokerUrls[brokerIndex]}'");

            _pulsarClient = new PulsarClient(_messagingClient.GetBuilder(), brokerUrls[brokerIndex]).GetClient() as IPulsarClient;

            var consumerBuilder = _pulsarClient.NewConsumer<byte[]>(Schema.ByteArray)
                .ConsumerName($"{_messagingConsumerConfiguration.ConsumerName}-{_instanceNumber}")
                .SubscriptionName(_messagingConsumerConfiguration.SubscriptionName)
                .Topic(_messagingConsumerConfiguration.BuildTopic())
                .PriorityLevel(_messagingConsumerConfiguration.PriorityLevel)
                .MessagePrefetchCount(_messagingConsumerConfiguration.MessagePrefetchCount)
                .StateChangedHandler(Monitor);

            switch (_messagingConsumerConfiguration.SubscriptionInitialPosition)
            {
                case SubscriptionInitialPositions.Earliest:
                    consumerBuilder.InitialPosition(SubscriptionInitialPosition.Earliest);
                    break;
                case SubscriptionInitialPositions.Latest:
                    consumerBuilder.InitialPosition(SubscriptionInitialPosition.Latest);
                    break;
                default:
                    break;
            }

            switch (_messagingConsumerConfiguration.SubscriptionType)
            {
                case SubscriptionTypes.Exclusive:
                    consumerBuilder.SubscriptionType(SubscriptionType.Exclusive);
                    break;
                case SubscriptionTypes.Failover:
                    consumerBuilder.SubscriptionType(SubscriptionType.Failover);
                    break;
                case SubscriptionTypes.Shared:
                    consumerBuilder.SubscriptionType(SubscriptionType.Shared);
                    break;
                default:
                    break;
            }

            consumerState = ConsumerState.Disconnected;
            consumer = consumerBuilder.Create();
        }

        private async Task InitializeMessageReceiver(CancellationToken cancellationToken)
        {
            await Task.Run(async () =>
            {
                try
                {
                    if (_messagingConsumerConfiguration.ShouldResendUnacknowledgedMessages == true)
                        await consumer.RedeliverUnacknowledgedMessages();

                    await foreach (var message in consumer.Messages(cancellationToken))
                    {
                        var recordRaw = Encoding.UTF8.GetString(message.Data.ToArray());
                        var recordTopic = recordRaw.ToTopicObject<T>();

                        // Raise messageReceivedMessage event
                        bool? shouldAcknowledge = MessageReceived?.Invoke(this, new MessageReceivedArgs<T>(recordTopic, recordRaw, message.EventTimeAsDateTime, message.PublishTimeAsDateTime));
                        proccesedMessages++;
                        if (shouldAcknowledge.HasValue)
                        {
                            if (shouldAcknowledge.Value == true)
                            {
                                await consumer.Acknowledge(message, cancellationToken);
                            }
                        }
                        else
                        {
                            _logger.LogError($"Message failed to acknowledged, ShouldAcknowledge is undefined, message id {message.MessageId}, message details {recordRaw}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Fatal Error. {_instanceName} stopped consuming, error description {ex.Message}");
                    // await RetryConnection();
                }
            }

            , cancellationToken);
        }

        private void Monitor(ConsumerStateChanged stateChanged, CancellationToken cancellationToken)
        {
            string stateMessage;

            switch (stateChanged.ConsumerState)
            {
                case ConsumerState.Active:
                    stateMessage = "is active";
                    break;
                case ConsumerState.Closed:
                    stateMessage = "has closed";
                    break;
                case ConsumerState.Disconnected:
                    stateMessage = "is disconnected";
                    break;
                case ConsumerState.Faulted:
                    stateMessage = "has faulted";
                    break;
                case ConsumerState.Inactive:
                    stateMessage = "is inactive";
                    break;
                case ConsumerState.ReachedEndOfTopic:
                    stateMessage = "has reached end of topic";
                    break;
                case ConsumerState.Unsubscribed:
                    stateMessage = "is unsubscibed";
                    break;
                default:
                    stateMessage = $"has an unknown state '{stateChanged.ConsumerState}'";
                    break;
            }

            var topic = stateChanged.Consumer.Topic;
            if (stateChanged.ConsumerState == ConsumerState.Disconnected ||
                stateChanged.ConsumerState == ConsumerState.Faulted ||
                stateChanged.ConsumerState == ConsumerState.Inactive ||
                stateChanged.ConsumerState == ConsumerState.Unsubscribed)

                _logger.LogError($"bisko://{_instanceName} '{topic}' " + stateMessage);
            else
                _logger.LogInformation($"bisko://{_instanceName} '{topic}' " + stateMessage);

            consumerState = stateChanged.ConsumerState;
            StatusChanged?.Invoke(this, stateChanged.ConsumerState.ToString());

            if (stateChanged.ConsumerState == ConsumerState.Faulted
                || stateChanged.ConsumerState == ConsumerState.Disconnected)
                CheckIfConsumerIsConnected(true);
        }

        public void StopConsuming()
        {
            cancellationToken.Cancel();
            _logger.LogWarning($"{_instanceName} stopped consuming");
            consumerState = ConsumerState.Inactive;


            IsConsumerReading = false;
        }

        public void StartConsuming()
        {
            cancellationToken = new CancellationTokenSource();
            InitializeMessageReceiver(cancellationToken.Token);

            IsConsumerReading = true;
        }

        public CancellationTokenSource GetCancellationTokenSource()
        {
            return cancellationToken;
        }

        public string GetInitializedConsumerName()
        {
            return _instanceName;
        }

        public string GetConsumerState()
        {
            return consumerState.ToString();
        }

        private async Task RetryConnection(bool goFaster = false)
        {
            if (goFaster == true)
                Thread.Sleep(500);
            else
                Thread.Sleep(5000);

            if (consumerState != ConsumerState.Active)
            {
                cancellationToken.Cancel();
                await _pulsarClient.DisposeAsync();
                await consumer.DisposeAsync();

                CreateNewConsumer();
                StartConsuming();
            }
        }

        private void LogMetricsTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (todayDate.DayOfWeek != DateTime.Now.DayOfWeek)
            {
                proccesedMessages = 0;
                todayDate = DateTime.Now;
            }

            if (consumerState == ConsumerState.Active || consumerState == ConsumerState.ReachedEndOfTopic)
                _logger.LogInformation($"[{_instanceName} Report] -> Date '{todayDate:dd.MM.yyyy}', Status '{consumerState}', Proccesed messages '{proccesedMessages}'");
            else
                _logger.LogError($"[{_instanceName} Report] -> Date '{todayDate:dd.MM.yyyy}', Status '{consumerState}', Proccesed messages '{proccesedMessages}'");
        }
    }
}
