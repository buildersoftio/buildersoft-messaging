using Andy.X.Client.Extensions;
using Buildersoft.Messaging.Abstraction;
using Buildersoft.Messaging.Configuration;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Buildersoft.Messaging.App.Pulsar
{
    public class PulsarProducer<T> : IMessagingProducer<T>
    {
        public event IMessagingProducer<T>.StatusChangedHandler StatusChanged;

        private readonly IMessagingClient _messagingClient;
        private IPulsarClient _pulsarClient;
        private readonly ILogger<PulsarProducer<T>> _logger;
        private readonly MessagingProducerConfiguration<T> _messagingProducerConfiguration;
        private string _instanceNumber;
        private string _instanceName;

        private DotPulsar.Abstractions.IProducer<byte[]> producer;
        private CancellationTokenSource cancellationToken;
        private ProducerState producerState = ProducerState.Closed;

        private DateTime todayDate;
        private double proccesedMessages = 0;
        private System.Timers.Timer LogMetricsTimer;

        private List<string> brokerUrls;
        private int brokerIndex = 0;

        private ConcurrentQueue<T> unProducedMessageQueue;
        private System.Timers.Timer tryToProduceTimer;
        private bool isTryToProducerTimerStarted;
        private bool isProcessingOldMessages;

        public PulsarProducer(IMessagingClient messagingClient, MessagingProducerConfiguration<T> messagingProducerConfiguration)
        {
            todayDate = DateTime.Now;
            InitializeTryToProducerTimer();

            brokerUrls = messagingClient
               .GetBuilder()
               .MessagingConfiguration
               .GetBrokers();

            _messagingClient = messagingClient;
            _pulsarClient = new PulsarClient(_messagingClient.GetBuilder(), brokerUrls[brokerIndex]).GetClient() as IPulsarClient;

            _instanceNumber = new Random().Next(0, 1000000).ToString("D6");
            _instanceName = $"producer-{messagingProducerConfiguration.ProducerName}-{messagingProducerConfiguration.Topic}-{_instanceNumber}";

            _logger = messagingClient
               .GetLoggerFactory()
               .CreateLogger<PulsarProducer<T>>();

            _messagingProducerConfiguration = messagingProducerConfiguration;
            cancellationToken = new CancellationTokenSource();

            producer = _pulsarClient.NewProducer<byte[]>(Schema.ByteArray)
                    .Topic(messagingProducerConfiguration.BuildTopic())
                    .StateChangedHandler(Monitor)
                    .ProducerName($"{messagingProducerConfiguration.ProducerName}-{_instanceNumber}")
                    .Create();

            LogMetricsTimer = new System.Timers.Timer()
            {
                Interval = new TimeSpan(0, 5, 0).TotalMilliseconds,
                AutoReset = true
            };
            LogMetricsTimer.Elapsed += LogMetricsTimer_Elapsed;
            LogMetricsTimer.Start();

            CheckIfConsumerIsConnected(false);
        }

        private void InitializeTryToProducerTimer()
        {
            unProducedMessageQueue = new ConcurrentQueue<T>();
            tryToProduceTimer = new System.Timers.Timer() { Interval = new TimeSpan(0, 4, 0).TotalMilliseconds, AutoReset = true };
            tryToProduceTimer.Elapsed += TryToProduceTimer_Elapsed;
            tryToProduceTimer.Stop();
            isTryToProducerTimerStarted = false;
            isProcessingOldMessages = false;
        }

        private void CheckIfConsumerIsConnected(bool goFaster = false)
        {
            new Thread(async () =>
            {
                while (producerState == ProducerState.Disconnected || producerState == ProducerState.Closed || producerState == ProducerState.Faulted)
                {
                    await RetryConnection(goFaster);
                }
            }).Start();
        }

        public CancellationTokenSource GetCancellationTokenSource()
        {
            return cancellationToken;
        }

        public async Task<Guid> SendAsync(T tEntity, string key = "")
        {
            int tryAgainIterator = 1;
            while (producerState != ProducerState.Connected)
            {
                Thread.Sleep(300);
                _logger.LogWarning($"bisko://producers/{_instanceName} is disconnected, trying '{tryAgainIterator}' to produce message");
                if (tryAgainIterator == 3)
                {
                    if (_messagingProducerConfiguration.IsAllowedToStoreInMemoryIfConnectionFailes == true)
                    {
                        _logger.LogWarning($"bisko://producers/{_instanceName}|failed to produce|stored in memory");
                        unProducedMessageQueue.Enqueue(tEntity);
                        StartUnproducedTimer();
                        return Guid.NewGuid();
                    }
                    _logger.LogWarning($"bisko://producers/{_instanceName}|message failed|details '{tEntity.ToJson()}'");
                    return Guid.NewGuid();
                }
                tryAgainIterator++;
            }

            try
            {
                MessageId msgId;
                if (key == "")
                    msgId = await producer.Send(Encoding.UTF8.GetBytes(tEntity.ToJson()), cancellationToken.Token);
                else
                    msgId = await producer.NewMessage().Key(key).Send(Encoding.UTF8.GetBytes(tEntity.ToJson()), cancellationToken.Token);

                proccesedMessages++;
                return Guid.NewGuid();
            }
            catch (Exception ex)
            {
                _logger.LogError($"bisko://producers/{_messagingProducerConfiguration.ProducerName}-{_instanceNumber} : message failed to send, error description '{ex.Message}'");
            }
            return Guid.Empty;
        }

        private void Monitor(ProducerStateChanged stateChanged, CancellationToken cancellationToken)
        {
            var stateMessage = stateChanged.ProducerState switch
            {
                ProducerState.Connected => "is connected",
                ProducerState.Disconnected => "is disconnected",
                ProducerState.Closed => "has closed",
                ProducerState.Faulted => "has faulted",
                _ => $"has an unknown state '{stateChanged.ProducerState}'"
            };

            StatusChanged?.Invoke(this, stateChanged.ProducerState.ToString());
            producerState = stateChanged.ProducerState;

            var topic = stateChanged.Producer.Topic;
            if (stateChanged.ProducerState == ProducerState.Disconnected
                || stateChanged.ProducerState == ProducerState.Faulted
                || stateChanged.ProducerState == ProducerState.Closed)
            {
                _logger.LogError($"bisko://producers/{_messagingProducerConfiguration.ProducerName}-{_instanceNumber} '{topic}' " + stateMessage);
            }
            else
                _logger.LogInformation($"bisko://producers/{_messagingProducerConfiguration.ProducerName}-{_instanceNumber} '{topic}' " + stateMessage);

            if (stateChanged.ProducerState == ProducerState.Faulted
                || stateChanged.ProducerState == ProducerState.Disconnected)
                CheckIfConsumerIsConnected(true);

        }
        private async Task RetryConnection(bool goFaster = false)
        {
            if (goFaster == true)
                Thread.Sleep(1000);
            else
                Thread.Sleep(5000);

            if (producerState != ProducerState.Connected)
            {
                cancellationToken.Cancel();
                await _pulsarClient.DisposeAsync();
                await producer.DisposeAsync();

                _instanceNumber = new Random().Next(0, 1000000).ToString("D6");
                _instanceName = $"producer-{_messagingProducerConfiguration.Topic}-{_instanceNumber}";

                brokerIndex += 1;
                if (brokerUrls.Count() == brokerIndex)
                    brokerIndex = 0;

                _logger.LogWarning($"bisko://producers/{_messagingProducerConfiguration.ProducerName}-{_instanceNumber} : trying to connect to '{brokerUrls[brokerIndex]}'");


                _pulsarClient = new PulsarClient(_messagingClient.GetBuilder(), brokerUrls[brokerIndex]).GetClient() as IPulsarClient;
                cancellationToken = new CancellationTokenSource();

                producer = _pulsarClient.NewProducer<byte[]>(Schema.ByteArray)
                    .Topic(_messagingProducerConfiguration.BuildTopic())
                    .StateChangedHandler(Monitor)
                    .ProducerName($"{_messagingProducerConfiguration.ProducerName}-{_instanceNumber}")
                    .Create();
            }
        }

        private void LogMetricsTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (todayDate.DayOfWeek != DateTime.Now.DayOfWeek)
            {
                proccesedMessages = 0;
                todayDate = DateTime.Now;
            }

            if (producerState == ProducerState.Connected)
                _logger.LogInformation($"[{_instanceName} Report] -> Date '{todayDate:dd.MM.yyyy}', Status '{producerState}', Proccesed messages '{proccesedMessages}', InMemory unproccesed '{unProducedMessageQueue.Count}'");
            else
                _logger.LogError($"[{_instanceName} Report] -> Date '{todayDate:dd.MM.yyyy}', Status '{producerState}', Proccesed messages '{proccesedMessages}', InMemory unproccesed '{unProducedMessageQueue.Count}'");
        }


        private void TryToProduceTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            // Ignore the second request if, the first request has not finished already.
            if (isProcessingOldMessages != true && producerState == ProducerState.Connected)
            {
                _logger.LogInformation($"bisko://producers/{_instanceName}|InMemory-Store|started to produce in-memory messages");
                isProcessingOldMessages = true;
                new Thread(() => ProduceOldMessages()).Start();
            }
        }

        private async void ProduceOldMessages()
        {
            isProcessingOldMessages = true;

            while (unProducedMessageQueue.Count > 0)
            {
                Thread.Sleep(100);

                T unProcessed;
                bool isReturned = unProducedMessageQueue.TryDequeue(out unProcessed);
                if (isReturned)
                    await this.SendAsync(unProcessed);
            }

            tryToProduceTimer.Stop();

            isProcessingOldMessages = false;
            isTryToProducerTimerStarted = false;
        }

        private void StartUnproducedTimer()
        {
            if (isTryToProducerTimerStarted != true)
            {
                isTryToProducerTimerStarted = true;
                tryToProduceTimer.Start();
            }
        }
    }
}
