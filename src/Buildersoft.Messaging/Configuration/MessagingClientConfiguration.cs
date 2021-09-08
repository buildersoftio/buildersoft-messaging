using Buildersoft.Messaging.Configuration.Parameters;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Buildersoft.Messaging.Configuration
{
    public class MessagingClientConfiguration
    {
        private List<string> brokerUrls;
        public DistributedStreamingPlatform DistributedStreamingPlatform { get; set; }
        public TimeSpan RetryInterval { get; set; }
        public Parameters.Environment MessagingEnvironment { get; set; }
        public ILoggerFactory LoggerFactory { get; set; }

        public void AddBrokerUrl(string brokerUrl)
        {
            brokerUrls.Add(brokerUrl);
        }

        public List<string> GetBrokers()
        {
            return brokerUrls;
        }

        public string GetRandomBroker()
        {
            return brokerUrls[new Random().Next(0, brokerUrls.Count())];
        }

        public void AddBrokersAsCSVRow(string brokersCsv)
        {
            brokerUrls.AddRange(brokersCsv.Split(","));
        }

        public string GetBrokersAsCSV()
        {
            return String.Join(",", brokerUrls.Select(x => x.ToString()).ToArray());
        }

        public MessagingClientConfiguration()
        {
            brokerUrls = new List<string>();
            DistributedStreamingPlatform = DistributedStreamingPlatform.Pulsar;
            RetryInterval = new TimeSpan(0, 0, 3);
            LoggerFactory = new LoggerFactory();
            MessagingEnvironment =  Parameters.Environment.Production;
            // Has to be null, because is mandatory field;
        }
    }

}
