using System;

namespace Buildersoft.Messaging.Domain.Message
{
    public class MessageReceivedArgs<T>
    {
        public T TopicRecordData { get; private set; }
        public string RecordRaw { get; private set; }
        public DateTime ReceivedDate { get; private set; }
        public DateTime SentDate { get; private set; }


        public MessageReceivedArgs(T topicRecordData, string recordRaw, DateTime receivedDate, DateTime sentDate)
        {
            TopicRecordData = topicRecordData;
            RecordRaw = recordRaw;
            ReceivedDate = receivedDate;
            SentDate = sentDate;
        }
    }
}
