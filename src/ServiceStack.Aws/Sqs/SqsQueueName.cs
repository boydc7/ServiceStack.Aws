using System;
using System.Collections.Concurrent;

namespace ServiceStack.Aws.Sqs
{
    public static class SqsQueueNames
    {
        private static readonly ConcurrentDictionary<string, SqsQueueName> queueNameMap = new ConcurrentDictionary<string, SqsQueueName>();

        public const string FifoQueueSuffix = ".fifo";

        public static SqsQueueName GetSqsQueueName(string originalQueueName, bool isFifoQueue = false)
        {
            if (queueNameMap.TryGetValue(originalQueueName, out var sqn))
                return sqn;

            isFifoQueue = isFifoQueue || originalQueueName.EndsWith(FifoQueueSuffix, StringComparison.OrdinalIgnoreCase);

            sqn = new SqsQueueName(originalQueueName, isFifoQueue);

            return queueNameMap.TryAdd(originalQueueName, sqn)
                ? sqn
                : queueNameMap[originalQueueName];
        }
    }

    public class SqsQueueName : IEquatable<SqsQueueName>
    {
        public SqsQueueName(string originalQueueName, bool isFifoQueue)
        {
            QueueName = originalQueueName;
            AwsQueueName = originalQueueName.ToValidQueueName(isFifoQueue);
        }

        public string QueueName { get; }
        public string AwsQueueName { get; private set; }

        public bool Equals(SqsQueueName other)
        {
            return other != null &&
                   QueueName.Equals(other.QueueName, StringComparison.OrdinalIgnoreCase);
        }
        
        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            return obj is SqsQueueName asQueueName && Equals(asQueueName);
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override string ToString()
        {
            return QueueName;
        }
    }
}