﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Amazon.SQS;

namespace ServiceStack.Aws.Sqs
{
    public interface ISqsQueueManager: IDisposable
    {
        SqsConnectionFactory ConnectionFactory { get; }
        int DefaultReceiveWaitTime { get; set; }
        int DefaultVisibilityTimeout { get; set; }
        bool DisableBuffering { get; set; }

        ConcurrentDictionary<string, SqsQueueDefinition> QueueNameMap { get; }
        IAmazonSQS SqsClient { get; }

        SqsQueueDefinition CreateQueue(string queueName, int? visibilityTimeoutSeconds = null, int? receiveWaitTimeSeconds = null, bool? disasbleBuffering = null, SqsRedrivePolicy redrivePolicy = null, bool isFifoQueue = false);
        SqsQueueDefinition CreateQueue(string queueName, SqsMqWorkerInfo info, string redriveArn = null);
        void DeleteQueue(string queueName, bool isFifoQueue = false);
        SqsQueueDefinition GetOrCreate(string queueName, int? visibilityTimeoutSeconds = null, int? receiveWaitTimeSeconds = null, bool? disasbleBuffering = null, bool isFifoQueue = false);
        SqsQueueDefinition GetQueueDefinition(string queueName, bool forceRecheck = false, bool isFifoQueue = false);
        string GetQueueUrl(string queueName, bool forceRecheck = false, bool isFifoQueue = false);
        void PurgeQueue(string queueName, bool isFifoQueue = false);
        void PurgeQueues(IEnumerable<string> queueNames);
        bool QueueExists(string queueName, bool forceRecheck = false, bool isFifoQueue = false);
        int RemoveEmptyTemporaryQueues(long createdBefore);
    }
}