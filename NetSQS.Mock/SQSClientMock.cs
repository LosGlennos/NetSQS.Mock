using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;

namespace NetSQS.Mock
{

    public class SQSClientMock : ISQSClient
    {
        private SQSClientMockObject MockClientObject { get; set; }

        public SQSClientMock(string mockEndpoint, string mockRegion)
        {
            MockClientObject = new SQSClientMockObject
            {
                Endpoint = mockEndpoint,
                Region = mockRegion
            };
        }

        public Queue<QueueMessage> GetMessages(string queueName)
        {
            return MockClientObject.Queues[queueName];
        }

        public async Task<string> SendMessageAsync(string message, string queueName)
        {
            MockClientObject.Queues.TryGetValue(queueName, out var queue);
            if (queue == null)
            {
                throw new QueueDoesNotExistException($"Queue: {queueName} does not exist");
            }

            queue.Enqueue(new QueueMessage
            {
                IsLocked = false,
                Message = message
            });

            MockClientObject.Queues[queueName] = queue;
            MockClientObject.QueueMessageProcessed[queueName] = false;

            return "theMessageId";
        }

        public async Task<string> CreateStandardQueueAsync(string queueName)
        {
            return await CreateQueueAsync(queueName, false, false);
        }

        public async Task<string> CreateStandardFifoQueueAsync(string queueName)
        {
            return await CreateQueueAsync(queueName, true, true);
        }

        public async Task<string> CreateQueueAsync(string queueName, bool isFifo, bool isEncrypted, int retentionPeriod = 345600,
            int visibilityTimeout = 30)
        {
            if (isFifo && !queueName.EndsWith(".fifo"))
            {
                throw new ArgumentException("Queue name must end with '.fifo'", nameof(queueName));
            }

            if (!isFifo && queueName.EndsWith(".fifo"))
            {
                throw new ArgumentException("Queue name is not allowed to end with .fifo if it is not specified as a FIFO-queue", nameof(queueName));
            }

            MockClientObject.Queues.Add(queueName, new Queue<QueueMessage>());
            return await Task.FromResult($"https://{MockClientObject.Region}/queue/{queueName}");
        }

        public Task<bool> DeleteQueueAsync(string queueName)
        {
            return Task.FromResult(MockClientObject.Queues.Remove(queueName));
        }

        public async Task<List<string>> ListQueuesAsync()
        {
            var queues = MockClientObject.Queues.Keys.Select(x => x.ToString()).ToList();
            return await Task.FromResult(queues);
        }

        [Obsolete("Use StartMessageReceiver-method that takes cancellation token as a parameter. This method will be removed in future releases", true)]
        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            int numRetries, int minBackOff, int maxBackOff, Func<string, Task<bool>> asyncMessageProcessor)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return StartMessageReceiverInternal(queueName, asyncMessageProcessor);
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll, int numRetries,
            int minBackOff, int maxBackOff, Func<string, Task<bool>> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return StartMessageReceiverInternal(queueName, asyncMessageProcessor, cancellationToken);
        }

        [Obsolete("Use StartMessageReceiver-method that takes cancellation token as a parameter. This method will be removed in future releases", true)]
        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            int numRetries, int minBackOff, int maxBackOff, Func<string, bool> messageProcessor)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return StartMessageReceiverInternal(queueName, async (arg) => messageProcessor(arg));
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll, int numRetries,
            int minBackOff, int maxBackOff, Func<string, bool> messageProcessor, CancellationToken cancellationToken)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return StartMessageReceiverInternal(queueName, async (arg) => messageProcessor(arg), cancellationToken);
        }

        [Obsolete("Use StartMessageReceiver-method that takes cancellation token as a parameter. This method will be removed in future releases", true)]
        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, Task<bool>> asyncMessageProcessor)
        {
            return StartMessageReceiverInternal(queueName, asyncMessageProcessor);
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, Task<bool>> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, asyncMessageProcessor, cancellationToken);
        }

        [Obsolete("Use StartMessageReceiver-method that takes cancellation token as a parameter. This method will be removed in future releases", true)]
        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, bool> messageProcessor)
        {
            return StartMessageReceiverInternal(queueName, async (arg) => messageProcessor(arg));
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll, Func<string, bool> messageProcessor,
            CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, async (arg) => messageProcessor(arg), cancellationToken);
        }

        private CancellationTokenSource StartMessageReceiverInternal(string queueName, Func<string, Task<bool>> asyncMessageProcessor)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            StartMessageReceiverInternal(queueName, asyncMessageProcessor, cancellationToken);

            return cancellationTokenSource;
        }

        private Task StartMessageReceiverInternal(string queueName, Func<string, Task<bool>> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    MockClientObject.Queues.TryGetValue(queueName, out var queue);
                    if (queue == null)
                    {
                        throw new QueueDoesNotExistException($"Queue {queueName} does not exist.");
                    }

                    if (!queue.Any() || queue.Peek().IsLocked)
                    {
                        await Task.Delay(10, cancellationToken);
                        continue;
                    }

                    if (IsFifoQueue(queueName)) LockFirstMessageInQueue(queue);
                    var message = PeekFirstMessageInQueue(queue);

                    var successful = await asyncMessageProcessor(message);
                    if (successful) queue.Dequeue();
                    else UnlockFirstMessageInQueue(queue);
                    MockClientObject.QueueMessageProcessed[queueName] = true;
                }
            }, cancellationToken);
        }

        private Task StartMessageReceiverInternal(string queueName, Func<ISQSMessage, Task> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    MockClientObject.Queues.TryGetValue(queueName, out var queue);
                    if (queue == null)
                    {
                        throw new QueueDoesNotExistException($"Queue {queueName} does not exist.");
                    }

                    if (!queue.Any() || queue.Peek().IsLocked)
                    {
                        await Task.Delay(10, cancellationToken);
                        continue;
                    }

                    if (IsFifoQueue(queueName)) LockFirstMessageInQueue(queue);
                    var message = PeekFirstMessageInQueue(queue);
                    var sqsMessage = new SQSMessageMock(this, queueName, "MockReceiptHandle") 
                    {
                        Body = message
                    };

                    await asyncMessageProcessor(sqsMessage);
                    UnlockFirstMessageInQueue(queue);
                }
            }, cancellationToken);
        }

        /// <summary>
        /// Wait until the message listener has attempted to process one message,
        /// no matter if the attempt was successful or not.
        /// This is reset by putting a message on the queue using SendMessageAsync.
        /// </summary>
        /// <param name="queueName"></param>
        /// <returns></returns>
        public async Task AwaitMessageProcessedAttempt(string queueName)
        {
            if (!MockClientObject.QueueMessageProcessed.ContainsKey(queueName))
                throw new QueueDoesNotExistException($"Queue {queueName} does not exist");

            while (!MockClientObject.QueueMessageProcessed[queueName])
            {
                await Task.Delay(10);
            }
        }

        private void LockFirstMessageInQueue(Queue<QueueMessage> queue)
        {
            queue.Peek().IsLocked = true;
        }

        private void UnlockFirstMessageInQueue(Queue<QueueMessage> queue)
        {
            queue.Peek().IsLocked = false;
        }

        private bool IsFifoQueue(string queueName)
        {
            return queueName.EndsWith(".fifo");
        }

        private void WaitForQueue(string queueName, int numRetries, int minBackOff, int maxBackOff)
        {
            for (var i = 0; i < numRetries; i++)
            {
                MockClientObject.Queues.TryGetValue(queueName, out var queue);
                if (queue == null && i == numRetries - 1)
                {
                    throw new QueueDoesNotExistException($"Queue {queueName} does not exist");
                }

                if (queue != null)
                    break;

                var timeSleep = new Random().Next(maxBackOff - minBackOff) + minBackOff;
                var timeSleepMilliseconds = (int)TimeSpan.FromSeconds(timeSleep).TotalMilliseconds;
                Task.Delay(timeSleepMilliseconds).Wait();
            }
        }

        private static string PeekFirstMessageInQueue(Queue<QueueMessage> queue)
        {
            var message = queue.Peek().Message;
            return message;
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTimeSeconds, int maxNumberOfMessagesPerPoll, Action<ISQSMessage> messageProcessor, CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, async (arg) => messageProcessor(arg), cancellationToken);
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTimeSeconds, int maxNumberOfMessagesPerPoll, Func<ISQSMessage, Task> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, asyncMessageProcessor, cancellationToken);
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTimeSeconds, int maxNumberOfMessagesPerPoll, int numRetries, int minBackOff, int maxBackOff, Action<ISQSMessage> messageProcessor, CancellationToken cancellationToken)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);
            return StartMessageReceiverInternal(queueName, async (arg) => messageProcessor(arg), cancellationToken);
        }

        public Task StartMessageReceiver(string queueName, int pollWaitTimeSeconds, int maxNumberOfMessagesPerPoll, int numRetries, int minBackOff, int maxBackOff, Func<ISQSMessage, Task> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);
            return StartMessageReceiverInternal(queueName, asyncMessageProcessor, cancellationToken);
        }

        public async Task DeleteMessageAsync(string queueName, string receiptHandle)
        {
            MockClientObject.Queues.TryGetValue(queueName, out var queue);
            await Task.Run(() => queue.Dequeue());
            MockClientObject.QueueMessageProcessed[queueName] = true;
        }

        private class SQSClientMockObject
        {
            public string Endpoint { get; set; }
            public string Region { get; set; }
            public Dictionary<string, Queue<QueueMessage>> Queues { get; } = new Dictionary<string, Queue<QueueMessage>>();
            public Dictionary<string, bool> QueueMessageProcessed { get; } = new Dictionary<string, bool>();
        }

        public class QueueMessage
        {
            public string Message { get; set; }
            public bool IsLocked { get; set; }
        }
    }
}
