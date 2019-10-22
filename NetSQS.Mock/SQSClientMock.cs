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
                Region = mockRegion,
                Queues = new Dictionary<string, Queue<QueueMessage>>()
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
            if (isFifo)
            {
                if (queueName.Length <= 5 || queueName.Substring(queueName.Length - 5) != ".fifo")
                {
                    throw new ArgumentException("Queue name must end with '.fifo'", nameof(queueName));
                }
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

        public CancellationTokenSource PollQueueAsync(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, Task<bool>> asyncMessageProcessor)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    MockClientObject.Queues.TryGetValue(queueName, out var queue);
                    if (queue == null)
                    {
                        throw new QueueDoesNotExistException($"Queue {queueName} does not exist.");
                    }

                    if (!queue.Any()) continue;
                    if (queue.Peek().IsLocked) continue;
                    
                    var message = LockAndPeekFirstMessageInQueue(queue);

                    var successful = await asyncMessageProcessor(message);
                    if (successful) queue.Dequeue();
                }
            }, cancellationToken);

            return cancellationTokenSource;
        }

        public CancellationTokenSource PollQueueAsync(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, bool> messageProcessor)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            Task.Run(() =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    MockClientObject.Queues.TryGetValue(queueName, out var queue);
                    if (queue == null)
                    {
                        throw new QueueDoesNotExistException($"Queue {queueName} does not exist.");
                    }

                    if (!queue.Any()) continue;
                    if (queue.Peek().IsLocked) continue;
                    
                    var message = LockAndPeekFirstMessageInQueue(queue);

                    var successful = messageProcessor(message);
                    if (successful) queue.Dequeue();
                }
            }, cancellationToken);

            return cancellationTokenSource;
        }

        public async Task<CancellationTokenSource> PollQueueWithRetryAsync(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll, int numRetries,
            int minBackOff, int maxBackOff, Func<string, Task<bool>> asyncMessageProcessor)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return PollQueueAsync(queueName, pollWaitTime, maxNumberOfMessagesPerPoll, asyncMessageProcessor);
        }

        public async Task<CancellationTokenSource> PollQueueWithRetryAsync(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll, int numRetries,
            int minBackOff, int maxBackOff, Func<string, bool> messageProcessor)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return PollQueueAsync(queueName, pollWaitTime, maxNumberOfMessagesPerPoll, messageProcessor);
        }

        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            int numRetries, int minBackOff, int maxBackOff, Func<string, Task<bool>> asyncMessageProcessor)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return StartMessageReceiver(queueName, pollWaitTime, maxNumberOfMessagesPerPoll, asyncMessageProcessor);
        }

        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            int numRetries, int minBackOff, int maxBackOff, Func<string, bool> messageProcessor)
        {
            WaitForQueue(queueName, numRetries, minBackOff, maxBackOff);

            return StartMessageReceiver(queueName, pollWaitTime, maxNumberOfMessagesPerPoll, messageProcessor);
        }

        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, Task<bool>> asyncMessageProcessor)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    MockClientObject.Queues.TryGetValue(queueName, out var queue);
                    if (queue == null)
                    {
                        throw new QueueDoesNotExistException($"Queue {queueName} does not exist.");
                    }

                    if (!queue.Any()) continue;
                    if (queue.Peek().IsLocked) continue;

                    var message = LockAndPeekFirstMessageInQueue(queue);

                    var successful = await asyncMessageProcessor(message);
                    if (successful) queue.Dequeue();
                }
            }, cancellationToken);

            return cancellationTokenSource;
        }

        public CancellationTokenSource StartMessageReceiver(string queueName, int pollWaitTime, int maxNumberOfMessagesPerPoll,
            Func<string, bool> messageProcessor)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            Task.Run(() =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    MockClientObject.Queues.TryGetValue(queueName, out var queue);
                    if (queue == null)
                    {
                        throw new QueueDoesNotExistException($"Queue {queueName} does not exist.");
                    }

                    if (!queue.Any()) continue;
                    if (queue.Peek().IsLocked) continue;

                    var message = LockAndPeekFirstMessageInQueue(queue);

                    var successful = messageProcessor(message);
                    if (successful) queue.Dequeue();
                }
            }, cancellationToken);

            return cancellationTokenSource;
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

        private static string LockAndPeekFirstMessageInQueue(Queue<QueueMessage> queue)
        {
            queue.Peek().IsLocked = true;
            var message = queue.Peek().Message;
            return message;
        }

        private class SQSClientMockObject
        {
            public string Endpoint { get; set; }
            public string Region { get; set; }
            public Dictionary<string, Queue<QueueMessage>> Queues { get; set; }
        }

        public class QueueMessage
        {
            public string Message { get; set; }
            public bool IsLocked { get; set; }
        }
    }
}
