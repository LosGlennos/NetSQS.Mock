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

        public async Task<string> SendMessageAsync(string message, string queueName, Dictionary<string, string> messageAttributes = null)
        {
            MockClientObject.Queues.TryGetValue(queueName, out var queue);
            if (queue == null)
            {
                throw new QueueDoesNotExistException($"Queue: {queueName} does not exist");
            }

            queue.Enqueue(new QueueMessage
            {
                IsLocked = false,
                Message = message,
                MessageAttributes = messageAttributes
            });

            MockClientObject.Queues[queueName] = queue;
            MockClientObject.QueueMessageProcessed[queueName] = false;

            return "theMessageId";
        }

        public async Task<IBatchResponse> SendMessageBatchAsync(BatchMessageRequest[] batchMessages, string queueName)
        {
            var response = new BatchResponse();
            response.SendResults = new BatchResponse.SendResult[0];

            foreach (var messageRequest in batchMessages)
            {
                var messageResult = new BatchResponse.SendResult {MessageRequest = messageRequest};
                try
                {
                    messageResult.MessageId = await SendMessageAsync(messageRequest.Message, queueName, messageRequest.MessageAttributes);
                    messageResult.Success = true;
                }
                catch (Exception e)
                {
                    messageResult.Error = e.ToString();
                }

                response.SendResults = response.SendResults.Append(messageResult).ToArray();
            }

            response.Success = response.SendResults.All(x => x.Success);

            return response;
        }

        public async Task CreateStandardQueueAsync(string queueName)
        {
            await CreateQueueAsync(queueName, false, true);
        }

        public async Task CreateStandardFifoQueueAsync(string queueName)
        {
            await CreateQueueAsync(queueName, true, true);
        }

        public async Task CreateQueueAsync(string queueName, bool isFifo, bool isEncrypted, int retentionPeriod = 345600,
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

        public Task StartMessageReceiver(string queueName, MessageReceiverOptions options, Func<string, bool> messageProcessor,
            CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, options, async (arg) => messageProcessor(arg), cancellationToken);
        }

        public Task StartMessageReceiver(string queueName, MessageReceiverOptions options, Func<string, Task<bool>> asyncMessageProcessor,
            CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, options, asyncMessageProcessor, cancellationToken);
        }

        public Task StartMessageReceiver(string queueName, MessageReceiverOptions options, Action<ISQSMessage> messageProcessor,
            CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, options, async (arg) => messageProcessor(arg), cancellationToken);
        }

        public Task StartMessageReceiver(string queueName, MessageReceiverOptions options, Func<ISQSMessage, Task> asyncMessageProcessor,
            CancellationToken cancellationToken)
        {
            return StartMessageReceiverInternal(queueName, options, asyncMessageProcessor, cancellationToken);
        }

        private Task StartMessageReceiverInternal(string queueName, MessageReceiverOptions options,
            Func<string, Task<bool>> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            if (options.WaitForQueue) WaitForQueue(queueName, options);

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

                    var successful = await asyncMessageProcessor(message.Message);
                    if (successful) queue.Dequeue();
                    else UnlockFirstMessageInQueue(queue);
                    MockClientObject.QueueMessageProcessed[queueName] = true;
                }
            }, cancellationToken);
        }

        private Task StartMessageReceiverInternal(string queueName, MessageReceiverOptions options,
            Func<ISQSMessage, Task> asyncMessageProcessor, CancellationToken cancellationToken)
        {
            if (options.WaitForQueue) WaitForQueue(queueName, options);

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
                        Body = message.Message,
                        MessageAttributes = message.MessageAttributes
                    };

                    await asyncMessageProcessor(sqsMessage);
                    UnlockFirstMessageInQueue(queue);
                    MockClientObject.QueueMessageProcessed[queueName] = true;
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
            if (queue.Count > 0) queue.Peek().IsLocked = true;
        }

        private void UnlockFirstMessageInQueue(Queue<QueueMessage> queue)
        {
            if (queue.Count > 0) queue.Peek().IsLocked = false;
        }

        private bool IsFifoQueue(string queueName)
        {
            return queueName.EndsWith(".fifo");
        }

        private void WaitForQueue(string queueName, MessageReceiverOptions options)
        {

            for (var i = 0; i < options.WaitForQueueTimeoutSeconds; i++)
            {
                MockClientObject.Queues.TryGetValue(queueName, out var queue);

                if (queue != null) return;

                Task.Delay(1000).Wait();
            }

            throw new QueueDoesNotExistException($"Queue {queueName} does not exist");
        }

        private static QueueMessage PeekFirstMessageInQueue(Queue<QueueMessage> queue)
        {
            var message = queue.Peek();
            return message;
        }

        public async Task DeleteMessageAsync(string queueName, string receiptHandle)
        {
            MockClientObject.Queues.TryGetValue(queueName, out var queue);
            await Task.Run(() => queue.Dequeue());
            MockClientObject.QueueMessageProcessed[queueName] = true;
        }

        public async Task<NumberOfMessagesResponse> GetNumberOfMessagesOnQueue(string queueName)
        {
            if (!MockClientObject.Queues.ContainsKey(queueName))
                throw new QueueDoesNotExistException($"Queue {queueName} does not exist");

            return new NumberOfMessagesResponse
            {
                QueueName = queueName,
                ApproximateNumberOfMessages = MockClientObject.Queues[queueName].Count
            };
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
            public Dictionary<string, string> MessageAttributes { get; set; }
        }
    }
}
