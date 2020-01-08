using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Xunit;

namespace NetSQS.Mock.Tests
{
    public class MockTests
    {
        private const string FifoQueueName = "mockQueue.fifo";

        [Fact]
        public async Task Mock_ShouldListQueues_WhenClientAvailable()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            var queues = await client.ListQueuesAsync();
            Assert.NotNull(queues);
        }

        [Fact]
        public async Task MockCreateStandardFifoQueueAsync_ShouldThrowArgumentException_WhenQueueNameDoesNotEndWithFifo()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");

            await Assert.ThrowsAsync<ArgumentException>(() => client.CreateStandardFifoQueueAsync("test"));
        }

        [Fact]
        public async Task MockCreateStandardQueueAsync_ShouldThrowArgumentException_WhenQueueNameEndsWithFifo()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");

            await Assert.ThrowsAsync<ArgumentException>(() => client.CreateStandardQueueAsync("test.fifo"));
        }

        [Fact]
        public async Task MockSendMessageAsync_ShouldPutAMessageOnTheQueue_WhenQueueExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            var messageId = await client.SendMessageAsync("Hello World!", FifoQueueName);

            Assert.NotNull(messageId);
        }

        [Fact]
        public async Task MockSendMessageBatchAsync_ShouldPutAMessageOnTheQueue_WhenQueueExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var batch = new List<BatchMessageRequest>
            {
                new BatchMessageRequest("testMessage1"),
                new BatchMessageRequest("testMessage2", new Dictionary<string, string>
                {
                    {"key2", "value2"}
                })
            }.ToArray();

            var response = await client.SendMessageBatchAsync(batch, FifoQueueName);

            Assert.Empty(response.GetFailed());
            Assert.Equal(2, response.GetSuccessful().Length);
            Assert.True(response.Success);

            Assert.True(response.SendResults[0].Success);
            Assert.Equal("testMessage1", response.SendResults[0].MessageRequest.Message);
            Assert.Null(response.SendResults[0].Error);

            Assert.True(response.SendResults[1].Success);
            Assert.Equal("testMessage2", response.SendResults[1].MessageRequest.Message);
            Assert.Equal("value2", response.SendResults[1].MessageRequest.MessageAttributes["key2"]);
            Assert.Null(response.SendResults[1].Error);
        }

        [Fact]
        public async Task MockSendMessageAsync_ShouldThrowQueueDoesNotExistException_WhenQueueDoesNotExist()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");

            await Assert.ThrowsAsync<QueueDoesNotExistException>(() => client.SendMessageAsync("test", "test"));
        }

        private bool _messagePicked;

        [Fact]
        public async Task MockStartMessageReceiver_ShouldRetrieveMessageWithSyncProcessor_WhenQueueAndMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;


            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                message =>
                {
                    Assert.Equal("Hello World!", message);
                    _messagePicked = true;
                    return true;
                }, cancellationToken);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationTokenSource.Cancel();
            Assert.True(_messagePicked);
            _messagePicked = false;
        }

        [Fact]
        public async Task MockStartMessageReceiver_ShouldRetrieveMessageWithAsyncProcessor_WhenQueueAndMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (message) =>
                {
                    Assert.Equal("Hello World!", message);
                    _messagePicked = true;
                    return await Task.FromResult(true);
                }, cancellationToken);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationTokenSource.Cancel();
            Assert.True(_messagePicked);
            _messagePicked = false;
        }

        [Fact]
        public void MockStartMessageReceiver_ShouldThrowQueueDoesNotExistException_WhenQueueDoesNotExist()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            Assert.ThrowsAsync<QueueDoesNotExistException>(() =>
                client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                    message => true, cancellationToken));
        }

        [Fact]
        public async Task MockDeleteQueue_ShouldDeleteQueue_IfQueueExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var queuesOnClientBeforeDeletion = await client.ListQueuesAsync();
            await client.DeleteQueueAsync(FifoQueueName);
            var queuesOnClientAfterDeletion = await client.ListQueuesAsync();

            Assert.Single(queuesOnClientBeforeDeletion);
            Assert.Empty(queuesOnClientAfterDeletion);
        }

        [Fact]
        public async Task MockGetMessagesOnQueue_ShouldContainSentMessage_WhenQueueExists()
        {
            var queueName = FifoQueueName;
            var messageContents = "Hello World!";
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(queueName);
            await client.SendMessageAsync(messageContents, queueName);

            var actual = client.GetMessages(queueName);

            Assert.Single(actual);
            Assert.Equal(messageContents, actual.First().Message);
        }

        [Fact]
        public async Task MockQueue_ShouldNotRemoveMessageFromQueue_UntilAcked()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (message) =>
                {
                    Assert.Single(client.GetMessages(FifoQueueName));
                    Assert.True(client.GetMessages(FifoQueueName).First().IsLocked);
                    Assert.Equal("Hello World!", message);
                    _messagePicked = true;
                    return await Task.FromResult(true);
                }, cancellationToken);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationTokenSource.Cancel();
            Assert.True(_messagePicked);
            Assert.Empty(client.GetMessages(FifoQueueName));
            _messagePicked = false;
        }

        [Fact]
        public async Task MockQueue_ShouldNotRemoveMessageFromQueue_IfFalseIsReturned()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (message) =>
                {
                    Assert.Single(client.GetMessages(FifoQueueName));
                    Assert.True(client.GetMessages(FifoQueueName).First().IsLocked);
                    Assert.Equal("Hello World!", message);
                    _messagePicked = true;
                    return await Task.FromResult(false);
                }, cancellationToken);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);

            cancellationTokenSource.Cancel();
            Assert.True(_messagePicked);
            Assert.Single(client.GetMessages(FifoQueueName));
            Assert.False(client.GetMessages(FifoQueueName).First().IsLocked);
            _messagePicked = false;
        }

        [Fact]
        public async Task AwaitMessageProcessedAttempt_ShouldWaitUntilMessageProcessorHasBeenCalled_WhenMessageProcessorSucceeds()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (message) =>
                {
                    await Task.Delay(1000);
                    return true;
                }, cancellationToken);

            await client.SendMessageAsync("Hello World!", FifoQueueName);
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            stopwatch.Stop();

            Assert.True(stopwatch.ElapsedMilliseconds > 900);
        }

        [Fact]
        public async Task AwaitMessageProcessedAttempt_ShouldWaitUntilMessageProcessorHasBeenCalled_WhenMessageProcessorFails()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (message) =>
                {
                    await Task.Delay(1000);
                    return false;
                }, cancellationToken);

            await client.SendMessageAsync("Hello World!", FifoQueueName);
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            stopwatch.Stop();

            Assert.True(stopwatch.ElapsedMilliseconds > 900);
        }

        [Fact]
        public async Task SQSMessage_ShouldRemoveMessageFromQueue_WhenAcked()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (ISQSMessage message) =>
                {
                    Assert.Equal("Bar", message.Body);

                    await message.Ack();
                }, cancellationToken);

            await client.SendMessageAsync("Bar", FifoQueueName);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationTokenSource.Cancel();

            Assert.Empty(client.GetMessages(FifoQueueName));
        }

        [Fact]
        public async Task SQSMessage_ShouldNotRemoveMessageFromQueue_WhenNotAcked()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            client.StartMessageReceiver(FifoQueueName, new MessageReceiverOptions(),
                async (ISQSMessage message) => { Assert.Equal("Bar", message.Body); }, cancellationToken);

            await client.SendMessageAsync("Bar", FifoQueueName);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);

            cancellationTokenSource.Cancel();
            Assert.Single(client.GetMessages(FifoQueueName));
        }

        [Fact]
        public async Task GetNumberOfMessagesOnQueue_ShouldReturnNumberOfMessagesOnQueue_WhenMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            await client.SendMessageAsync("Foo", FifoQueueName);

            var actual = await client.GetNumberOfMessagesOnQueue(FifoQueueName);

            Assert.Equal(FifoQueueName, actual.QueueName);
            Assert.Equal(1, actual.ApproximateNumberOfMessages);
        }
    }
}
