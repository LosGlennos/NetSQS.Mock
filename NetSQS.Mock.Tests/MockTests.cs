using System;
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
        public async Task MockCreateQueueAsync_ShouldReturnQueueUrl_WhenCreatingQueue()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            var queueUrl = await client.CreateQueueAsync("test", false, false);

            Assert.Equal("https://mockRegion/queue/test", queueUrl);
        }

        [Fact]
        public async Task MockCreateStandardQueueAsync_ShouldReturnQueueUrl_WhenCreatingStandardQueue()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            var queueUrl = await client.CreateStandardQueueAsync("test");

            Assert.Equal("https://mockRegion/queue/test", queueUrl);
        }

        [Fact]
        public async Task MockCreateStandardFifoQueueAsync_ShouldReturnQueueUrl_WhenCreatingFifoQueue()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            var queueUrl = await client.CreateStandardFifoQueueAsync("test.fifo");

            Assert.Equal("https://mockRegion/queue/test.fifo", queueUrl);
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
        public async Task MockSendMessageAsync_ShouldThrowQueueDoesNotExistException_WhenQueueDoesNotExist()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");

            await Assert.ThrowsAsync<QueueDoesNotExistException>(() => client.SendMessageAsync("test", "test"));
        }

        private bool _messagePicked;

        [Fact]
        public async Task MockStartMessageReceiver_ShouldRetrieveMessage_WhenQueueAndMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationToken = client.StartMessageReceiver(FifoQueueName, 1, 1, message =>
            {
                Assert.Equal("Hello World!", message);
                _messagePicked = true;
                return true;
            });

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationToken.Cancel();
            Assert.True(_messagePicked);
            _messagePicked = false;
        }

        [Fact]
        public async Task MockStartMessageReceiver_ShouldRetrieveMessageWithAsyncProcessor_WhenQueueAndMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationToken = client.StartMessageReceiver(FifoQueueName, 1, 1, async (message) =>
            {
                Assert.Equal("Hello World!", message);
                _messagePicked = true;
                return await Task.FromResult(true);
            });

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationToken.Cancel();
            Assert.True(_messagePicked);
            _messagePicked = false;
        }

        [Fact]
        public async Task MockStartMessageReceiver_ShouldRetrieveMessageWithRetry_WhenQueueAndMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationToken = client.StartMessageReceiver(FifoQueueName, 1, 1, 10, 1, 10, message =>
            {
                Assert.Equal("Hello World!", message);
                _messagePicked = true;
                return true;
            });


            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationToken.Cancel();
            Assert.True(_messagePicked);
            _messagePicked = false;
        }

        [Fact]
        public async Task MockStartMessageReceiver_ShouldRetrieveMessageWithAsyncMessageProcessor_WhenQueueAndMessageExists()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);

            var cancellationToken = client.StartMessageReceiver(FifoQueueName, 1, 1, 10, 1, 10, async message =>
            {
                Assert.Equal("Hello World!", message);
                _messagePicked = true;
                return await Task.FromResult(true);
            });

            await client.SendMessageAsync("Hello World!", FifoQueueName);

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationToken.Cancel();
            Assert.True(_messagePicked);
            _messagePicked = false;
        }

        [Fact]
        public void MockStartMessageReceiver_ShouldThrowQueueDoesNotExistException_WhenQueueDoesNotExist()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");

            Assert.Throws<QueueDoesNotExistException>(() =>
                client.StartMessageReceiver(FifoQueueName, 1, 1, 3, 1, 1, message => true));
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
        public async Task MockQueue_ShouldNotPickMessageFromQueue_UntilAcked()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationToken = client.StartMessageReceiver(FifoQueueName, 1, 1, async (message) =>
            {
                Assert.Single(client.GetMessages(FifoQueueName));
                Assert.True(client.GetMessages(FifoQueueName).First().IsLocked);
                Assert.Equal("Hello World!", message);
                _messagePicked = true;
                return await Task.FromResult(true);
            });

            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            cancellationToken.Cancel();
            Assert.True(_messagePicked);
            Assert.Empty(client.GetMessages(FifoQueueName));
            _messagePicked = false;
        }

        [Fact]
        public async Task MockQueue_ShouldNotPickMessageFromQueue_IfFalseIsReturned()
        {
            var client = new SQSClientMock("mockEndpoint", "mockRegion");
            await client.CreateStandardFifoQueueAsync(FifoQueueName);
            await client.SendMessageAsync("Hello World!", FifoQueueName);

            var cancellationToken = client.StartMessageReceiver(FifoQueueName, 1, 1, async (message) =>
            {
                Assert.Single(client.GetMessages(FifoQueueName));
                Assert.True(client.GetMessages(FifoQueueName).First().IsLocked);
                Assert.Equal("Hello World!", message);
                _messagePicked = true;
                return await Task.FromResult(false);
            });

            await client.AwaitMessageProcessedAttempt(FifoQueueName);

            cancellationToken.Cancel();
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

            client.StartMessageReceiver(FifoQueueName, 1, 1,
                async (message) =>
                {
                    await Task.Delay(1000);
                    return true;
                });

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

            client.StartMessageReceiver(FifoQueueName, 1, 1,
                async (message) =>
                {
                    await Task.Delay(1000);
                    return false;
                });

            await client.SendMessageAsync("Hello World!", FifoQueueName);
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            await client.AwaitMessageProcessedAttempt(FifoQueueName);
            stopwatch.Stop();

            Assert.True(stopwatch.ElapsedMilliseconds > 900);
        }
    }
}
