using System.Threading.Tasks;

namespace NetSQS.Mock
{
    public class SQSMessageMock : ISQSMessage
    {
        public SQSMessageMock(SQSClientMock mockClient, string queueName, string receiptHandle)
        {
            Client = mockClient;
            QueueName = queueName;
            ReceiptHandle = receiptHandle;
        }

        private SQSClientMock Client { get; set; }
        public string Body { get; set; }
        private string QueueName { get; set; }
        private string ReceiptHandle { get; set; }

        public async Task Ack()
        {
            await Client.DeleteMessageAsync(QueueName, ReceiptHandle);
        }
    }
}