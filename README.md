# NetSQS.Mock

A mocking library to use together with NetSQS. Is used to simplify testing.

## Usage

The mock implements `ISQSClient` so that you easily can pass it to wherever it is used.

## About the implementation

The implementation uses a dictionary defined like `Dictionary<string, Queue<string>>` where the key is the queue name.
It will, just like the AWS implementation, poll this queue for new messages, meaning that it will create a Task that runs in a parallel thread as long as you don't use the cancellation token to cancel the operation.
