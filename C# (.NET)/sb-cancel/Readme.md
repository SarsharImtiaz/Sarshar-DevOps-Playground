This is a small Dotnet utility which helps us to delete a scheduled message without purging the whole queue.

Since scheduled messages in Azure Service Bus cannot be canceled directly from the Azure Portal UI, you need to call the Service Bus client API using the message’s SequenceNumber
