Since scheduled messages in Azure Service Bus cannot be canceled directly from the Azure Portal UI, you need to call the Service Bus client API using the message’s SequenceNumber.

This Utility helps us to surgically remove a scheduled message without purging the whole queue.