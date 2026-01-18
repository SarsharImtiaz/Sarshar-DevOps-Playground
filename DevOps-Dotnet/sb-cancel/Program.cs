using Azure.Messaging.ServiceBus;

var conn = "<Your_SB_Connnection_String>";
var queue = "examplequeuename";
long seqToCancel = <Your_Seq_Number>;

await using var client = new ServiceBusClient(conn);
ServiceBusSender sender = client.CreateSender(queue);

await sender.CancelScheduledMessageAsync(seqToCancel);

Console.WriteLine($"Cancelled scheduled message seq={seqToCancel}");
