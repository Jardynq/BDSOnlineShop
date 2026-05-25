using Client.Transaction;

// Remember to start the Orleans server before running the client program
var transactionClient = new TransactionClient();
await transactionClient.RunClient();
return;
