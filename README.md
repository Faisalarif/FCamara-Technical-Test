
Technical Task:

You are building an event receiver that receives banking transactions from Messages.

# Intructions
Carefully Read the description, fork the repository and implement the challenge below:
- We are looking for **production quality**, so code it as if it was your daily job.
- Feel free to add your thoughts or things you were considering on THOUGHTS.md
- Your entry point is on: MessageWorker.cs
- This project uses EF Core migrations and SQL Express if you need to set up the database

The message receiver is using Azure Service Bus using the Peek Lock method for messages, reading messages **at-least once**.

Write a production quality event receiver, that receives Credit/Debit notifications and updates the total balance in the BankAccounts table.
You can add new tables to the database if you need to.
The important part is **production quality**, from Quality levels, resiliency and data integrity.

The MessageProcessor runs into multiple containers at the same time.

# Requirements:
- If IEventReceiver.Peek returns null, it means there are no messages in the queue, so await 10 seconds
- Messages abandoned more than 3 times automatically go to the Deadletter (You don't need to code this)
- Credit messages: Add amount to the existing balance
- Debit messages: Deduct amount from the existing balance
- Transient failures needs to be exponentially retried by 5, 25 and 125 seconds.
- Non-transient failures should be moved to deadletter on the spot
- - ex: MessageTypes other than Credit/Debit are moved to DeadLetter at the first processing.

EventMessage:
Credit Example
```
{
  "id": "89479d8a-549b-41ea-9ccc-25a4106070a1",
  "messageType": "Credit",
  "bankAccountId": "7d445724-24ec-4d52-aa7a-ff2bac9f191d",
  "amount": 90.00
}
```

Debit Example:
```
{
  "id": "89479d8a-549b-41ea-9ccc-25a4106070a1",
  "messageType": "Debit",
  "bankAccountId": "3bbaf4ca-5bfa-4922-a395-d755beac475f"
  "amount": 90.00
}
```
