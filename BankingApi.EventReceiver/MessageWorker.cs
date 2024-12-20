using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace BankingApi.EventReceiver
{
    public class MessageWorker
    {
        private readonly IServiceBusReceiver _serviceBusReceiver;
        private readonly BankDbContext _dbContext;
        private readonly ILogger<MessageWorker> _logger;

        public MessageWorker(IServiceBusReceiver serviceBusReceiver, BankDbContext dbContext, ILogger<MessageWorker> logger)
        {
             _serviceBusReceiver = serviceBusReceiver ?? throw new ArgumentNullException(nameof(serviceBusReceiver));
             _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
             _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        // public Task Start()
        // {
        //     // Implement logic to listen to messages here
        //     return Task.CompletedTask;
        // }

        public async Task StartAsync()
        {
            _logger.LogInformation("MessageWorker started.");

            while (true)
            {
                var message = await _serviceBusReceiver.Peek();

                if (message == null)
                {
                    _logger.LogDebug("No messages available. Waiting...");
                    await Task.Delay(TimeSpan.FromSeconds(10));
                    continue;
                }

                try
                {
                    await ProcessMessageAsync(message);
                    await _serviceBusReceiver.Complete(message);
                    _logger.LogInformation($"Message {message.Id} processed successfully.");
                }
                catch (TransientException ex)
                {
                    _logger.LogWarning(ex, $"Transient error processing message {message.Id}. Retrying...");
                    await ApplyRetryPolicyAsync(message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Non-transient error for message {message.Id}. Moving to DeadLetter.");
                    await _serviceBusReceiver.MoveToDeadLetter(message);
                }
            }
        }

        private async Task ProcessMessageAsync(EventMessage message)
        {
            // Check if the message has already been processed
            if (await _dbContext.ProcessedMessages.AnyAsync(pm => pm.MessageId == message.Id))
            {
                _logger.LogWarning($"Duplicate message detected: {message.Id}. Ignoring.");
                return;
            }

            // Deserialize message
            var notification = JsonSerializer.Deserialize<TransactionNotification>(message.MessageBody);
            if (notification == null || (notification.MessageType != "Credit" && notification.MessageType != "Debit"))
            {
                throw new NonTransientException($"Invalid message type: {notification?.MessageType}");
            }

            // Process transaction
            using var transaction = await _dbContext.Database.BeginTransactionAsync();
            var account = await _dbContext.BankAccounts.FindAsync(notification.BankAccountId);
            if (account == null)
            {
                throw new NonTransientException($"Account not found: {notification.BankAccountId}");
            }

            account.Balance += notification.MessageType == "Credit" ? notification.Amount : -notification.Amount;

            if (account.Balance < 0)
            {
                throw new NonTransientException("Insufficient balance for Debit transaction.");
            }

            // Save changes and record message as processed
            await _dbContext.SaveChangesAsync();
            _dbContext.ProcessedMessages.Add(new ProcessedMessage
            {
                MessageId = message.Id,
                ProcessedAt = DateTime.UtcNow
            });
            await _dbContext.SaveChangesAsync();

            await transaction.CommitAsync();
            _logger.LogInformation($"Transaction processed for Account {notification.BankAccountId}. New Balance: {account.Balance}");
        }


        private async Task ApplyRetryPolicyAsync(EventMessage message)
        {
            var retryDelays = new[] { 5, 25, 125 }; // Seconds
            foreach (var delay in retryDelays)
            {
                try
                {
                    _logger.LogInformation($"Retrying message {message.Id} after {delay} seconds...");
                    await Task.Delay(TimeSpan.FromSeconds(delay));
                    await ProcessMessageAsync(message);
                    await _serviceBusReceiver.Complete(message);
                    return;
                }
                catch (TransientException ex)
                {
                    _logger.LogWarning(ex, $"Retry failed for message {message.Id}. Will retry again.");
                }
            }

            // If retries fail, abandon the message
            _logger.LogWarning($"Max retries exceeded for message {message.Id}. Abandoning message.");
            await _serviceBusReceiver.Abandon(message);
        }
    }



    // Custom Exceptions
    public class TransientException : Exception
    {
        public TransientException(string message) : base(message) { }
    }

    public class NonTransientException : Exception
    {
        public NonTransientException(string message) : base(message) { }
    }

    // DbContext
    public class BankDbContext : DbContext
    {
        public BankDbContext(DbContextOptions<BankDbContext> options) : base(options) { }

        public DbSet<BankAccount> BankAccounts { get; set; }
        public DbSet<ProcessedMessage> ProcessedMessages { get; set; }
    }
}
