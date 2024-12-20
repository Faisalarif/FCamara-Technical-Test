namespace BankingApi.EventReceiver;

public class TransactionNotification
{
    public string MessageType { get; set; } = string.Empty;
    public Guid BankAccountId { get; set; }
    public decimal Amount { get; set; }
}