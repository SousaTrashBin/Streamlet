package app;

import utils.application.Transaction;

import java.util.Random;

public class TransactionPoolSimulator {

    private final int numberOfNodes;
    private final Random random = new Random(1L);
    private Long transactionId = 1L;

    public TransactionPoolSimulator(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public Transaction generateNewTransaction() {
        int sender = random.nextInt(numberOfNodes);
        int receiver;
        do {
            receiver = random.nextInt(numberOfNodes);
        } while (receiver == sender);

        double amount = sender + receiver + random.nextDouble(5);

        return new Transaction(this.transactionId++, amount, sender, receiver);
    }

    public Transaction[] generateTransactions() {
        int count = random.nextInt(2, 6);
        Transaction[] transactions = new Transaction[count];
        for (int i = 0; i < count; i++) transactions[i] = generateNewTransaction();
        return transactions;
    }
}