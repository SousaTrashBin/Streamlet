package utils.application;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public record Block(byte[] parentHash, Integer epoch, Integer length, Transaction[] transactions) implements Content {
    private static final Pattern BLOCK_REGEX = Pattern.compile(
            "Block\\[(?<epoch>\\d+),(?<length>\\d+),(?<parentHash>.*),\\[(?<transactions>.*)]]"
    );

    public Block(byte[] parentHash, Integer epoch, Integer length, Transaction[] transactions) {
        this.parentHash = parentHash;
        this.epoch = epoch;
        this.length = length;
        this.transactions = transactions.clone();
    }

    public byte[] getSHA1() {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");

            sha1.update(parentHash);

            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(epoch);
            buffer.putInt(length);
            sha1.update(buffer.array());

            for (Transaction transaction : transactions) {
                ByteBuffer transactionBuffer = ByteBuffer.allocate(24);
                transactionBuffer.putLong(transaction.id());
                transactionBuffer.putDouble(transaction.amount());
                transactionBuffer.putInt(transaction.sender());
                transactionBuffer.putInt(transaction.receiver());
                sha1.update(transactionBuffer.array());
            }

            return sha1.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Block block)) return false;

        return epoch.equals(block.epoch) && length.equals(block.length) && Arrays.equals(parentHash, block.parentHash);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(parentHash);
        result = 31 * result + epoch.hashCode();
        result = 31 * result + length.hashCode();
        return result;
    }

    public String toStringSummary() {
        String partialParentHash = IntStream.range(0, Math.min(4, parentHash.length))
                .mapToObj(i -> String.format("%02X", parentHash[i]))
                .collect(Collectors.joining(" "));

        String txSummary = Arrays.stream(transactions)
                .map(Transaction::toStringSummary)
                .collect(Collectors.joining("; "));

        return String.format(
                "Epoch: %d | Length: %d | Parent: %s | Tx: [%s]",
                epoch, length, partialParentHash, txSummary
        );
    }

    public static Block fromPersistanceString(String persistanceString) {
        Matcher matcher = BLOCK_REGEX.matcher(persistanceString);
        if (!matcher.matches()) {
            return null;
        }

        Integer epoch = Integer.parseInt(matcher.group("epoch"));
        Integer length = Integer.parseInt(matcher.group("length"));
        byte[] parentHash = Base64.getDecoder().decode(matcher.group("parentHash"));

        String transactionsString = matcher.group("transactions");
        Transaction[] transactions = transactionsString.isEmpty() ? new Transaction[0] : Arrays.stream(transactionsString.substring(1, transactionsString.length() - 1).split(","))
                .map(Transaction::fromPersistanceString)
                .toArray(Transaction[]::new);

        return new Block(parentHash, epoch, length, transactions);
    }

    public String getPersistanceString() {
        return "Block[%s,%s,%s,[%s]]".formatted(
                epoch,
                length,
                Base64.getEncoder().encodeToString(parentHash),
                Arrays.stream(transactions).map(Transaction::getPersistanceString).collect(Collectors.joining(","))
        );
    }
}
