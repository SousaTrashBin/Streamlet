package utils.application;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record Transaction(Long id, Double amount, Integer sender, Integer receiver) implements Serializable {
    private static final Pattern TX_REGEX = Pattern.compile("Tx\\[(?<id>\\d+),(?<amount>\\d+(.\\d+)?),(?<sender>\\d+),(?<receiver>\\d+)\\]");

    public String toStringSummary() {
        return String.format("id=%d, %d→%d: %.2f", id, sender, receiver, amount);
    }

    public static Transaction fromPersistanceString(String persistanceString) {
        Matcher matcher = TX_REGEX.matcher(persistanceString);
        if (!matcher.matches()) {
            return null;
        }
        Long id = Long.parseLong(matcher.group("id"));
        Double amount = Double.parseDouble(matcher.group("amount"));
        Integer sender = Integer.parseInt(matcher.group("sender"));
        Integer receiver = Integer.parseInt(matcher.group("receiver"));
        return new Transaction(id, amount, sender, receiver);
    }

    public String getPersistanceString() {
        return "Tx[%d,%f,%d,%d]".formatted(id, amount, sender, receiver);
    }
}
