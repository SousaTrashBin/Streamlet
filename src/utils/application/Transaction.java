package utils.application;

import java.io.Serializable;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record Transaction(Long id, Double amount, Integer sender, Integer receiver) implements Serializable {
    private static final Pattern TX_REGEX = Pattern.compile("Tx\\<(?<id>\\d+),(?<amount>\\d+(.\\d+)?),(?<sender>\\d+),(?<receiver>\\d+)>");

    public String toStringSummary() {
        return String.format("id=%d, %d→%d: %.2f", id, sender, receiver, amount);
    }

    public String getPersistenceString() {
        return String.format(Locale.US, "Tx<%d,%.2f,%d,%d>", id, amount, sender, receiver);
    }

    public static Transaction fromPersistenceString(String persistenceString) {
        Matcher matcher = TX_REGEX.matcher(persistenceString);
        if (!matcher.matches()) {
            return null;
        }
        long id = Long.parseLong(matcher.group("id"));
        double amount = Double.parseDouble(matcher.group("amount"));
        int sender = Integer.parseInt(matcher.group("sender"));
        int receiver = Integer.parseInt(matcher.group("receiver"));
        return new Transaction(id, amount, sender, receiver);
    }
}
