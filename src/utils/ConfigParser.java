package utils;

import utils.communication.Address;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;
import utils.logs.LogLevel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigParser {
    public final static String CONFIG_FILE = "config.txt";
    public final static DateTimeFormatter START_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    public final static LocalDateTime DEFAULT_START_DATE =
            LocalDateTime.parse("01-01-2000 00:00:00", START_FORMAT);

    private static final Pattern P2P_PATTERN = Pattern.compile("^P2P\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern START_PATTERN = Pattern.compile("^start\\s*=\\s*(\\d{2}-\\d{2}-\\d{4} \\d{2}:\\d{2}:\\d{2})$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SERVER_PATTERN = Pattern.compile("^server\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern LOGLEVEL_PATTERN = Pattern.compile("^logLevel\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRANSACTION_MODE_PATTERN = Pattern.compile("^transactionsMode\\s*=\\s*(.+)$", Pattern.CASE_INSENSITIVE);

    public static ConfigData parseConfig() throws IOException {
        ConfigData configData = new ConfigData();
        List<String> lines = Files.readAllLines(Paths.get(CONFIG_FILE));
        int peerIndex = 0;
        int serverIndex = 0;

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue; // skip empty lines or comments

            Matcher p2pMatcher = P2P_PATTERN.matcher(line);
            Matcher startMatcher = START_PATTERN.matcher(line);
            Matcher serverMatcher = SERVER_PATTERN.matcher(line);
            Matcher logLevelMatcher = LOGLEVEL_PATTERN.matcher(line);
            Matcher transactionMatcher = TRANSACTION_MODE_PATTERN.matcher(line);

            if (p2pMatcher.matches()) {
                configData.peers.add(new PeerInfo(peerIndex++, Address.fromString(p2pMatcher.group(1).trim())));
            } else if (startMatcher.matches()) {
                configData.start = parseToDate(startMatcher.group(1).trim());
            } else if (serverMatcher.matches()) {
                configData.servers.put(serverIndex++, Address.fromString(serverMatcher.group(1).trim()));
            } else if (logLevelMatcher.matches()) {
                try {
                    configData.logLevel = LogLevel.valueOf(logLevelMatcher.group(1).trim().toUpperCase());
                } catch (IllegalArgumentException ignored) {
                    configData.logLevel = LogLevel.NORMAL;
                }
            } else if (transactionMatcher.matches()) {
                configData.isClientGeneratingTransactions = transactionMatcher.group(1).trim().equalsIgnoreCase("CLIENT");
            }
        }

        return configData;
    }

    private static LocalDateTime parseToDate(String dateStr) {
        try {
            return LocalDateTime.parse(dateStr, START_FORMAT);
        } catch (DateTimeParseException e) {
            AppLogger.logWarning(
                    "Invalid format for protocol start date. Default start date was used. Should be: dd-MM-yyyy HH:mm:ss."
            );
            return DEFAULT_START_DATE;
        }
    }

    public static class ConfigData {
        public List<PeerInfo> peers = new ArrayList<>();
        public LocalDateTime start = DEFAULT_START_DATE;
        public Map<Integer, Address> servers = new HashMap<>();
        public LogLevel logLevel = LogLevel.NORMAL;
        public boolean isClientGeneratingTransactions = false;
    }
}
