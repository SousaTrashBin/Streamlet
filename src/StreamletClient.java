import utils.ConfigParser;
import utils.application.Transaction;
import utils.communication.Address;
import utils.logs.AppLogger;

private final Random random = new Random(1L);
private boolean running = true;
private Socket socket;
private BufferedReader userInput;
private ObjectOutputStream out;
private Map<Integer, Address> serverAddressees;

void main() throws IOException {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
            System.in.close();
        } catch (IOException ignored) {
        }
        closeResources();
    }));

    ConfigParser.ConfigData configData = ConfigParser.parseConfig();
    AppLogger.updateLoggerLevel(configData.logLevel());

    userInput = new BufferedReader(new InputStreamReader(System.in));
    serverAddressees = ConfigParser.parseConfig().servers();
    printInfoGui();

    while (running) {
        if (socket == null || socket.isClosed()) {
            try {
                connectToRandomStreamlet();
            } catch (IOException e) {
                AppLogger.logWarning("Failed to connect to a server.");
                AppLogger.logWarning("Check if Streamlet nodes are running");
                AppLogger.logWarning("Retrying in 2s...");
                AppLogger.logDebug(String.valueOf(e));
                sleepMillis(2000);
                continue;
            }
        }

        printClientGui();
        Transaction clientTransaction = handleClientInput(userInput);
        if (clientTransaction != null) {
            sendTransaction(clientTransaction);
        }
    }

    closeResources();
}

private void connectToRandomStreamlet() throws IOException {
    int randomInt = random.nextInt(serverAddressees.size());
    Address address = serverAddressees.get(randomInt);

    socket = new Socket(address.ip(), address.port());
    out = new ObjectOutputStream(socket.getOutputStream());
    out.flush();
    AppLogger.logInfo("Connected to server " + address.ip() + ":" + address.port());
}

private Transaction handleClientInput(BufferedReader userInput) {
    try {
        String input = userInput.readLine();
        if (input == null || input.equalsIgnoreCase("quit")) {
            running = false;
            AppLogger.logInfo("Closing client...");
            return null;
        }

        String[] parts = input.trim().split(" ");
        if (parts.length != 3) {
            AppLogger.logWarning("Invalid input format. Expected: <amount> <sender> <receiver>");
            return null;
        }

        double amount = Double.parseDouble(parts[0]);
        int sender = Integer.parseInt(parts[1]);
        int receiver = Integer.parseInt(parts[2]);
        long id = UUID.randomUUID().getLeastSignificantBits();

        return new Transaction(id, amount, sender, receiver);
    } catch (IOException e) {
        AppLogger.logError("Error reading input from user.", e);
    } catch (NumberFormatException e) {
        AppLogger.logError("Failed to parse transaction numbers. Please enter valid numbers.", e);
    }
    return null;
}

private void sendTransaction(Transaction transaction) {
    if (socket == null || socket.isClosed() || out == null) {
        AppLogger.logWarning("No connection to server. Transaction not sent.");
        return;
    }

    try {
        out.writeObject(transaction);
        out.flush();
        AppLogger.logInfo("Transaction sent: " + transaction);
    } catch (IOException e) {
        AppLogger.logError("Failed to send transaction. Will attempt reconnect.", e);
        closeSocketAndStream();
    }
}

private void printClientGui() {
    AppLogger.logInfo("Enter a transaction: ");
}

private void printInfoGui() {
    AppLogger.logInfo("Type transaction as <amount> <sender> <receiver>");
    AppLogger.logInfo("To exit type: quit");
    AppLogger.logInfo("Example: 23.45 2 3");
}

private void closeResources() {
    closeSocketAndStream();
    try {
        if (userInput != null) {
            userInput.close();
            AppLogger.logDebug("User input stream closed.");
        }
    } catch (IOException e) {
        AppLogger.logError("Failed to close user input stream.", e);
    }
}

private void closeSocketAndStream() {
    try {
        if (socket != null && !socket.isClosed()) {
            socket.close();
            AppLogger.logDebug("Socket closed.");
        }
    } catch (IOException e) {
        AppLogger.logError("Failed to close socket.", e);
    }

    try {
        if (out != null) {
            out.close();
            AppLogger.logDebug("Output stream closed.");
        }
    } catch (IOException e) {
        AppLogger.logError("Failed to close output stream.", e);
    }

    socket = null;
    out = null;
}

private void sleepMillis(long ms) {
    try {
        Thread.sleep(ms);
    } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
    }
}
