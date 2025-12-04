package app;

import utils.application.Block;
import utils.application.Hash;
import utils.logs.AppLogger;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PersistenceFilesManager {

    private final Path logFilePath;
    private final Path blockchainFilePath;

    public PersistenceFilesManager(Path logFilePath, Path blockchainFilePath, Path outputPath) {
        this.logFilePath = logFilePath;
        this.blockchainFilePath = blockchainFilePath;

        try {
            createIfNotExistsOutputFiles(outputPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createIfNotExistsOutputFiles(Path outputPath) throws IOException {
        Files.createDirectories(outputPath);
        try {
            Files.createFile(logFilePath);
        } catch (FileAlreadyExistsException ignored) {
        }
        try {
            Files.createFile(blockchainFilePath);
        } catch (FileAlreadyExistsException ignored) {
        }
    }

    public int initializeFromFile(
            Map<Hash, BlockNode> blockNodesByHash,
            Map<Hash, List<Hash>> blockchainByParentHash,
            Set<BlockNode> recoveredBlocks,
            Set<Block> pendingProposals
    ) {

        String content = "";

        try {
            content = Files.readString(blockchainFilePath);
        } catch (IOException e) {
        }
        if (content.isBlank()) return -1;

        String[] sections = content.split("\n\n");

        int mostRecentEpoch = -1;

        if (sections.length > 0) {
            String[] blockNodeLines = sections[0].trim().split("\n");
            mostRecentEpoch = initializeBlockNodesByHash(blockNodesByHash, blockNodeLines);
        }

        if (sections.length > 1) {
            String[] chainLines = sections[1].trim().split("\n");
            initializeBlockChainByParentHash(blockchainByParentHash, chainLines);
        }

        if (sections.length > 2) {
            String[] recoveredLines = sections[2].trim().split("\n");
            initializeRecoveredBlocks(recoveredBlocks, recoveredLines);
        }

        if (sections.length > 3) {
            String[] pendingLines = sections[3].trim().split("\n");
            initializePendingProposals(pendingProposals, pendingLines);
        }
        return mostRecentEpoch;
    }

    private int initializeBlockNodesByHash(Map<Hash, BlockNode> blockNodesByHash, String[] blockNodeLines) {
        int maxEpoch = -1;

        for (String line : blockNodeLines) {
            if (line.isBlank()) continue;

            int colonIndex = line.indexOf(":");
            if (colonIndex == -1) continue;

            String hashStr = line.substring(0, colonIndex).trim();
            String blockNodeStr = line.substring(colonIndex + 1).trim();

            AppLogger.logWarning("[PERSISTENCE] BLOCKNODE OF HASH " + hashStr);

            try {
                Hash hash = Hash.fromPersistenceString(hashStr);
                BlockNode blockNode = BlockNode.fromPersistenceString(blockNodeStr);

                if (blockNode != null) {
                    blockNodesByHash.put(hash, blockNode);

                    if (blockNode.block().epoch() > maxEpoch) {
                        maxEpoch = blockNode.block().epoch();
                    }
                }
            } catch (IllegalArgumentException e) {
                AppLogger.logWarning("Failed to parse blockNode entry: " + line);
            }
        }
        return maxEpoch;
    }

    private void initializeBlockChainByParentHash(Map<Hash, List<Hash>> blockchainByParentHash, String[] chainLines) {
        for (String line : chainLines) {
            if (line.isBlank()) continue;

            int colonIndex = line.indexOf(":");
            if (colonIndex == -1) continue;

            int startBracketIndex = line.indexOf("[");
            int lastBracketIndex = line.lastIndexOf("]");
            if (startBracketIndex == -1 || lastBracketIndex == -1) continue;

            String hashStr = line.substring(0, colonIndex).trim();
            String childrenStr = line.substring(startBracketIndex + 1, lastBracketIndex);

            try {
                Hash parentHash = Hash.fromPersistenceString(hashStr);

                List<Hash> childrenHashes = new LinkedList<>();

                if (!childrenStr.isBlank()) {
                    String[] childHashStrings = childrenStr.split(",");

                    for (String childHashStr : childHashStrings) {
                        if (childHashStr.isBlank()) continue;

                        try {
                            Hash childHash = Hash.fromPersistenceString(childHashStr.trim());
                            childrenHashes.add(childHash);
                            AppLogger.logWarning("[PERSISTENCE] FOUND CHILD HASH FOR PARENT " + hashStr);
                        } catch (Exception e) {
                            AppLogger.logWarning("Could not parse child hash: " + childHashStr);
                        }
                    }
                }

                blockchainByParentHash.put(parentHash, childrenHashes);
            } catch (IllegalArgumentException e) {
                AppLogger.logWarning("Failed to parse chain entry: " + line);
            }
        }
    }

    private void initializeRecoveredBlocks(Set<BlockNode> recoveredBlocks, String[] recoveredLines) {
        for (String line : recoveredLines) {
            if (line.isBlank()) continue;

            try {
                BlockNode blockNode = BlockNode.fromPersistenceString(line.trim());

                if (blockNode != null) {
                    recoveredBlocks.add(blockNode);
                }
            } catch (Exception e) {
                AppLogger.logWarning("Failed to parse recovered block: " + line);
            }
        }
    }

    private void initializePendingProposals(Set<Block> pendingProposals, String[] pendingLines) {
        for (String line : pendingLines) {
            if (line.isBlank()) continue;

            try {
                Block block = Block.fromPersistenceString(line.trim());

                if (block != null) {
                    pendingProposals.add(block);
                }
            } catch (Exception e) {
                AppLogger.logWarning("Failed to parse pending proposal: " + line);
            }
        }
    }

    public void persistToFile(String persistenceString) {
        try {
            Files.writeString(blockchainFilePath, persistenceString, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
            Files.writeString(logFilePath, "", StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (IOException ignored) {}
    }
}