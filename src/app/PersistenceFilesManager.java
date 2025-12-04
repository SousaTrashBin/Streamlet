package app;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import utils.application.Block;
import utils.application.Hash;
import utils.logs.AppLogger;

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
        Map<Hash, BlockNode> blockNodesByHash, Map<Hash, List<BlockNode>> blockchainByParentHash,
        Set<BlockNode> recoveredBlocks, Set<Block> pendingProposals
    ) {

        String content = "";
        
        try {
            content = Files.readString(blockchainFilePath);
        } catch (IOException e) {}
        if (content.isBlank()) return -1;

        String[] sections = content.split("\n\n");

        String[] blockNodeLines = sections[0].trim().split("\n");
        initializeBlockNodesByHash(blockNodesByHash, blockNodeLines);

        String[] chainLines = sections[1].trim().split("\n");
        int mostRecentEpoch = initializeBlockChainByParentHash(blockchainByParentHash, chainLines);

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

    private void initializeBlockNodesByHash(Map<Hash, BlockNode> blockNodesByHash, String[] blockNodeLines) {
        for (String line : blockNodeLines) {
            if (line.isBlank()) continue;

            int colonIndex = line.indexOf(":");
            if (colonIndex == -1) continue;

            String hashStr = line.substring(0, colonIndex).trim();
            String blockNodeStr = line.substring(colonIndex + 1).trim();

            AppLogger.logWarning("[PERSISTENCE] BLOCKNODE OF HASH " + hashStr);
            AppLogger.logWarning("[PERSISTENCE] " + blockNodeStr);

            try {
                Hash hash = Hash.fromPersistenceString(hashStr);
                BlockNode blockNode = BlockNode.fromPersistenceString(blockNodeStr);

                if (blockNode != null) {
                    blockNodesByHash.put(hash, blockNode);
                }
            } catch (IllegalArgumentException e) {
                AppLogger.logWarning("Failed to parse blockNode entry: " + line);
            }
        }
    }

    private int initializeBlockChainByParentHash(Map<Hash, List<BlockNode>> blockchainByParentHash, String[] chainLines) {
        int mostRecentEpoch = -1;

        for (String line : chainLines) {
            if (line.isBlank()) continue;

            int colonIndex = line.indexOf(":");
            if (colonIndex == -1) continue;

            int startBracketIndex = line.indexOf("[");
            if (startBracketIndex == -1) continue;

            int lastBracketIndex = line.lastIndexOf("]");
            if (lastBracketIndex == -1) continue;

            String hashStr = line.substring(0, colonIndex).trim();
            String childrenStr = line.substring(startBracketIndex + 1, lastBracketIndex);

            try {
                Hash hash = Hash.fromPersistenceString(hashStr);

                List<BlockNode> children = new LinkedList<>();

                if (!childrenStr.isBlank()) {
                    String[] blockNodeStrings = childrenStr.split(",(?=BlockNode\\[)");
                    for (String blockNodeStr : blockNodeStrings) {
                        BlockNode blockNode = BlockNode.fromPersistenceString(blockNodeStr.trim());
                        AppLogger.logWarning("[PERSISTENCE] FOUND A BLOCK CHILD OF " + Base64.getEncoder().encodeToString(hash.hash()));
                        if (blockNode != null) {
                            AppLogger.logWarning("[PERSISTENCE] AND IT IS NOT NULL!");
                            children.add(blockNode);
                            int blockEpoch = blockNode.block().epoch();
                            if (blockEpoch > mostRecentEpoch) {
                                mostRecentEpoch = blockEpoch;
                            }
                        }
                    }
                }

                blockchainByParentHash.put(hash, children);
            } catch (IllegalArgumentException e) {
                AppLogger.logWarning("Failed to parse chain entry: " + line);
            }
        }
        return mostRecentEpoch;
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
