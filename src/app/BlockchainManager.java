package app;

import utils.application.Block;
import utils.application.Hash;
import utils.application.Transaction;
import utils.logs.AppLogger;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BlockchainManager {
    public static final int FINALIZATION_MIN_SIZE = 3;
    public static final String LOG_FILE_NAME = "log.txt";
    public static final String BLOCK_CHAIN_FILE_NAME = "blockChain.txt";
    private static final int SHA1_LENGTH = 20;
    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);
    private final Hash genesisParentHash;

    //{1:2}
    //-----
    //{1:[],}
    //-----
    //{}
    //-----
    //{2}

    private final Map<Hash, BlockNode> blockNodesByHash = new HashMap<>(); // blocos
    private final Map<Hash, List<BlockNode>> blockchainByParentHash = new HashMap<>();
    private final Set<BlockNode> recoveredBlocks = new HashSet<>();
    private final Set<Block> pendingProposals = new HashSet<>();

    private final Path logFilePath;
    private final Path blockchainFilePath;

    private int mostRecentNotarizedEpoch = -1;

    public BlockchainManager(Path outputPath) {
        this.logFilePath = outputPath.resolve(LOG_FILE_NAME);
        this.blockchainFilePath = outputPath.resolve(BLOCK_CHAIN_FILE_NAME);
        try {
            createIfNotExistsOutputFiles(outputPath);
            initializeFromFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BlockNode genesisNode = new BlockNode(GENESIS_BLOCK, true);
        genesisParentHash = new Hash(GENESIS_BLOCK.parentHash());
        Hash genesisHash = new Hash(GENESIS_BLOCK.getSHA1());

        List<BlockNode> genesisRoot = new LinkedList<>();
        genesisRoot.add(genesisNode);
        blockchainByParentHash.putIfAbsent(genesisParentHash, genesisRoot);
        blockchainByParentHash.putIfAbsent(genesisHash, new LinkedList<>());

        blockNodesByHash.putIfAbsent(genesisHash, genesisNode);
    }


    private void initializeFromFile() throws IOException {
        String content = Files.readString(blockchainFilePath);
        if (content.isBlank()) {
            return;
        }

        String[] sections = content.split("\n\n");

        String[] blockNodeLines = sections[0].trim().split("\n");
        for (String line : blockNodeLines) {
            if (line.isBlank()) continue;
            int colonIndex = line.indexOf(":");
            if (colonIndex == -1) continue;

            String hashStr = line.substring(0, colonIndex).trim();
            String blockNodeStr = line.substring(colonIndex + 1).trim();

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

        String[] chainLines = sections[1].trim().split("\n");
        for (String line : chainLines) {
            if (line.isBlank()) continue;

            int lastBracketIndex = line.lastIndexOf("]");
            if (lastBracketIndex == -1) continue;

            int startBracketIndex = line.lastIndexOf("[");
            if (startBracketIndex == -1) continue;

            String hashStr = line.substring(0, startBracketIndex).trim();
            if (hashStr.endsWith(",")) {
                hashStr = hashStr.substring(0, hashStr.length() - 1).trim();
            }
            String childrenStr = line.substring(startBracketIndex, lastBracketIndex + 1);

            try {
                Hash hash = Hash.fromPersistenceString(hashStr);

                List<BlockNode> children = new LinkedList<>();
                if (childrenStr.startsWith("[") && childrenStr.endsWith("]")) {
                    String innerContent = childrenStr.substring(1, childrenStr.length() - 1);
                    if (!innerContent.isBlank()) {
                        String[] blockNodeStrings = innerContent.split(",(?=BlockNode\\[)");
                        for (String blockNodeStr : blockNodeStrings) {
                            BlockNode blockNode = BlockNode.fromPersistenceString(blockNodeStr.trim());
                            if (blockNode != null) {
                                children.add(blockNode);
                            }
                        }
                    }
                }
                blockchainByParentHash.put(hash, children);
            } catch (IllegalArgumentException e) {
                AppLogger.logWarning("Failed to parse chain entry: " + line);
            }
        }

        String[] recoveredLines = sections[2].trim().split("\n");
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

        String[] pendingLines = sections[3].trim().split("\n");
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

    public void persistToFile() {
        try {
            Files.writeString(blockchainFilePath, getPersistenceString(), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
            Files.writeString(logFilePath, "", StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (Exception ignored) {
        }
    }

    private String getPersistenceString() {
        StringBuilder sb = new StringBuilder();
        blockNodesByHash.forEach((hash, blockNode) -> {
            sb.append("%s:%s".formatted(hash.getPersistenceString(), blockNode.getPersistenceString()));
            sb.append("\n");
        });
        sb.append("\n\n");
        blockchainByParentHash.forEach((hash, children) -> {
            sb.append("%s,[%s]".formatted(
                            hash.getPersistenceString(),
                            children.stream().map(BlockNode::getPersistenceString).collect(Collectors.joining(","))
                    )
            );
            sb.append("\n");
        });
        sb.append("\n\n");
        sb.append("%s".formatted(recoveredBlocks.stream().map(BlockNode::getPersistenceString).collect(Collectors.joining("\n"))));
        sb.append("\n\n");
        sb.append("%s".formatted(pendingProposals.stream().map(Block::getPersistenceString).collect(Collectors.joining("\n"))));
        return sb.toString();
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

    public List<Block> getBiggestNotarizedChain() {
        return findBiggestChainMatching(genesisParentHash, _ -> true);
    }

    public List<Block> getBiggestFinalizedChain() {
        return findBiggestChainMatching(genesisParentHash, BlockNode::finalized);
    }

    private List<Block> findBiggestChainMatching(Hash parentHash, Predicate<BlockNode> predicate) {
        List<Block> chain = new LinkedList<>();

        if (!parentHash.equals(genesisParentHash)) {
            chain.add(blockNodesByHash.get(parentHash).block());
        }

        chain.addAll(
                blockchainByParentHash.get(parentHash).stream()
                        .filter(predicate)
                        .map(child -> findBiggestChainMatching(new Hash(child.block().getSHA1()), predicate))
                        .max(Comparator.comparing(List::size))
                        .orElseGet(LinkedList::new)
        );
        return chain;
    }

    public boolean onPropose(Block proposedBlock) {
        boolean isLongerThanAnyChain = blockchainByParentHash.keySet().stream()
                .filter(parentHash -> blockchainByParentHash.get(parentHash).isEmpty())
                .map(blockNodesByHash::get)
                .filter(Objects::nonNull)
                .anyMatch(blockNode -> proposedBlock.length() > blockNode.block().length());

        if (!isLongerThanAnyChain) {
            return false;
        }

        pendingProposals.add(proposedBlock);

        Hash blockHash = new Hash(proposedBlock.getSHA1());
        BlockNode blockNode = new BlockNode(proposedBlock, false);
        blockNodesByHash.put(blockHash, blockNode);
        return true;
    }

    public void notarizeBlock(Block blockHeader) {
        Block fullBlock = pendingProposals.stream()
                .filter(blockHeader::equals)
                .findFirst()
                .orElse(null);
        if (fullBlock == null) return;

        Hash parentHash = new Hash(fullBlock.parentHash());
        Hash blockHash = new Hash(fullBlock.getSHA1());
        BlockNode blockNode = blockNodesByHash.get(blockHash);

        blockchainByParentHash.computeIfAbsent(parentHash, _ -> new LinkedList<>())
                .add(blockNode);
        blockchainByParentHash.computeIfAbsent(blockHash, _ -> new LinkedList<>());

        if (blockHeader.epoch() > mostRecentNotarizedEpoch) {
            mostRecentNotarizedEpoch = blockHeader.epoch();
        }

        pendingProposals.remove(blockHeader);

        AppLogger.logInfo("Block notarized: epoch " + blockHeader.epoch() + " length " + blockHeader.length());
        finalizeAndPropagate(blockNode);
    }

    private void finalizeAndPropagate(BlockNode targetBlock) {
        propagateFinalizedStatusDownstream(new Hash(targetBlock.block().getSHA1()));
        finalizeByConsecutiveEpochBlocks(targetBlock);
    }

    private void propagateFinalizedStatusDownstream(Hash parentHash) {
        for (BlockNode child : blockchainByParentHash.get(parentHash)) {
            if (child.finalized()) {
                finalizeChainUpstream(child);
            }
            propagateFinalizedStatusDownstream(new Hash(child.block().getSHA1()));
        }
    }

    private void finalizeByConsecutiveEpochBlocks(BlockNode anchorBlock) {
        List<BlockNode> blocksBefore = collectPrecedingConsecutiveBlocks(anchorBlock);
        List<BlockNode> blocksAfter = collectFollowingConsecutiveBlocks(anchorBlock);

        List<BlockNode> finalizationCandidate = new LinkedList<>(blocksBefore);
        finalizationCandidate.add(anchorBlock);
        finalizationCandidate.addAll(blocksAfter);

        if (finalizationCandidate.size() >= FINALIZATION_MIN_SIZE) {
            finalizeChainUpstream(finalizationCandidate.getLast());
        }
    }

    private List<BlockNode> collectFollowingConsecutiveBlocks(BlockNode startBlock) {
        List<BlockNode> consecutiveBlocks = new LinkedList<>();
        BlockNode currentBlock = startBlock;
        int currentEpoch = startBlock.block().epoch();

        for (int i = 1; i < FINALIZATION_MIN_SIZE; i++) {
            List<BlockNode> children = blockchainByParentHash.get(new Hash(currentBlock.block().getSHA1()));

            int targetEpoch = currentEpoch + 1;
            Optional<BlockNode> nextBlock = children.stream()
                    .filter(blockNode -> blockNode.block().epoch() == targetEpoch)
                    .findFirst();

            if (nextBlock.isEmpty()) break;

            BlockNode child = nextBlock.get();
            currentEpoch++;
            consecutiveBlocks.add(child);
            currentBlock = child;
        }

        return consecutiveBlocks;
    }

    private List<BlockNode> collectPrecedingConsecutiveBlocks(BlockNode startBlock) {
        List<BlockNode> consecutiveBlocks = new LinkedList<>();
        BlockNode currentBlock = startBlock;
        int currentEpoch = startBlock.block().epoch();

        for (int i = 1; i < FINALIZATION_MIN_SIZE; i++) {
            BlockNode parentBlock = blockNodesByHash.get(new Hash(currentBlock.block().parentHash()));

            if (parentBlock == null
                    || parentBlock.block().epoch() != currentEpoch - 1
                    || isGenesis(parentBlock)) {
                break;
            }

            currentEpoch--;
            consecutiveBlocks.add(parentBlock);
            currentBlock = parentBlock;
        }

        return consecutiveBlocks;
    }

    private void finalizeChainUpstream(BlockNode anchorBlock) {
        BlockNode parentBlock = blockNodesByHash.get(new Hash(anchorBlock.block().parentHash()));

        for (BlockNode currentBlock = parentBlock;
             currentBlock != null && !isGenesis(currentBlock);
             currentBlock = blockNodesByHash.get(new Hash(currentBlock.block().parentHash()))) {
            if (currentBlock.finalized()) break;
            currentBlock.finalizeBlock();
        }
    }

    private boolean isGenesis(BlockNode block) {
        return block.block().equals(GENESIS_BLOCK);
    }

    public int getLastNotarizedEpoch() {
        return mostRecentNotarizedEpoch;
    }

    public List<BlockNode> getBlocksInEpochRange(int fromEpoch, int toEpoch) {
        return blockchainByParentHash.values().stream()
                .flatMap(List::stream)
                .sorted(Comparator.comparing(block -> block.block().epoch()))
                .dropWhile(block -> block.block().epoch() < fromEpoch)
                .takeWhile(block -> block.block().epoch() < toEpoch)
                .toList();
    }

    public void insertMissingBlocks(List<BlockNode> missingBlocks) {
        for (BlockNode blockNode : missingBlocks) {
            if (!recoveredBlocks.add(blockNode)) continue;

            Hash parentHash = new Hash(blockNode.block().parentHash());
            Hash blockHash = new Hash(blockNode.block().getSHA1());

            blockchainByParentHash.computeIfAbsent(parentHash, _ -> new LinkedList<>())
                    .add(blockNode);
            blockchainByParentHash.computeIfAbsent(blockHash, _ -> new LinkedList<>());
            blockNodesByHash.put(blockHash, blockNode);
        }
    }

    public void printBiggestFinalizedChain() {
        final String GREEN = "\u001B[32m";
        final String RESET = "\u001B[0m";

        String header = "=== LONGEST FINALIZED CHAIN ===";
        String border = "=".repeat(header.length());

        List<Block> finalizedChain = getBiggestFinalizedChain();

        String chainString = finalizedChain.stream()
                .skip(1)
                .map(block -> "%sBlock[%d-%d]%s".formatted(GREEN, block.epoch(), block.length(), RESET))
                .collect(Collectors.joining(" <- ", "%sGENESIS%s <- ".formatted(GREEN, RESET), ""));

        String output = String.format(
                "%s%n%s%n%s%n%s%n%s",
                border,
                header,
                border,
                finalizedChain.size() == 1 ? "No Finalized Chain Yet" : chainString,
                border
        );

        synchronized (AppLogger.class) {
            for (String line : output.split("\n")) {
                AppLogger.logInfo(line);
            }
        }
    }
}