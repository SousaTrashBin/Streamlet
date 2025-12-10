package app;

import utils.application.Block;
import utils.application.Hash;
import utils.application.Transaction;
import utils.logs.AppLogger;

import java.nio.file.Path;
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

    private final Map<Hash, BlockNode> blockNodesByHash = new HashMap<>();
    private final Map<Hash, List<Hash>> blockchainByParentHash = new HashMap<>();
    private final Set<BlockNode> recoveredBlocks = new HashSet<>();
    private final Set<Block> pendingProposals = new HashSet<>();

    private final PersistenceFilesManager persistenceManager;

    private int mostRecentNotarizedEpoch;

    private boolean isRestoring = false;

    public BlockchainManager(Path outputPath) {
        Path logFilePath = outputPath.resolve(LOG_FILE_NAME);
        Path blockchainFilePath = outputPath.resolve(BLOCK_CHAIN_FILE_NAME);
        persistenceManager = new PersistenceFilesManager(logFilePath, blockchainFilePath, outputPath);

        mostRecentNotarizedEpoch = persistenceManager.initializeFromFile(
                blockNodesByHash, blockchainByParentHash, recoveredBlocks, pendingProposals
        );

        isRestoring = true;
        persistenceManager.getPendingOperations().forEach(this::processOperation);
        isRestoring = false;

        BlockNode genesisNode = new BlockNode(GENESIS_BLOCK, true);
        genesisParentHash = new Hash(GENESIS_BLOCK.parentHash());
        Hash genesisHash = new Hash(GENESIS_BLOCK.getSHA1());

        List<Hash> genesisRoot = new LinkedList<>();
        genesisRoot.add(genesisHash);
        blockchainByParentHash.putIfAbsent(genesisParentHash, genesisRoot);
        blockchainByParentHash.putIfAbsent(genesisHash, new LinkedList<>());

        blockNodesByHash.putIfAbsent(genesisHash, genesisNode);

        restoreFinalizationStateFromTips();
    }

    private void restoreFinalizationStateFromTips() {
        blockNodesByHash.values().stream()
                .filter(node -> {
                    Hash hash = new Hash(node.block().getSHA1());
                    List<Hash> children = blockchainByParentHash.get(hash);
                    return children == null || children.isEmpty();
                })
                .forEach(this::finalizeAndPropagate);
    }

    public boolean containsBlock(Hash blockHash) {
        return blockNodesByHash.containsKey(blockHash);
    }

    private void processOperation(Operation operation) {
        switch (operation.getType()) {
            case PROPOSE -> onPropose(operation.getBlock());
            case NOTARIZE -> notarizeBlock(operation.getBlock());
            case FINALIZE -> {
                Hash hash = new Hash(operation.getBlock().getSHA1());
                BlockNode node = blockNodesByHash.get(hash);
                if (node != null && !node.finalized()) {
                    finalizeChainUpstream(node);
                }
            }
        }
    }

    public void persistToFile() {
        persistenceManager.persistToFile(getPersistenceString());
    }

    private String getPersistenceString() {
        StringBuilder sb = new StringBuilder();

        blockNodesByHash.forEach((hash, blockNode) -> {
            sb.append("%s:%s".formatted(hash.getPersistenceString(), blockNode.getPersistenceString()));
            sb.append("\n");
        });
        sb.append("\n\n");

        blockchainByParentHash.forEach((hash, childHashes) -> {
            String childrenStr = childHashes.stream()
                    .map(Hash::getPersistenceString)
                    .collect(Collectors.joining(","));

            sb.append("%s:[%s]".formatted(hash.getPersistenceString(), childrenStr));
            sb.append("\n");
        });

        sb.append("\n\n");
        sb.append("%s".formatted(recoveredBlocks.stream().map(BlockNode::getPersistenceString).collect(Collectors.joining("\n"))));
        sb.append("\n\n");
        sb.append("%s".formatted(pendingProposals.stream().map(Block::getPersistenceString).collect(Collectors.joining("\n"))));
        return sb.toString();
    }

    public List<Block> getBiggestNotarizedChain() {
        return getChainEndingAtBestTip(_ -> true);
    }

    public List<Block> getBiggestFinalizedChain() {
        return getChainEndingAtBestTip(BlockNode::finalized);
    }

    private List<Block> getChainEndingAtBestTip(Predicate<BlockNode> predicate) {
        BlockNode bestTip = blockNodesByHash.values().stream()
                .filter(predicate)
                .max(Comparator.comparingInt((BlockNode n) -> n.block().length())
                        .thenComparing(n -> new Hash(n.block().getSHA1()).toString()))
                .orElse(null);

        if (bestTip == null) {
            return new ArrayList<>();
        }

        LinkedList<Block> chain = new LinkedList<>();
        BlockNode current = bestTip;

        while (current != null) {
            chain.addFirst(current.block());

            if (isGenesis(current)) break;

            Hash parentHash = new Hash(current.block().parentHash());
            current = blockNodesByHash.get(parentHash);
        }

        return chain;
    }

    public boolean onPropose(Block proposedBlock) {
        Hash blockHash = new Hash(proposedBlock.getSHA1());

        if (blockNodesByHash.containsKey(blockHash)) {
            return false;
        }

        boolean isLongerThanAnyChain = blockchainByParentHash.keySet().stream()
                .filter(parentHash -> {
                    List<Hash> children = blockchainByParentHash.get(parentHash);
                    return children == null || children.isEmpty();
                })
                .map(blockNodesByHash::get)
                .filter(Objects::nonNull)
                .anyMatch(blockNode -> proposedBlock.length() > blockNode.block().length());

        if (!isLongerThanAnyChain) {
            return false;
        }

        pendingProposals.add(proposedBlock);

        BlockNode blockNode = new BlockNode(proposedBlock, false);
        blockNodesByHash.put(blockHash, blockNode);

        if (!isRestoring) {
            persistenceManager.appendToLog(new Operation.Propose(proposedBlock));
        }

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
                .add(blockHash);
        blockchainByParentHash.computeIfAbsent(blockHash, _ -> new LinkedList<>());

        if (blockHeader.epoch() > mostRecentNotarizedEpoch) {
            mostRecentNotarizedEpoch = blockHeader.epoch();
        }

        pendingProposals.remove(blockHeader);

        if (!isRestoring) {
            persistenceManager.appendToLog(new Operation.Notarize(fullBlock));
        }

        AppLogger.logInfo("Block notarized: epoch " + blockHeader.epoch() + " length " + blockHeader.length());
        finalizeAndPropagate(blockNode);
    }

    private void finalizeAndPropagate(BlockNode targetBlock) {
        propagateFinalizedStatusDownstream(new Hash(targetBlock.block().getSHA1()));
        finalizeByConsecutiveEpochBlocks(targetBlock);
    }

    private void propagateFinalizedStatusDownstream(Hash parentHash) {
        List<Hash> childrenHashes = blockchainByParentHash.get(parentHash);
        if (childrenHashes == null || childrenHashes.isEmpty()) return;

        for (Hash childHash : childrenHashes) {
            BlockNode child = blockNodesByHash.get(childHash);
            if (child == null) continue;

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
            BlockNode lastBlock = finalizationCandidate.getLast();
            finalizeChainUpstream(lastBlock);
            verifyAndFinalizeBasedOnChildren(lastBlock);
        }
    }

    private void verifyAndFinalizeBasedOnChildren(BlockNode lastBlock) {
        List<Hash> childrenHashes = blockchainByParentHash.get(new Hash(lastBlock.block().getSHA1()));

        if (childrenHashes != null && !childrenHashes.isEmpty()) {
            for (Hash childHash : childrenHashes) {
                BlockNode child = blockNodesByHash.get(childHash);
                if (child != null && !child.finalized()) {
                    finalizeByConsecutiveEpochBlocks(child);
                }
            }
        }
    }

    private List<BlockNode> collectFollowingConsecutiveBlocks(BlockNode startBlock) {
        List<BlockNode> consecutiveBlocks = new LinkedList<>();
        BlockNode currentBlock = startBlock;
        int currentEpoch = startBlock.block().epoch();

        for (int i = 1; i < FINALIZATION_MIN_SIZE; i++) {
            List<Hash> childHashes = blockchainByParentHash.get(new Hash(currentBlock.block().getSHA1()));
            if (childHashes == null) break;

            int targetEpoch = currentEpoch + 1;
            Optional<BlockNode> nextBlock = childHashes.stream()
                    .map(blockNodesByHash::get)
                    .filter(Objects::nonNull)
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
        for (BlockNode currentBlock = anchorBlock;
             currentBlock != null && !isGenesis(currentBlock);
             currentBlock = blockNodesByHash.get(new Hash(currentBlock.block().parentHash()))) {
            if (currentBlock.finalized()) break;
            currentBlock.finalizeBlock();

            if (!isRestoring) {
                persistenceManager.appendToLog(new Operation.Finalize(currentBlock.block()));
            }
        }
    }

    private boolean isGenesis(BlockNode block) {
        return block.block().equals(GENESIS_BLOCK);
    }

    public int getLastNotarizedEpoch() {
        return mostRecentNotarizedEpoch;
    }

    public List<BlockNode> getBlocksInEpochRange(int fromEpoch, int toEpoch) {
        return blockNodesByHash.values().stream()
                .sorted(Comparator.comparing(block -> block.block().epoch()))
                .dropWhile(block -> block.block().epoch() < fromEpoch)
                .takeWhile(block -> block.block().epoch() < toEpoch)
                .toList();
    }

    public void insertMissingBlocks(List<BlockNode> missingBlocks) {
        Set<Hash> affectedBlocks = new HashSet<>();

        for (BlockNode blockNode : missingBlocks) {
            Hash parentHash = new Hash(blockNode.block().parentHash());
            Hash blockHash = new Hash(blockNode.block().getSHA1());

            recoveredBlocks.add(blockNode);
            blockNodesByHash.put(blockHash, blockNode);

            List<Hash> siblings = blockchainByParentHash.computeIfAbsent(parentHash, _ -> new LinkedList<>());

            if (!siblings.contains(blockHash)) {
                siblings.add(blockHash);
            }

            blockchainByParentHash.computeIfAbsent(blockHash, _ -> new LinkedList<>());

            affectedBlocks.add(parentHash);
            affectedBlocks.add(blockHash);
        }

        affectedBlocks.stream()
                .map(blockNodesByHash::get)
                .filter(Objects::nonNull)
                .forEach(this::finalizeAndPropagate);

        persistToFile();
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

    public boolean isBlockPending(Block block) {
        return pendingProposals.contains(block);
    }
}