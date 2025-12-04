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
    private final Map<Hash, List<BlockNode>> blockchainByParentHash = new HashMap<>();
    private final Set<BlockNode> recoveredBlocks = new HashSet<>();
    private final Set<Block> pendingProposals = new HashSet<>();

    private final PersistenceFilesManager persistenceManager;

    private int mostRecentNotarizedEpoch = -1;

    public BlockchainManager(Path outputPath) {
        Path logFilePath = outputPath.resolve(LOG_FILE_NAME);
        Path blockchainFilePath = outputPath.resolve(BLOCK_CHAIN_FILE_NAME);
        persistenceManager = new PersistenceFilesManager(logFilePath, blockchainFilePath, outputPath);
        mostRecentNotarizedEpoch = persistenceManager.initializeFromFile(
            blockNodesByHash, blockchainByParentHash, recoveredBlocks, pendingProposals
        );

        BlockNode genesisNode = new BlockNode(GENESIS_BLOCK, true);
        genesisParentHash = new Hash(GENESIS_BLOCK.parentHash());
        Hash genesisHash = new Hash(GENESIS_BLOCK.getSHA1());

        AppLogger.logWarning("RESTARTING AND THE GENESIS'S PARENT HAS THIS MANY CHILDREN...");
        if (blockchainByParentHash.get(genesisParentHash) != null) {
            AppLogger.logWarning("" + blockchainByParentHash.get(genesisParentHash).size());
            AppLogger.logWarning("LENGTH BLOCK NODES BY HASH: " + blockNodesByHash.size());
            AppLogger.logWarning("LENGTH BLOCK CHAIN BY PARENT HASH: " + blockchainByParentHash.size());
        } else {
            AppLogger.logWarning("GENESIS NOT INSERTED");
        }

        List<BlockNode> genesisRoot = new LinkedList<>();
        genesisRoot.add(genesisNode);
        blockchainByParentHash.putIfAbsent(genesisParentHash, genesisRoot);
        blockchainByParentHash.putIfAbsent(genesisHash, new LinkedList<>());

        blockNodesByHash.putIfAbsent(genesisHash, genesisNode);
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
        blockchainByParentHash.forEach((hash, children) -> {
            sb.append("%s:[%s]".formatted(
                            hash.getPersistenceString(),
                            children.stream().map(BlockNode::getPersistenceString).collect(Collectors.joining(","))
                    )
            );
            sb.append("\n");
        });
        sb.append("\n\n");
        sb.append("%s".formatted(recoveredBlocks.stream().map(BlockNode::getPersistenceString).collect(Collectors.joining("\n"))));
        sb.append("\n\n\n");
        sb.append("%s".formatted(pendingProposals.stream().map(Block::getPersistenceString).collect(Collectors.joining("\n"))));
        return sb.toString();
    }

    public List<Block> getBiggestNotarizedChain() {
        AppLogger.logWarning("STARTING THE SEARCH...");
        AppLogger.logWarning("LENGTH BLOCK NODES BY HASH: " + blockNodesByHash.size());
        AppLogger.logWarning("LENGTH BLOCK CHAIN BY PARENT HASH: " + blockchainByParentHash.size());
        AppLogger.logWarning("LENGTH OF GENESIS PARENT CHILDREN: " + blockchainByParentHash.get(genesisParentHash).size());
        AppLogger.logWarning("GENESIS PARENT HASH: " + Base64.getEncoder().encodeToString(genesisParentHash.hash()));
        return findBiggestChainMatching(genesisParentHash, _ -> true);
    }

    public List<Block> getBiggestFinalizedChain() {
        return findBiggestChainMatching(genesisParentHash, BlockNode::finalized);
    }

    private List<Block> findBiggestChainMatching(Hash parentHash, Predicate<BlockNode> predicate) {
        List<Block> chain = new LinkedList<>();

        AppLogger.logWarning("FINDING BIGGEST CHAIN ON EPOCH...");

        if (!parentHash.equals(genesisParentHash)) {
            AppLogger.logWarning(blockNodesByHash.get(parentHash).block().epoch().toString());
            chain.add(blockNodesByHash.get(parentHash).block());
        }

        for (BlockNode child : blockchainByParentHash.get(parentHash)) {
            AppLogger.logWarning("\tCHILD");
            AppLogger.logWarning("\t" + child.getPersistenceString());
            AppLogger.logWarning("\tWITH HASH: " + Base64.getEncoder().encodeToString(child.block().getSHA1()));
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
            finalizeAndPropagate(blockNode);
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