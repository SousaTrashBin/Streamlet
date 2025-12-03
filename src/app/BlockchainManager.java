package app;

import utils.application.Block;
import utils.application.Transaction;
import utils.logs.AppLogger;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BlockchainManager {
    public static final int FINALIZATION_MIN_SIZE = 3;
    private static final int SHA1_LENGTH = 20;
    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);

    private final Map<Hash, List<BlockNode>> blockchain = new HashMap<>();
    private final Map<Hash, BlockNode> hashToBlockNode = new HashMap<>();
    private final Set<BlockNode> blocksForRecovery = new HashSet<>();
    private final Set<Block> pendingProposes = new HashSet<>();

    private int mostRecentEpoch = -1;

    public BlockchainManager() {
        BlockNode genesis = new BlockNode(GENESIS_BLOCK, true);
        Hash genesisParentHash = new Hash(GENESIS_BLOCK.parentHash());
        Hash genesisHash = new Hash(GENESIS_BLOCK.getSHA1());

        List<BlockNode> root = new LinkedList<>();
        root.add(genesis);
        blockchain.put(genesisParentHash, root);
        blockchain.put(genesisHash, new LinkedList<>());

        hashToBlockNode.put(genesisHash, genesis);
    }

    public List<Block> getBiggestNotarizedChain() {
        return findBiggestChainFromPredicate(new Hash(GENESIS_BLOCK.parentHash()), _ -> true);
    }

    public List<Block> getBiggestFinalizedChain() {
        return findBiggestChainFromPredicate(new Hash(GENESIS_BLOCK.parentHash()), BlockNode::finalized);
    }

    private List<Block> findBiggestChainFromPredicate(Hash parentHash, Predicate<BlockNode> predicate) {
        List<Block> result = new LinkedList<>();

        if (!parentHash.equals(new Hash(GENESIS_BLOCK.parentHash())))
            result.add(hashToBlockNode.get(parentHash).block());

        result.addAll(
                blockchain.get(parentHash).stream()
                        .filter(predicate::test)
                        .map(child -> findBiggestChainFromPredicate(new Hash(child.block().getSHA1()), predicate))
                        .max(Comparator.comparing(List::size))
                        .orElseGet(LinkedList::new)
        );
        return result;
    }

    public boolean onPropose(Block proposedBlock) {
        // Check if this block is strictly longer than any notarized chain this block has seen thus far 
        boolean isStrictlyLonger = blockchain.keySet().stream()
                .filter(parentHash -> blockchain.get(parentHash).isEmpty())
                .anyMatch(hash -> proposedBlock.length() > hashToBlockNode.get(hash).block().length());
        if (!isStrictlyLonger) {
            return false;
        }
        pendingProposes.add(proposedBlock);

        // While recovering from crash, a node will likely receive propose messages
        // even if it still does not have some of the previous blocks it needs
        // so this block is placed in the data structure anyway and
        // it can be linked with the others later when/if this node receives the previous blocks
        Hash blockHash = new Hash(proposedBlock.getSHA1());
        BlockNode blockNode = new BlockNode(proposedBlock, false);
        hashToBlockNode.put(blockHash, blockNode);
        return true;
    }

    public void notarizeBlock(Block headerBlock) {
        // The same block with or without its transactions are considered equal
        Block fullBlock = null;
        for (Block pendingBlock : pendingProposes) {
            if (headerBlock.equals(pendingBlock)) {
                fullBlock = pendingBlock;
                break;
            }
        }
        if (fullBlock == null) return;

        // Add to the blockchain and prepare another entry with the hash of this block as parent
        // if there was not one already due to out of order proposals
        Hash parentHash = new Hash(fullBlock.parentHash());
        Hash blockHash = new Hash(fullBlock.getSHA1());
        BlockNode block = hashToBlockNode.get(blockHash);
        blockchain.computeIfAbsent(parentHash, _ -> new LinkedList<>())
                .add(block);
        blockchain.computeIfAbsent(blockHash, _ -> new LinkedList<>());

        if (headerBlock.epoch() > mostRecentEpoch) mostRecentEpoch = headerBlock.epoch();

        pendingProposes.remove(headerBlock);

        AppLogger.logInfo("Block notarized: epoch " + headerBlock.epoch() + " length " + headerBlock.length());
        tryToFinalizeChain(block);
    }

    private void tryToFinalizeChain(BlockNode block) {
        // For every child of this block or of the next blocks, finalize the chain if the child is finalized
        finalizeFromTheNextBlocks(new Hash(block.block().getSHA1()));

        // Finds an epoch interval of blocks from the epoch of this block +-
        // the min consecutive blocks needed to finalize a chain.
        // If this interval of consecutive blocks has enough size,
        // finalize the chain until the last block of this interval
        finalizeFromConsecutivesOf(block);
    }

    private void finalizeFromTheNextBlocks(Hash parentHash) {
        for (BlockNode child : blockchain.get(parentHash)) {
            if (child.finalized()) finalizeFrom(child);
            finalizeFromTheNextBlocks(new Hash(child.block().getSHA1()));
        }
    }

    private void finalizeFromConsecutivesOf(BlockNode block) {
        List<BlockNode> beforeBlockInConsecutiveEpochs = new LinkedList<>();
        List<BlockNode> afterBlockInConsecutiveEpochs = new LinkedList<>();

        // After block in consecutive epochs
        BlockNode currBlock = block;
        int currEpoch = block.block().epoch();
        for (int i = 1; i < FINALIZATION_MIN_SIZE; i++) {
            List<BlockNode> children = blockchain.get(new Hash(currBlock.block().getSHA1()));

            final int currEpochCopy = currEpoch;
            Optional<BlockNode> maybeChild = children.stream()
                    .filter(blockNode -> blockNode.block().epoch() == currEpochCopy + 1)
                    .findFirst();

            if (maybeChild.isEmpty()) break;
            BlockNode child = maybeChild.get();

            currEpoch++;
            afterBlockInConsecutiveEpochs.add(child);
            currBlock = child;
        }

        // Before block in consecutive epochs
        currBlock = block;
        currEpoch = block.block().epoch();
        for (int i = 1; i < FINALIZATION_MIN_SIZE; i++) {
            currBlock = hashToBlockNode.get(new Hash(currBlock.block().parentHash()));
            if (currBlock == null
                    || currBlock.block().epoch() != currEpoch - 1
                    || currBlock.equals(new BlockNode(GENESIS_BLOCK, true)))
                break;

            currEpoch--;
            beforeBlockInConsecutiveEpochs.add(currBlock);
        }

        // Build interval of consecutive blocks
        List<BlockNode> finalizeInterval = new LinkedList<>();
        finalizeInterval.addAll(beforeBlockInConsecutiveEpochs);
        finalizeInterval.add(block);
        finalizeInterval.addAll(afterBlockInConsecutiveEpochs);

        if (finalizeInterval.size() >= FINALIZATION_MIN_SIZE) finalizeFrom(finalizeInterval.getLast());
    }

    private void finalizeFrom(BlockNode block) {
        BlockNode finalizationStarter = hashToBlockNode.get(new Hash(block.block().parentHash()));

        for (BlockNode currBlock = finalizationStarter;
             !currBlock.block().equals(GENESIS_BLOCK);
             currBlock = hashToBlockNode.get(new Hash(currBlock.block().parentHash()))) {
            if (currBlock.finalized()) break;
            currBlock.finalizeBlock();
        }
    }

    public int getLastEpoch() {
        return mostRecentEpoch;
    }

    public List<BlockNode> blocksFromToEpoch(int from, int to) {
        return blockchain.values().stream()
                .flatMap(List::stream)
                .sorted(Comparator.comparing(block -> block.block().epoch()))
                .dropWhile(block -> block.block().epoch() < from)
                .takeWhile(block -> block.block().epoch() < to)
                .toList();
    }

    public void insertMissingBlocks(List<BlockNode> missingBlocks) {
        // It is possible that a proposed block that arrives to the blockchain before calling this method
        // would supposingly finalize any of these missing blocks but in that case,
        // then they will eventually be finalized because the next proposed blocks
        // extend from the biggest chain which would have to be this one according to the consistency proof
        for (BlockNode missingBlock : missingBlocks) {
            // Ignore if it was already inserted in the blockchain from a previous UPDATE message
            if (!blocksForRecovery.add(missingBlock)) continue;

            Hash parentHash = new Hash(missingBlock.block().parentHash());
            Hash blockHash = new Hash(missingBlock.block().getSHA1());
            blockchain.computeIfAbsent(parentHash, _ -> new LinkedList<>())
                    .add(missingBlock);
            blockchain.computeIfAbsent(blockHash, _ -> new LinkedList<>());
            hashToBlockNode.put(blockHash, missingBlock);
        }
    }

    public void printBiggestFinalizedChain() {
        final String GREEN = "\u001B[32m";
        final String RESET = "\u001B[0m";

        String header = "=== LONGEST FINALIZED CHAIN ===";
        String border = "=".repeat(header.length());

        List<Block> biggestFinalizedChain = getBiggestFinalizedChain();

        String chainString = biggestFinalizedChain.stream()
                .skip(1)
                .map(block -> "%sBlock[%d-%d]%s".formatted(GREEN, block.epoch(), block.length(), RESET))
                .collect(Collectors.joining(" <- ", "%sGENESIS%s <- ".formatted(GREEN, RESET), ""));

        String output = String.format(
                "%s%n%s%n%s%n%s%n%s",
                border,
                header,
                border,
                biggestFinalizedChain.size() == 1 ? "No Finalized Chain Yet" : chainString,
                border
        );

        synchronized (AppLogger.class) {
            for (String line : output.split("\n")) {
                AppLogger.logInfo(line);
            }
        }

    }
}
