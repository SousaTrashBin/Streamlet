package StreamletApp;

import utils.application.Block;
import utils.application.Transaction;
import utils.logs.AppLogger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Gatherers;

public class BlockchainManager {
    public static final int FINALIZATION_MIN_SIZE = 3;
    private static final int SHA1_LENGTH = 20;
    private static final Block GENESIS_BLOCK =
            new Block(new byte[SHA1_LENGTH], 0, 0, new Transaction[0]);

    private final Set<ChainView> seenNotarizedChains = new HashSet<>();
    private final Set<ChainView> finalizedChains = new HashSet<>();
    private final Map<Block, ChainView> pendingProposes = new HashMap<>();
    private LinkedList<Block> biggestNotarizedChain = new LinkedList<>();

    public BlockchainManager() {
        biggestNotarizedChain.add(GENESIS_BLOCK);
        seenNotarizedChains.add(new ChainView(biggestNotarizedChain));
    }

    public LinkedList<Block> getBiggestNotarizedChain() {
        return biggestNotarizedChain;
    }

    public boolean onPropose(Block proposedBlock) {
        Optional<ChainView> chainOpt = seenNotarizedChains.stream()
                .filter(notarizedChain -> 
                    Arrays.equals(proposedBlock.parentHash(), notarizedChain.blocks().getLast().getSHA1()))
                .findFirst();
        if (chainOpt.isEmpty()) return false;

        LinkedList<Block> proposedChain = new LinkedList<>(chainOpt.get().blocks());
        proposedChain.add(proposedBlock);
        ChainView parentChain = new ChainView(proposedChain);

        boolean isStrictlyLonger = seenNotarizedChains.stream()
                .anyMatch(notarizedChain -> proposedBlock.length() > notarizedChain.blocks().getLast().length());
        if (!isStrictlyLonger) {
            return false;
        }
        pendingProposes.put(proposedBlock, parentChain);
        return true;
    }

    public void notarizeBlock(Block headerBlock) {
        ChainView chain = pendingProposes.get(headerBlock);
        if (chain == null) {
            return;
        }

        Block addedBlock = chain.blocks().removeLast();
        seenNotarizedChains.remove(chain);
        chain.blocks().add(addedBlock);
        seenNotarizedChains.add(chain);

        pendingProposes.remove(headerBlock);

        if (chain.blocks().getLast().length() > biggestNotarizedChain.getLast().length()) {
            biggestNotarizedChain = chain.blocks();
        }
        AppLogger.logInfo("Block notarized: epoch " + headerBlock.epoch() + " length " + headerBlock.length());
        tryToFinalizeChain(chain.blocks());
    }

    private void tryToFinalizeChain(LinkedList<Block> chain) {
        int size = chain.size();
        if (size < FINALIZATION_MIN_SIZE) return;
        boolean shouldChainBeFinalized = chain
                .subList(chain.size() - FINALIZATION_MIN_SIZE, chain.size())
                .stream()
                .map(Block::epoch)
                .gather(Gatherers.windowSliding(2)) // zip xs $ tail xs
                .map(window -> window.getLast() - window.getFirst())
                .allMatch(delta -> delta == 1);
        if (shouldChainBeFinalized) {
            finalizedChains.add(new ChainView(new LinkedList<>(biggestNotarizedChain.subList(0, size - 1))));
        }
    }

    public int getLastEpoch() {
        // When we persist the blockchain in the disk,
        // the file should contain the current epoch in the 1st line to increase performance
        int lastEpoch = -1;
        for (ChainView chain : seenNotarizedChains) {
            Block lastChainBlock = chain.blocks().getLast();
            int highestEpochInChain = lastChainBlock.epoch();
            if (!lastChainBlock.equals(GENESIS_BLOCK) && highestEpochInChain > lastEpoch)
                lastEpoch = highestEpochInChain;
        }
        return lastEpoch;
    }

    public LinkedList<Block> blocksFromToEpoch(int from, int to) {
        LinkedList<Block> missingBlocks = new LinkedList<>();
        seenNotarizedChains.stream()
                .forEach(chain -> chain.blocks().stream()
                        .forEach(block -> {
                            if (from <= block.epoch() && block.epoch() < to) missingBlocks.add(block);
                        }));

        LinkedList<Block> missingBlocksSorted = new LinkedList<>(
            missingBlocks.stream()
                .distinct()
                .sorted()
                .toList()
        );
        return missingBlocksSorted;
    }

    public void insertMissingBlocks(LinkedList<Block> missingBlocks) {
        for (Block b : missingBlocks) {
            byte[] parentHash = b.parentHash();
            LinkedList<Block> chainOfTheBlock = null;
            boolean newChain = false;

            for (ChainView chain : seenNotarizedChains) {
                LinkedList<Block> chainBlocks = chain.blocks();
                for (int i = 0; i < chainBlocks.size(); i++) {
                    Block bChain = chainBlocks.get(i);
                    if (!Arrays.equals(parentHash, bChain.getSHA1())) continue;

                    if (bChain.equals(chainBlocks.getLast())) {
                        chainOfTheBlock = chainBlocks;
                        newChain = false;
                    } else {
                        chainOfTheBlock = new LinkedList<>(chainBlocks.subList(0, chainBlocks.indexOf(bChain) + 1));
                        newChain = true;
                    }
                    break;
                }
                if (chainOfTheBlock != null) break;
            }

            if (chainOfTheBlock == null) continue;
            chainOfTheBlock.add(b);
            if (newChain) {
                seenNotarizedChains.add(new ChainView(chainOfTheBlock));
            }
        }
    }

    public void printBiggestFinalizedChain() {
        final String GREEN = "\u001B[32m";
        final String RESET = "\u001B[0m";

        String header = "=== LONGEST FINALIZED CHAIN ===";
        String border = "=".repeat(header.length());

        LinkedList<Block> biggestFinalizedChain = finalizedChains.stream()
                .max(Comparator.comparing(c -> c.blocks().getLast().length()))
                .map(ChainView::blocks)
                .orElse(new LinkedList<>());

        String chainString = biggestFinalizedChain.stream()
                .skip(1)
                .map(block -> "%sBlock[%d-%d]%s".formatted(GREEN, block.epoch(), block.length(), RESET))
                .collect(Collectors.joining(" <- ", "%sGENESIS%s <- ".formatted(GREEN, RESET), ""));

        String output = String.format(
                "%s%n%s%n%s%n%s%n%s",
                border,
                header,
                border,
                biggestFinalizedChain.isEmpty() ? "No Finalized Chain Yet" : chainString,
                border
        );

        synchronized (AppLogger.class) {
            for (String line : output.split("\n")) {
                AppLogger.logInfo(line);
            }
        }

    }
}
