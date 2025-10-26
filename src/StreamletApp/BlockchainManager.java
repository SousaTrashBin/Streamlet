package StreamletApp;

import utils.application.Block;
import utils.application.BlockWithChain;
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
    private final HashMap<Block, BlockWithChain> pendingProposes = new HashMap<>();
    private LinkedList<Block> biggestNotarizedChain = new LinkedList<>();

    public BlockchainManager() {
        biggestNotarizedChain.add(GENESIS_BLOCK);
        seenNotarizedChains.add(new ChainView(biggestNotarizedChain));
    }

    public void notarizeBlock(Block headerBlock) {
        BlockWithChain proposal = pendingProposes.get(headerBlock);
        if (proposal == null) {
            return;
        }
        LinkedList<Block> chain = proposal.chain();
        chain.add(proposal.block());
        pendingProposes.remove(headerBlock);
        seenNotarizedChains.add(new ChainView(chain));
        if (chain.getLast().length() > biggestNotarizedChain.getLast().length()) {
            biggestNotarizedChain = chain;
        }
        AppLogger.logInfo("Block notarized: epoch " + headerBlock.epoch() + " length " + headerBlock.length());
        tryToFinalizeChain(chain);
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

    public boolean onPropose(BlockWithChain proposal) {
        Block proposedBlock = proposal.block();
        LinkedList<Block> chain = proposal.chain();
        Block parentTip = chain.getLast();

        if (!Arrays.equals(proposedBlock.parentHash(), parentTip.getSHA1())) return false;

        boolean isStrictlyLonger = seenNotarizedChains.stream()
                .anyMatch(notarizedChain -> proposedBlock.length() > notarizedChain.blocks().getLast().length());
        if (!isStrictlyLonger) {
            return false;
        }
        pendingProposes.put(proposedBlock, proposal);
        return true;
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

    public LinkedList<Block> getBiggestNotarizedChain() {
        return biggestNotarizedChain;
    }
}
