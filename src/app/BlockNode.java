package app;

import utils.application.Block;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlockNode implements Serializable {
    private static final Pattern BLOCK_NODE_REGEX = Pattern.compile(
            "BlockNode\\[(?<finalized>true|false),(?<block>.*)]"
    );

    private final Block block;
    private Boolean finalized;

    public BlockNode(Block block, Boolean finalized) {
        this.block = block;
        this.finalized = finalized;
    }

    public Block block() {
        return block;
    }

    public Boolean finalized() {
        return finalized;
    }

    public void finalizeBlock() {
        finalized = true;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BlockNode blockNode)) return false;

        return block.equals(blockNode.block);
    }

    @Override
    public int hashCode() {
        return block.hashCode();
    }

    public static BlockNode fromPersistenceString(String persistenceString) {
        Matcher matcher = BLOCK_NODE_REGEX.matcher(persistenceString);
        if (!matcher.matches()) {
            return null;
        }

        Boolean finalized = Boolean.parseBoolean(matcher.group("finalized"));
        String blockString = matcher.group("block");
        Block block = Block.fromPersistenceString(blockString);

        return new BlockNode(block, finalized);
    }

    public String getPersistenceString() {
        return "BlockNode[%s,%s]".formatted(finalized, block.getPersistenceString());
    }
}
