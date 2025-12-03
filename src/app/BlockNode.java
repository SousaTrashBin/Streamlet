package app;

import utils.application.Block;

import java.io.Serializable;

public class BlockNode implements Serializable {

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
}
