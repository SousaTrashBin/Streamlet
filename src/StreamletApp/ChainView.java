package StreamletApp;

import utils.application.Block;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

// this is just a wrapper to overwrite both hashcode and equals
record ChainView(LinkedList<Block> blocks) {
    @Override
    public int hashCode() {
        return Objects.hash(blocks.stream().map(Block::getSHA1).toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ChainView(LinkedList<Block> blocks1))) return false;
        if (blocks.size() != blocks1.size()) return false;
        for (int i = 0; i < blocks.size(); i++) {
            if (!Arrays.equals(blocks.get(i).getSHA1(), blocks1.get(i).getSHA1())) return false;
        }
        return true;
    }
}