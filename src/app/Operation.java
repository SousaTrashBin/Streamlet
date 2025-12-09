package app;

import utils.application.Block;

public interface Operation {
    Type getType();

    Block getBlock();

    String getPersistenceString();

    enum Type {
        PROPOSE,
        NOTARIZE,
        FINALIZE
    }

    record Propose(Block block) implements Operation {
        @Override
        public Type getType() {
            return Type.PROPOSE;
        }

        @Override
        public Block getBlock() {
            return block;
        }

        @Override
        public String getPersistenceString() {
            return "PROPOSE:" + block.getPersistenceString();
        }
    }

    record Notarize(Block block) implements Operation {
        @Override
        public Type getType() {
            return Type.NOTARIZE;
        }

        @Override
        public Block getBlock() {
            return block;
        }

        @Override
        public String getPersistenceString() {
            return "NOTARIZE:" + block.getPersistenceString();
        }
    }

    record Finalize(Block block) implements Operation {
        @Override
        public Type getType() {
            return Type.FINALIZE;
        }

        @Override
        public Block getBlock() {
            return block;
        }

        @Override
        public String getPersistenceString() {
            return "FINALIZE:" + block.getPersistenceString();
        }
    }
}
