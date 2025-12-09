package utils.application;

import app.BlockNode;

import java.util.LinkedList;
import java.util.List;

public record CatchUp(Integer slackerId, List<BlockNode> missingChain, Integer currentEpoch) implements Content {

    public CatchUp(Integer slackerId, List<BlockNode> missingChain, Integer currentEpoch) {
        this.slackerId = slackerId;
        this.missingChain = new LinkedList<>(missingChain);
        this.currentEpoch = currentEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CatchUp catchUp)) return false;

        return slackerId.equals(catchUp.slackerId) && currentEpoch.equals(catchUp.currentEpoch);
    }

    @Override
    public int hashCode() {
        return 31 * slackerId.hashCode() + currentEpoch.hashCode();
    }
}
