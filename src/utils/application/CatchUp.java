package utils.application;

import java.util.LinkedList;
import java.util.Random;

public record CatchUp(
    Integer slackerId,
    LinkedList<Block> missingChain,
    Random leaderRand,
    Integer leaderId,
    Integer currentEpoch
) implements Content {

    public CatchUp(
        Integer slackerId,
        LinkedList<Block> missingChain,
        Random leaderRand,
        Integer leaderId,
        Integer currentEpoch
    ) {
        this.slackerId = slackerId;
        this.missingChain = new LinkedList<>(missingChain);
        this.leaderRand = leaderRand;
        this.leaderId = leaderId;
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
