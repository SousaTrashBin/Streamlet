package StreamletApp;

import java.util.Arrays;

record Hash(byte[] hash) {

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Hash hashOther)) return false;

        return Arrays.equals(this.hash, hashOther.hash);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(hash);
    }
}
