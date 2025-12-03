package app;

import java.util.Arrays;

record Hash(byte[] hash) {

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Hash(byte[] hash1))) return false;

        return Arrays.equals(this.hash, hash1);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(hash);
    }
}
