package utils.application;

import java.util.Arrays;
import java.util.Base64;

public record Hash(byte[] hash) {

    public static Hash fromPersistenceString(String persistenceString) {
        return new Hash(Base64.getDecoder().decode(persistenceString));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Hash(byte[] hash1))) return false;

        return Arrays.equals(this.hash, hash1);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(hash);
    }

    public String getPersistenceString() {
        return Base64.getEncoder().encodeToString(hash);
    }
}
