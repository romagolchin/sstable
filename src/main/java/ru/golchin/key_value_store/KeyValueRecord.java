package ru.golchin.key_value_store;

import java.util.Objects;

class KeyValueRecord {
    private final String key;
    private final String value;

    public KeyValueRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueRecord record = (KeyValueRecord) o;
        return Objects.equals(key, record.key) &&
                Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KeyValueRecord{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
