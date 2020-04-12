package ru.golchin.key_value_store;

import java.util.*;

public abstract class AbstractTest {
    private final Random random = new Random(42L);

    public List<Map<String, String>> generateKeysAndValues(int size, int nKeys, int length) {
        var result = new ArrayList<Map<String, String>>();
        for (int j = 0; j < size; j++) {
            var map = new HashMap<String, String>();
            for (int i = 0; i < nKeys; i++) {
                String key = String.valueOf(random.nextInt(nKeys));
                String value = generate(length);
                map.put(key, value);
            }
            result.add(map);
        }
        return result;
    }

    public String generate(int length) {
        if (random.nextInt(100) == 0) {
            return null;
        }
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) random.nextInt(Character.MAX_VALUE);
        }
        return new String(chars);
    }
}
