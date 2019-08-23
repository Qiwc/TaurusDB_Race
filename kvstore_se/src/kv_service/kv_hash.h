//
// Created by parallels on 8/7/19.
//

#ifndef PROJECT_KV_HASH_H
#define PROJECT_KV_HASH_H

#include <malloc.h>
#include <assert.h>
#include <cmath>
#include <chrono>
#include <vector>
#include <iostream>
#include <algorithm>
#include <mutex>

#include "params.h"

class KVHash {
public:
    explicit KVHash(int capacity) {
        this->keyMixer = initialKeyMixer();
        this->assigned = 0;
        allocateBuffers(capacity);
    };

    ~KVHash() {
        free(keys);
        free(values);
    };

    int size() {
        return assigned;
    }

    int put(const long &key, const uint32_t &value) {
        assert(this->assigned < mask + 1);
        if(key == 0) {
            hasEmptyKey = true;
            int previousValue = values[mask + 1];
            values[mask + 1] = value;
            return previousValue;
        } else {
            int slot = hashKey(key) & mask;
            long existing;
            while ((existing = keys[slot]) != 0) {
                if(existing == key) {
                    int previousValue = values[slot];
                    values[slot] = value;
                    return previousValue;
                }
                slot = (slot + 1) & mask;
            }
            keys[slot] = key;
            values[slot] = value;
            assigned++;
            return 0;
        }

    };

    bool get(const long & key, uint32_t &val) {
        if(key == 0) {
            if (hasEmptyKey) {
                val = values[mask + 1];
                return true;
            } else {
                return false;
            }
        } else {
            int slot = hashKey(key) & mask;
            long exsisting;
            while((exsisting = keys[slot])!=0) {
                if(exsisting == key) {
                    val = values[slot];
                    return true;
                }
                slot = (slot + 1) & mask;
            }
            return false;
        }
    }

private:
    long *keys;
    uint32_t *values;
    int keyMixer;
    int assigned;
    int mask;
    int hasEmptyKey;

    void allocateBuffers(int capacity) {
        int arraySize = capacity;
        int emptyElementSlot = 1;
        this->keys = static_cast<long *>(malloc((arraySize + emptyElementSlot) * sizeof(long)));
        this->values = static_cast<uint32_t *>(malloc((arraySize + emptyElementSlot) * sizeof(uint32_t)));
        this->mask = arraySize - 1;

    }

    int hashKey(const long &key) {
        assert(key != 0);
        return mix(key, this->keyMixer);

    }

    int initialKeyMixer() {
        return static_cast<int>(-1686789945);
    }

    int mix(long key, int seed) {
        return static_cast<int>(mix64(key ^ seed));
    }

    long mix64(long z) {
        z = (z ^ (z >> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >> 29)) * 0xfc12c5b19d3259e9L;
        return z ^ (z >> 32);
    }


    int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }
};

#endif //PROJECT_KV_HASH_H
