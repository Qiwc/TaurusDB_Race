#ifndef KVSTORE_KV_HASHLOG_H
#define KVSTORE_KV_HASHLOG_H
#include <vector>
#include <iostream>
#include <algorithm>
#include "kv_hash.h"
#include "params.h"

using namespace std;

class HashLog {

private:
    KVHash * kvHash = nullptr;
    u_int32_t nums;

public:

    HashLog() : nums(0) {
        this->kvHash = new KVHash(HASH_CAPACITY);
    }

    ~HashLog() {
        delete kvHash;
    }

    int size() {
        return nums;
    }

    void put(u_int64_t &bigEndkey) {
        kvHash->put(bigEndkey, nums);
        nums++;
    }

    int find(u_int64_t &bigEndkey) {
        return kvHash->getOrDefault(bigEndkey, -1);
    }

};
#endif //KVSTORE_KV_HASHLOG_H
