//
// Created by parallels on 8/8/19.
//

#ifndef PROJECT_KV_HASHLOG_H
#define PROJECT_KV_HASHLOG_H
#include <vector>
#include <iostream>
#include <algorithm>
#include <mutex>
#include <condition_variable>

#include "kv_hash.h"
#include "params.h"

using namespace std;

class HashLog {

private:
    KVHash ** kvHash = nullptr;
    int client_ref;
    int hash_finsh;
    std::mutex mutex1;
    std::condition_variable cond;

    HashLog(): client_ref(0), hash_finsh(0) {
        init();
    }

    ~HashLog() {
        if (kvHash != nullptr) {
            for (int i = 0; i < HASH_NUM; i++) {
                delete kvHash[i];
            }
            delete[] kvHash;
        }
    }

public:

    bool isInStep1;//判断是否在校验阶段

    void init() {
        this->kvHash = (KVHash **) malloc(HASH_NUM * sizeof(KVHash *));
        for (int i = 0; i < HASH_NUM; i++) {
            kvHash[i] = new KVHash(HASH_CAPACITY);
        }
    }


    void client_on() {
        std::lock_guard<std::mutex> lock(mutex1);
        if (kvHash == nullptr) {
            init();
        }
        client_ref += 1;
        hash_finsh += 1;

        isInStep1 = true;
        if (client_ref > 10) {
            isInStep1 = false;
        }

    }


    void wait_finish() {
        std::unique_lock <std::mutex> lck(mutex1);
        hash_finsh --;
        if (hash_finsh != 0) {
            while (hash_finsh != 0) cond.wait(lck);
        } else {
            cond.notify_all();
        }
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex1);
        if (--client_ref == 0) {
            for (int i = 0; i < HASH_NUM; i++) {
                delete kvHash[i];
            }
            delete [] kvHash;
            kvHash = nullptr;
        }
    }

    void put(uint64_t &bigEndkey, uint32_t compress_id_pos, uint32_t id) {
//        auto slot = bigEndkey & (HASH_NUM - 1);
//        std::lock_guard<std::mutex> lock(mutex_[slot]);
//        kvHash[slot]->put(bigEndkey, compress_id_pos);
        kvHash[id]->put(bigEndkey, compress_id_pos);
    }

    bool find(uint64_t &bigEndkey, uint32_t &val, uint32_t &id) {
//        auto slot = bigEndkey & (HASH_NUM - 1);
//        return kvHash[slot]->get(bigEndkey, val);
        if (!kvHash[id]->get(bigEndkey, val)) {
            for (uint32_t i = 0; i < HASH_NUM; i++) {
                if (kvHash[i]->get(bigEndkey, val)) {
                    id = i;
                    return true;
                }
            }
            return false;
        } else {
            return true;
        }
    }

    static HashLog & getInstance() {
        static HashLog hashLog;
        return hashLog;
    }

};
#endif //PROJECT_KV_HASHLOG_H
