#ifndef KV_ENGINE_H
#define KV_ENGINE_H

#include <memory>
#include <malloc.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <atomic>

#include "kv_file.h"
#include "kv_log.h"
#include "kv_sortlog.h"
#include "kv_hashlog.h"
#include "kv_string.h"
#include "params.h"

using namespace std;


class KVEngine {
public:
    void close() {
//        printf("Close KVEngine!\n");
        delete kvFile;
        kvFile = nullptr;
        delete kvLog;
        kvLog = nullptr;

        if (hashLog != nullptr) {
//            printf("delete hashlog\n");
            delete hashLog;
            hashLog = nullptr;
        }
    }

    bool init(const char * dir, int id) {
//        printf("Engine init %s, %d\n", dir, id);
        string path = dir;
        std::ostringstream ss;
        ss << path << "/value-" << id;
        string filePath = ss.str();
        kvFile = new KVFile(path, id, true, VALUE_LOG_SIZE, KEY_LOG_SIZE, BLOCK_SIZE);
        kvLog = new KVLog(kvFile->getValueFd(), kvFile->getBlockBuffer(), kvFile->getKeyBuffer());
        return true;
    }

    int set(KVString &key, KVString & val) {
//        if (kvLog == nullptr) {
//            printf("[SET] KeyLog is null!!!\n");
//            return 0;
//        }
        if (hashLog == nullptr) {
            recoverIndex();
        }
        kvLog->putValueKey(val.Buf(), key.Buf());
        hashLog->put(*((u_int64_t *) key.Buf()));
        return 1;
    }

    int get(KVString &key, KVString & val) {
//        if (kvLog == nullptr) {
//            printf("[GET] KeyLog is null!!!\n");
//            return 0;
//        }
        if (hashLog == nullptr) {
            recoverIndex();
        }
        int idx = hashLog->find(*((u_int64_t *) key.Buf()));
        if (idx == -1) {
            printf("Not found!\n");
            return 0;
        } else {
            char * buffer = readBuffer.get();
            int offset = kvLog->readValue(idx, buffer);
            val = readVal.put(buffer + offset * VALUE_SIZE);
//            val = KVString(buffer + offset * VALUE_SIZE, VALUE_SIZE);
            return 1;
        }
    }

private:
    void recoverIndex() {
        if (hashLog == nullptr) {
            hashLog = new HashLog();
            u_int64_t k;
            kvLog->resetKeyPosition();
            while (kvLog->getKey(k)) {
                hashLog->put(k);
            }
            kvLog->recover((size_t) hashLog->size());
        }
    }

    std::unique_ptr<char> readBuffer =
            unique_ptr<char>(static_cast<char *> (memalign((size_t) getpagesize(), VALUE_SIZE * READ_CACHE_SIZE)));
    KVFile * kvFile = nullptr;
    KVLog * kvLog = nullptr;
    HashLog *hashLog = nullptr;
    KVString readVal = KVString(4096);
};
#endif