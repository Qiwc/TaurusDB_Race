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
#include "kv_string.h"
#include "params.h"

using namespace std;
using namespace std::chrono;

class KVEngine {
public:
    void close() {
//        printf("Close KVEngine!\n");
        delete kvFile;
        kvFile = nullptr;
        delete kvLog;
        kvLog = nullptr;
    }

    bool init(const char * dir, int id) {
        printf("Engine init %s, %d\n", dir, id);
        string path = dir;
        std::ostringstream ss;
        ss << path << "/value-" << id;
        string filePath = ss.str();
        kvFile = new KVFile(path, id, true, VALUE_LOG_SIZE, KEY_LOG_SIZE, BLOCK_SIZE);
        kvLog = new KVLog(kvFile->getValueFd(), kvFile->getBlockBuffer(), kvFile->getKeyBuffer());
        return true;
    }

    void putKV(char * key, char * val) {
        kvLog->putValueKey(val, key);
    }

    bool getV(char * val, int offset) {
        return kvLog->preadValue((size_t)offset, val);
    }

    bool get_v_one(char * val, int offset) {
        return kvLog->preadValueOne((size_t)offset, val);
    }

    void resetKeyPosition() {
        kvLog->resetKeyPosition();
    }

    char * getK() {
        return kvLog->getKey();
    }

    void recoverKeyPosition(int sum) {
        kvLog->recover((size_t)sum);
    }


private:

    KVFile * kvFile = nullptr;
    KVLog * kvLog = nullptr;

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }
    milliseconds start;
    int setTimes, getTimes;
    long sTime, gTime;

};
#endif
