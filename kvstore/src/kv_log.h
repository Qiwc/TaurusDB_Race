#ifndef KV_LOG_H
#define KV_LOG_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include "params.h"
#include <limits.h>

class KVLog {
private:
    //对应ValueFile
    int fd;
    size_t filePosition;

    //对应KeyFile
    u_int64_t *keyBuffer;
    size_t keyBufferPosition;
    char *cacheBuffer;
    size_t cacheBufferPosition;

    //当前读缓存块是从第几个开始
    int readCacheStartNo;

public:
    KVLog(const int &fd, char *cacheBuffer, u_int64_t *keyBuffer) :
            fd(fd), filePosition(0),
            keyBuffer(keyBuffer), keyBufferPosition(0),
            cacheBuffer(cacheBuffer), cacheBufferPosition(0),
            readCacheStartNo(INT_MIN){
    }

    ~KVLog() {
    }

    size_t size() {
        return filePosition + (cacheBufferPosition * VALUE_SIZE);
    }

    inline void putValueKey(const char *value, const char * key) {
        //写入key
        *(keyBuffer + keyBufferPosition) = *((u_int64_t *) key);
        keyBufferPosition++;
        //写入写缓存块
        memcpy(cacheBuffer + (cacheBufferPosition * VALUE_SIZE), value, VALUE_SIZE);
        cacheBufferPosition++;
        //value缓存达到缓存块大小就刷盘
        if (cacheBufferPosition == PAGE_PER_BLOCK) {
            pwrite(this->fd, cacheBuffer, BLOCK_SIZE, filePosition);
            filePosition += BLOCK_SIZE;
            cacheBufferPosition = 0;
        }
    }

    //返回值是要读的值是读缓存块的第几个
    inline int readValue(int index, char *value) {
        //如果要读的value在mmap中
        if (this->filePosition <= index * VALUE_SIZE) {
            auto pos = index % PAGE_PER_BLOCK;
            memcpy(value, cacheBuffer + (pos * VALUE_SIZE), VALUE_SIZE);
            readCacheStartNo = INT_MIN;
            return 0;
        }
        //这里就是读valueLog
        else
            {
            //如果当前要读的命中读缓存块
            int now = index / READ_CACHE_SIZE;
            if (now == readCacheStartNo / READ_CACHE_SIZE) {
                return index % READ_CACHE_SIZE;
            }
            //如果没命中
            else{
                readCacheStartNo = now * READ_CACHE_SIZE;
                size_t cap = VALUE_SIZE * READ_CACHE_SIZE;
                if (keyBufferPosition - readCacheStartNo < READ_CACHE_SIZE) {
                    cap = (keyBufferPosition - readCacheStartNo) * VALUE_SIZE;
                }
                pread(this->fd, value, cap, (readCacheStartNo * VALUE_SIZE));
                return index % READ_CACHE_SIZE;
            }
        }
    }

    inline void readValue(size_t offset, char *value, size_t size) {
        pread(this->fd, value, size, offset);
    }

    //再次open时恢复写的位置
    void recover(size_t sum) {
        this->keyBufferPosition = sum;
        auto cacheSize = sum % PAGE_PER_BLOCK;
        if (cacheSize == 0) {
            this->filePosition = sum * VALUE_SIZE;
        } else {
            this->filePosition = (sum - cacheSize) * VALUE_SIZE;
            this->cacheBufferPosition = cacheSize;
        }
    }

    inline bool getKey(u_int64_t & key) {
        key = *(keyBuffer + keyBufferPosition);
        keyBufferPosition++;
        return key != 0 || (key == 0 && *(keyBuffer + keyBufferPosition) != 0);
    }

    inline void resetKeyPosition() {
        keyBufferPosition = 0;
    }
};


#endif //KV_LOG_H
