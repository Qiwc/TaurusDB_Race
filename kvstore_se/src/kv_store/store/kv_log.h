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
#include <malloc.h>
#include <limits.h>
#include "kv_string.h"
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>

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

    // 顺序读 buffer
    char *value_buf_seq = nullptr;
    uint32_t value_buf_seq_start_index;

public:
    KVLog(const int &fd, char *cacheBuffer, u_int64_t *keyBuffer) :
            fd(fd), filePosition(0),
            keyBuffer(keyBuffer), keyBufferPosition(0),
            cacheBuffer(cacheBuffer), cacheBufferPosition(0) {
    }

    ~KVLog() {
        if (value_buf_seq != nullptr)
            delete[] value_buf_seq;
    }


    void putValueKey(const char *value, const char *key) {
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

    bool preadValue(uint32_t index, char *value) {

        // mmap 里面有东西
        if (cacheBufferPosition > 0) {
            pwrite(this->fd, cacheBuffer, cacheBufferPosition * VALUE_SIZE, filePosition);
            filePosition += cacheBufferPosition * VALUE_SIZE;
            cacheBufferPosition = 0;
        }

        if (index * VALUE_SIZE >= this->filePosition)
            return true;

        if (value_buf_seq == nullptr) {
            value_buf_seq = static_cast<char *> (memalign((size_t) getpagesize(), VALUE_SIZE * SERVER_READ_CACHE_SIZE));
            value_buf_seq_start_index = UINT32_MAX;
        }

        uint32_t current_buf_no = index / SERVER_READ_CACHE_SIZE;

        if (current_buf_no != value_buf_seq_start_index / SERVER_READ_CACHE_SIZE) {
            value_buf_seq_start_index = current_buf_no * SERVER_READ_CACHE_SIZE;
            pread(this->fd, value_buf_seq, VALUE_SIZE * SERVER_READ_CACHE_SIZE,
                  (value_buf_seq_start_index * VALUE_SIZE));
        }

        uint32_t value_buf_offset = index % SERVER_READ_CACHE_SIZE;
        memcpy(value, value_buf_seq + value_buf_offset * VALUE_SIZE, VALUE_SIZE);
//        value = static_cast<char *>(value_buf_seq + value_buf_offset * VALUE_SIZE);

        return false;
    }

    bool preadValueOne(uint32_t index, char *value) {

        if (index * VALUE_SIZE > this->filePosition + cacheBufferPosition * VALUE_SIZE) {
            return true;
        }

        //如果要读的value在mmap中
        if (this->filePosition <= index * VALUE_SIZE) {
            auto pos = index % PAGE_PER_BLOCK;
            memcpy(value, cacheBuffer + (pos * VALUE_SIZE), VALUE_SIZE);
            return false;
        }
        pread(this->fd, value, VALUE_SIZE, (index * VALUE_SIZE));
        return false;
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

    char *getKey() {
        char *key = (char *) (keyBuffer + keyBufferPosition);
        keyBufferPosition += KEY_NUM_TCP;
        return key;
    }

    inline void resetKeyPosition() {
        keyBufferPosition = 0;
    }
};


#endif //KV_LOG_H
