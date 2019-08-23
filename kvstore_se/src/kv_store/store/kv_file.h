#ifndef KV_FILE_H
#define KV_FILE_H

#include <stdint.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include <limits.h>
#include <stdlib.h>

class KVFile {
private:
    //value文件 dio
    int valueFd;
    size_t valueFileSize;
    //key文件 mmap
    int mapKeyFd;
    u_int64_t * keyBuffer;
    size_t keyFileSize;
    //写入缓存文件 mmap
    int mapBlockFd;
    char * blockBuffer;
    size_t blockFileSize;

public:
    KVFile(const std::string &path, const int &id, const bool &dio,
           const size_t &valueFileSize, const size_t &keyFileSize,
           const size_t &blockFileSize) :
            valueFileSize(valueFileSize), keyFileSize(keyFileSize), blockFileSize(blockFileSize){

        std::ostringstream fp;
        fp << path << "/value-" << id;

        //valuelog可以用dio或者缓存io
        if (dio) {
            this->valueFd = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
        } else {
            this->valueFd = open(fp.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
        }
        fallocate(this->valueFd, 0, 0, valueFileSize);
        ftruncate(this->valueFd, valueFileSize);

        //key文件
        std::ostringstream mpKey;
        mpKey << path << "/mpKey-" << id;
        this->mapKeyFd = open(mpKey.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
        fallocate(this->mapKeyFd, 0, 0, keyFileSize);
        ftruncate(this->mapKeyFd, keyFileSize);
        this->keyBuffer = static_cast<u_int64_t *>(mmap(nullptr, keyFileSize, PROT_READ | PROT_WRITE,
                                                        MAP_SHARED | MAP_POPULATE | MAP_NONBLOCK, this->mapKeyFd,
                                                        0));
        //写入缓存文件
        std::ostringstream mpBlock;
        mpBlock << path << "/mpBlock-" << id;
        this->mapBlockFd = open(mpBlock.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
        fallocate(this->mapBlockFd, 0, 0, blockFileSize);
        ftruncate(this->mapBlockFd, blockFileSize);
        this->blockBuffer = static_cast<char *>(mmap(nullptr, blockFileSize, PROT_READ | PROT_WRITE,
                                                     MAP_SHARED | MAP_POPULATE | MAP_NONBLOCK, this->mapBlockFd,
                                                     0));
        printf("Finish create file\n");
    }

    ~KVFile() {
        munmap(blockBuffer, this->blockFileSize);
        munmap(keyBuffer, this->keyFileSize);
        close(this->mapBlockFd);
        close(this->mapKeyFd);
        close(this->valueFd);
    }


    int getValueFd() const {
        return valueFd;
    }

    u_int64_t *getKeyBuffer() const {
        return keyBuffer;
    }

    char *getBlockBuffer() const {
        return blockBuffer;
    }
};

#endif //KV_FILE_H