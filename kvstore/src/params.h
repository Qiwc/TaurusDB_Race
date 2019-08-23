#ifndef PARAMS_H
#define PARAMS_H

#include <string>


const int NUM_PER_SLOT = 4 * 1024 * 1024;

const int KEY_SIZE = 8;
const size_t VALUE_SIZE = 4096;
const size_t VALUE_LOG_SIZE = NUM_PER_SLOT * VALUE_SIZE;
const size_t KEY_LOG_SIZE = NUM_PER_SLOT * KEY_SIZE;

//读缓存大小（多少块4KB）
const int READ_CACHE_SIZE = 4096 * 4;
//写缓存大小（多少块4KB）
const int PAGE_PER_BLOCK = 256;
//写缓存大小
const size_t BLOCK_SIZE = PAGE_PER_BLOCK * VALUE_SIZE;

//sortlog的容量
const int SORT_LOG_SIZE = NUM_PER_SLOT;
//hash的容量
const int HASH_CAPACITY = 2 * NUM_PER_SLOT;

struct KVInfo {
    u_int64_t key;
    u_int32_t offset;
};

#endif //PARAMS_H
