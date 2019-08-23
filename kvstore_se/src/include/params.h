#ifndef PARAMS_H
#define PARAMS_H

#include <string>

const size_t THREAD_NUM = 16;
const size_t HASH_NUM = 16;
const size_t NUM_PER_SLOT = 4 * 1024 * 1024;

const size_t KEY_SIZE = 8;
const size_t VALUE_SIZE = 4096;
const size_t VALUE_LOG_SIZE = NUM_PER_SLOT * VALUE_SIZE;
const size_t KEY_LOG_SIZE = NUM_PER_SLOT * KEY_SIZE;
//hash的容量
const size_t HASH_CAPACITY = 2 * NUM_PER_SLOT / HASH_NUM * THREAD_NUM;

//写缓存大小（多少块4KB）
const size_t PAGE_PER_BLOCK = 16;

//写缓存大小
const size_t BLOCK_SIZE = PAGE_PER_BLOCK * VALUE_SIZE;

//读缓存大小（多少块4KB）
const int CLIENT_READ_CACHE_SIZE = 256 * 16;

const int SERVER_READ_CACHE_SIZE = 256 * 16;

const int SPIN_PERIOD = 70;


const size_t KEY_NUM_TCP = 512;//一次最多传输KEY的数量

const size_t PACKET_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
const size_t MAX_PACKET_SIZE = PACKET_HEADER_SIZE + VALUE_SIZE * 2;

#endif //PARAMS_H
