#ifndef KV_SORT_LOG_H
#define KV_SORT_LOG_H

#include <vector>
#include <iostream>
#include <algorithm>
#include "params.h"

using namespace std;

class SortLog {

private:
//    u_int64_t * keys = nullptr;
    KVInfo * kvInfoSort = nullptr;
    u_int32_t nums;
    bool sorted = false;

public:

    SortLog() : nums(0) {
//        this->keys = (u_int64_t *) malloc(SORT_LOG_SIZE * sizeof(u_int64_t));
        this->kvInfoSort = (KVInfo *) malloc(SORT_LOG_SIZE * sizeof(KVInfo));
    }

    ~SortLog() {
    }

    int size() {
        return nums;
    }

    void put(u_int64_t &bigEndkey) {
//        keys[nums] = bigEndkey;
        kvInfoSort[nums].key = bigEndkey;
        kvInfoSort[nums].offset = nums;
        nums++;
    }

    static bool cmpKVInfo(KVInfo k1, KVInfo k2) {
        return (k1.key < k2.key || (k1.key == k2.key && k1.offset < k2.offset));
    }

    void quicksort() {
        sorted = true;
        if (nums > 0) {
            printf("sort\n");
            sort(kvInfoSort, kvInfoSort + nums, cmpKVInfo);

            u_int32_t k = 0;
            for (int i = 0; i < nums; i++)
                if (i == nums - 1 || kvInfoSort[i].key != kvInfoSort[i + 1].key) {
                    kvInfoSort[k] = kvInfoSort[i];
                    k++;
                } else {
                    printf("dup\n");
                }
            nums = k;

        }
    }

    int find(u_int64_t &bigEndkey) {
        u_int64_t key = bigEndkey;
        u_int64_t kk = key + 1;

        int left = 0;
        int right = nums;
        int middle;
        while (left < right) {
            middle = left + ((right - left) >> 1);
            if (kvInfoSort[middle].key < kk)
                left = middle + 1;
            else
                right = middle;
        }
        auto res = -1;
        if (left > 0 && kvInfoSort[left - 1].key == key)
            res = kvInfoSort[left - 1].offset;
        return res;
    }

    bool isSorted() {
        return this->sorted;
    }

    u_int64_t getKey(int idx) {
        return kvInfoSort[idx].key;
    }

};


#endif //KV_SORT_LOG_H