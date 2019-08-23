#ifndef KV_STORE_H
#define KV_STORE_H

#include "kv_string.h"
#include "kv_intf.h"
#include "kv_engine.h"

class KVStore : public KVIntf {
public:
    KVStore();

    bool Init(const char * dir, int id);

    void Close();

    int Set(KVString &key, KVString & val);

    int Get(KVString &key, KVString & val);

private:
    KVEngine engine;
    int setTimes = 0;
    int getTimes = 0;
    int id;
};

#endif

