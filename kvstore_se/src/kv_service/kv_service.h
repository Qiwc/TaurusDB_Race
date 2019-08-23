#pragma once
#include <memory>
#include <mutex>

#include "kv_string.h"
#include "kv_intf.h"
#include "kv_client.h"

class KVService : public KVIntf, public std::enable_shared_from_this<KVService> {
public:
    KVService() {
    }

    bool Init(const char * host, int id);

    void Close();

    int Set(KVString &key, KVString & val);

    int Get(KVString &key, KVString & val);

    std::shared_ptr<KVService> GetPtr() {
        return shared_from_this();
    }

private:
    std::mutex mutex_;

    int ref_ = 0;

    KVClient kvClient;

    int no_;

};

