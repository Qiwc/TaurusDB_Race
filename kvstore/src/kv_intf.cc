#include "kv_intf.h"
#include "kv_store.h"

std::shared_ptr<KVIntf> GetKVIntf() {
    return std::make_shared<KVStore>();
}


