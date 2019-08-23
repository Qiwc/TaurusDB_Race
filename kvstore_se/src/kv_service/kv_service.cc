#include "kv_service.h"
#include "utils.h"

bool KVService::Init(const char * host, int id) {
    kvClient.clientInit(host, id);
    return true;
}

void KVService::Close() {
    kvClient.clientClose();
}

int KVService::Set(KVString & key, KVString & val) {
    return kvClient.set(key, val);
}

int KVService::Get(KVString & key, KVString & val) {
    return kvClient.get(key, val);
}
