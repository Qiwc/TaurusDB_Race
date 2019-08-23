#include "kv_store.h"

KVStore::KVStore() {
//    printf("New KVStore\n");
}

bool KVStore::Init(const char * dir, int id) {
    this->id = id;
    engine.init(dir, id);
    return true;
}

void KVStore::Close() {
//    printf("close kvstore\n");
    engine.close();
//    delete engine;
}

int KVStore::Set(KVString & key, KVString & val) {
    return engine.set(key, val);
}

int KVStore::Get(KVString & key, KVString & val) {
    return engine.get(key, val);
}
