#include "simple_case.h"

#include <dlfcn.h>
#include <chrono>

#include <easylogging++.h>

using namespace std::chrono;

bool SimpleCase::Init(const KVString &path) {
    auto entry = LoadSo(path.Buf(), entry_name_);
    if (entry == nullptr) {
        return false;
    }

    entry_ = reinterpret_cast<std::shared_ptr<KVIntf> (*)()>(entry);

    return true;
}

void SimpleCase::Uninit() {
    if (so_ != NULL) {
        dlclose(so_);
        so_ = NULL;
    }
}

void * SimpleCase::LoadSo(const char * path, const char * entry) {
    Uninit();

    void *dl = dlopen(path, RTLD_NOW | RTLD_DEEPBIND | RTLD_LOCAL);
    if (!dl) {
      LOG(ERROR) << "load " << path << " failed: " << dlerror();
      return nullptr;
    }

    so_ = dl;
    auto f_entry = dlsym(dl, entry);
    if (NULL == f_entry) {
      LOG(ERROR) << "find symbol " << entry << " failed: " << dlerror();
      return nullptr;
    }

    return f_entry;
}

double SimpleCase::Run(const char * dir, int times, int &err) {
    if (entry_ == nullptr) {
        LOG(ERROR) << "Entry function in user so is nullptr, call Init() first";
        return -1.0;
    }

    auto stor = entry_();
    if (stor == nullptr) {
        LOG(ERROR) << "Get kv_store shared_ptr failed, check \"GetKVIntf\" in user SO first";
        return -1.0;
    }

    stor->Init(dir, 0);

    double write_time = Write(stor, 127, err);

    stor->Close();
    stor->Init(dir, 0);
    double read_time = Read(stor, 127, err);
    stor->Close();

//    stor->Init(dir, 0);
//    write_time += Write(stor, times, err);
//    stor->Close();
//    stor->Init(dir, 0);
//    read_time += Read(stor, times, err);
//    stor->Close();
    return write_time + read_time;
}

static auto buildKey = [](int64_t idx)->KVString {
    return KVString((const char *)&idx, sizeof(idx));
};

static auto buildVal = [](int idx)->KVString {
    char buf[4096];
    char ch = (char)(idx % 10 + '0');
    for (int i = 0; i < 4096; i++) buf[i] = ch;
    return KVString(buf, 4096);
};

double SimpleCase::Write(std::shared_ptr<KVIntf> stor, int times, int & err) {
    if (stor == nullptr) {
        return -1.0;
    }
    auto begin = system_clock::now();
    /////////////////////////////////////////////////////////////////////////////
    for (int i = 0; i < times; i ++) {
        auto key = buildKey(i);
        auto val = buildVal(i);
        stor->Set(key, val);
    }

    KVString val;
    for (int i = 0; i < times; i ++) {
        auto key = buildKey(i);
        stor->Get(key, val);
        auto build = buildVal(i);
        if (!(val == build) ) {
            err ++;
            LOG(ERROR) << "get key " << i << " error, val: " << val.Size();
            break;
        }
    }
    /////////////////////////////////////////////////////////////////////////////
    return duration_cast<duration<double>>(system_clock::now() - begin).count();
}

double SimpleCase::Read(std::shared_ptr<KVIntf> stor, int times, int & err) {
    auto begin = std::chrono::system_clock::now();
    KVString val;
    for (int i = 0; i < times; i ++) {
        auto key = buildKey(i);
        stor->Get(key, val);
        auto build = buildVal(i);
        if (!(val == build) ) {
            printf("A: %c\n", val.Buf()[0]);
            printf("B: %c\n", build.Buf()[0]);
            err ++;
            LOG(ERROR) << "get key " << i << " error, val: " << val.Size();
            break;
        }
    }

    return duration_cast<duration<double>>(system_clock::now() - begin).count();
}

