#pragma once

#include "kv_string.h"
#include "kv_intf.h"

class SimpleCase {
public:
    SimpleCase() {};

    virtual ~SimpleCase() {};

    bool Init(const KVString & path);

    void Uninit();

    double Run(const char * dir, int times, int &err);

protected:
    void * LoadSo(const char * path, const char * entry);

    double Write(std::shared_ptr<KVIntf> stor, int times, int & err);

    double Read(std::shared_ptr<KVIntf> stor, int times, int & err);

private:
    void *so_ = nullptr;

    std::shared_ptr<KVIntf> (* entry_)() = nullptr;

    const char * entry_name_ = "GetKVIntf";
};
