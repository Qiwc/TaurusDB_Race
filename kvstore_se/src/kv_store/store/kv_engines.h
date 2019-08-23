#ifndef KV_ENGINES_H
#define KV_ENGINES_H

#include "kv_string.h"
#include "kv_engine.h"

class KVEngines{
public:

    bool Init(const char * dir) {
        this->engine = new KVEngine[THREAD_NUM];
        printf("THREAD NUM : %d\n", THREAD_NUM);
        for (int id = 0; id < THREAD_NUM ; ++id) {
            engine[id].init(dir, id);
        }
        return true;
    }
    
    void Close() {
        for (int id = 0; id < THREAD_NUM ; ++id) {
            engine[id].close();
        }
    }

    void putKV(char* key, char * val, int threadId) {
        engine[threadId].putKV(key, val);
    }

    bool getV(char * val, uint32_t offset, int threadId) {
        return engine[threadId].getV(val, offset);
    }


    bool get_v_one(char * val, uint32_t offset, int threadId) {
        return engine[threadId].get_v_one(val, offset);
    }

    char * getK(int threadId) {
        return engine[threadId].getK();
    }

    void resetKeyPosition(int threadId) {
        return engine[threadId].resetKeyPosition();
    }

    void recoverKeyPosition(int sum, int threadId) {
        return engine[threadId].recoverKeyPosition(sum);
    }



private:
    KVEngine* engine;
};

#endif

