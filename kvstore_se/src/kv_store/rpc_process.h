#ifndef __HUAWEI_RPC_SERVICE_H__
#define __HUAWEI_RPC_SERVICE_H__
/////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <list>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <functional>
#include "kv_string.h"
#include "store/kv_engines.h"
#include "utils.h"
#include "params.h"
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <unistd.h>
#include <thread>
#include <dirent.h>

typedef std::function<void (const char *, int)> DoneCbFunc;

class RpcProcess : public std::enable_shared_from_this<RpcProcess> {
public:
    RpcProcess(){
    }

    ~RpcProcess() {
        Stop();
    }

    std::shared_ptr<RpcProcess> GetPtr() {
        return shared_from_this();
    }

    bool Insert(int sfd, Packet * recv_buf, char * send_buf) {
        switch(recv_buf->type) {
            case KV_OP_PUT_KV:
                processPutKV(sfd, recv_buf, send_buf);
                break;

            case KV_OP_GET_V_BATCH:
                processGetV(sfd, recv_buf, send_buf);
                break;

            case KV_OP_GET_ONE_V:
                processGetVOne(sfd, recv_buf, send_buf);
                break;

            case KV_OP_RESET_K:
                processResetKeyPosition(sfd, recv_buf, send_buf);
                break;

            case KV_OP_GET_K:
                processGetK(sfd, recv_buf, send_buf);
                break;

            case KV_OP_RECOVER:
                processRecoverKeyPosition(sfd, recv_buf, send_buf);
                break;

            default:
                LOG(ERROR) << "unknown rpc type: " << recv_buf->type;
                break;
        }
        return true;
    }

    void processPutKV(int sfd, Packet * buf, char * send_buf) {
        int threadId = *((uint32_t *)buf->buf);
        kv_engines.putKV(buf->buf + sizeof(uint32_t), buf->buf + sizeof(uint32_t) + KEY_SIZE, threadId);
        send(sfd, "1", 1, 0);
    }

    void processGetVOne(int sfd, Packet * buf, char * send_buf) {
        uint32_t compress = *(uint32_t *)buf->buf;
        int threadId = compress >> 28;
        int offset = compress & 0x0FFFFFFF;
        kv_engines.get_v_one(send_buf, offset, threadId);
        send(sfd, send_buf, VALUE_SIZE, 0);
    }

    void processGetV(int sfd, Packet * buf, char * send_buf) {
        uint32_t compress = *(uint32_t *)buf->buf;
        int threadId = compress >> 28;
        uint32_t offset = compress & 0x0FFFFFFF;

        for (int i = 0; i < CLIENT_READ_CACHE_SIZE; i++) {
            if (kv_engines.getV(send_buf, offset + i, threadId)) {
                // end file
                break;
            }
            send(sfd, send_buf, VALUE_SIZE, 0);

            if (i > 0)
                for (int j = 0; j < SPIN_PERIOD; j++){}
        }
    }

    void processResetKeyPosition(int sfd, Packet * buf, char * send_buf) {
        int threadId = *((uint32_t *)buf->buf);
        kv_engines.resetKeyPosition(threadId);
        send(sfd, "1", 1, 0);
    }

    void processGetK(int sfd, Packet * buf, char * send_buf) {
        int threadId = *((uint32_t *)buf->buf);
        auto & tmp = * (Packet *) send_buf;
        char * key_buf = kv_engines.getK(threadId);
        send(sfd, key_buf, KEY_NUM_TCP * KEY_SIZE, 0);
    }

    void processRecoverKeyPosition(int sfd, Packet * buf, char * send_buf) {
        int threadId = *((uint32_t *)buf->buf);
        auto sum = *(uint32_t *)(buf->buf + sizeof(uint32_t));
        kv_engines.recoverKeyPosition(sum, threadId);
        send(sfd, "1", 1, 0);
    }

    void Getfilepath(const char *path, const char *filename,  char *filepath)
    {
        strcpy(filepath, path);
        if(filepath[strlen(path) - 1] != '/')
            strcat(filepath, "/");
        strcat(filepath, filename);
    }

    bool DeleteFile(const char* path)
    {
        DIR *dir;
        struct dirent *dirinfo;
        struct stat statbuf;
        char filepath[256] = {0};
        lstat(path, &statbuf);

        if (S_ISREG(statbuf.st_mode))//判断是否是常规文件
        {
            remove(path);
        }
        else if (S_ISDIR(statbuf.st_mode))//判断是否是目录
        {
            if ((dir = opendir(path)) == NULL)
                return 1;
            while ((dirinfo = readdir(dir)) != NULL)
            {
                Getfilepath(path, dirinfo->d_name, filepath);
                if (strcmp(dirinfo->d_name, ".") == 0 || strcmp(dirinfo->d_name, "..") == 0)//判断是否是特殊目录
                    continue;
                DeleteFile(filepath);
                rmdir(filepath);
            }
            closedir(dir);
        }
        return 0;
    }


    bool Run(const char * dir, bool clear) {
        if (clear) {
            DeleteFile(dir);
        }
        if (access(dir, 0) == -1) {
            mkdir(dir, 0777);
        }
        kv_engines.Init(dir);
    }

    void Stop() {
        kv_engines.Close();
        sleep(1);
    }




protected:

    KVEngines   kv_engines;

};
/////////////////////////////////////////////////////////////////////////////////////////////
#endif
