#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>
#include "rpc_process.h"
#include "utils.h"

class TcpServer {
public:
    TcpServer();
    ~TcpServer();

    static int Run(int port, std::shared_ptr<RpcProcess> process);

    static void StopAll();

    static int start(const char * host, int port);

protected:

    static TcpServer & getInst();

    void stopAll();

    static int recvPack(int fd, char * buf);

    static void processRecv(int sfd, std::shared_ptr<RpcProcess> process);

protected:
    std::mutex mutex_;

    std::vector<int> fds_;
};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

