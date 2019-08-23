#include "tcp_server.h"
#include <unistd.h>
#include <thread>
#include <memory>
#include <chrono>
#include <condition_variable>

#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

//#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())


TcpServer::TcpServer() {
}

TcpServer::~TcpServer() {
    stopAll();
}

void TcpServer::StopAll() {
    printf("STOP ALL...\n");
    getInst().stopAll();
}

void TcpServer::stopAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & fd : fds_) {
        //        nn_close(fd);
    }
    fds_.clear();
}

TcpServer & TcpServer::getInst() {
    static TcpServer server;
    return server;
}



int TcpServer::start(const char * host, int port) {

    struct sockaddr_in servaddr;
    int ssfd;

    memset(&servaddr, 0, sizeof(servaddr));
    if( (ssfd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ){
        printf("create ssfd error\n");
    }

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(host);
    servaddr.sin_port = htons(port);

    if(bind(ssfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1){
        printf("bind socket error\n");
    }
    if(listen(ssfd, 100) == -1){
        printf("listen socket error\n");
    }

    return ssfd;
}

int TcpServer::Run(int ssfd, std::shared_ptr<RpcProcess> rpc_process) {

    if (ssfd != -1) {
        // 启动一个线程监听
        std::thread recv(&TcpServer::processRecv, ssfd, rpc_process);
        recv.detach();
    }

    return ssfd;
}

void TcpServer::processRecv(int ssfd, std::shared_ptr<RpcProcess> process) {
    if (ssfd == -1 || process == nullptr) {
        return ;
    }

    int sfd;
    while(1) {

        printf("======waiting for client's request======\n");

        //阻塞直到有客户端连接，不然多浪费CPU资源。
        if ((sfd = accept(ssfd, (struct sockaddr *) NULL,  NULL)) == -1) {
            printf("accept socket error\n");
            break;
        } else {
            printf("accept socket successful\n");
        }
        
        int on = 1;
        setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&on, sizeof(on));

        // 接收缓冲区
        int nRecvBuf= 1 * 1024 * 1024;//设置为1M
        setsockopt(sfd,SOL_SOCKET,SO_RCVBUF,(const char*)&nRecvBuf,sizeof(int));
        //发送缓冲区
        int nSendBuf= 20 * 1024 * 1024;
        setsockopt(sfd,SOL_SOCKET,SO_SNDBUF,(const char*)&nSendBuf,sizeof(int));

        char * recv_buf = new char[MAX_PACKET_SIZE];
        char * send_buf = static_cast<char *> (memalign((size_t) getpagesize(), MAX_PACKET_SIZE));

        while (1) {
            int rc = recvPack(sfd, recv_buf);
            if (rc < 0) {
                break;
            }

            process->Insert(sfd, (Packet *) recv_buf, send_buf);
        }

    }

    printf("thread close\n");
}

int TcpServer::recvPack(int fd, char * buf) {
    auto bytes = 0;

    while (bytes < sizeof(int)) {
        auto b = recv(fd, buf + bytes, sizeof(int) - bytes, 0);
        if (b <= 0) {
            return -1;
        }
        bytes += b;
    }
    int total = *(int *) buf;
    while (total != bytes) {
        auto b = recv(fd, buf + bytes, total - bytes, 0);
        if (b <= 0) {
            return -1;
        }
        bytes += b;
    }
    return total;
}
