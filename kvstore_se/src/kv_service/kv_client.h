#ifndef PROJECT_KV_CLIENT_H
#define PROJECT_KV_CLIENT_H

#include <memory>
#include <malloc.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <atomic>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#include "kv_hashlog.h"
#include "kv_string.h"
#include "params.h"
#include "utils.h"

using namespace std::chrono;

class KVClient {
public:

    void clientClose() {
        printf("Client close start %d\n", id);
        close(fd);
        delete [] sendBuf;
        delete [] recvBuf;
        delete [] value_buf_head;
        delete [] value_buf_tail;
        HashLog::getInstance().close();
        printf("Client close over %d\n", id);
    }

    bool clientInit(const char *host, int id) {

        printf("Client init start %s, %d\n", host, id);

        this->id = (uint32_t)id;
        nums = 0;
        recoverFlag = false;
        isInStep2 = false;
        isInStep3 = false;

        sendBuf = new char[MAX_PACKET_SIZE];
        recvBuf = new char[MAX_PACKET_SIZE];

        value_buf_head = new char[CLIENT_READ_CACHE_SIZE * VALUE_SIZE];
        value_buf_head_start_index = UINT32_MAX;
        value_buf_tail = new char[CLIENT_READ_CACHE_SIZE * VALUE_SIZE];
        value_buf_tail_start_index = UINT32_MAX;

        random_pre_read = false;

        setTimes = 0;
        getTimes = 0;

        // connect to storage
        auto port = 9500;
        if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            printf("socket error\n");
            exit(-1);
        }
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        server_addr.sin_addr.s_addr = inet_addr(host + 6);
        bzero(&(server_addr.sin_zero), sizeof(server_addr.sin_zero));

        while (connect(fd, (struct sockaddr *) &server_addr, sizeof(struct sockaddr_in)) == -1) {
            sleep(1);
        }

        int on = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void *) &on, sizeof(on));
        // 接收缓冲区
        int nRecvBuf = 32 * 1024 * 1024;//设置为16M
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (const char *) &nRecvBuf, sizeof(int));
        //发送缓冲区
        int nSendBuf = 1 * 1024 * 1024;//设置为1M
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (const char *) &nSendBuf, sizeof(int));

        KV_LOG(INFO) << "connect to store node success. fd: " << fd;

        HashLog::getInstance().client_on();

        this->start = now();

        printf("Client init over %s, %d\n", host, id);
        return true;
    }

    void recoverIndex() {
        printf("start recover index: %d.\n", id);
        reset();
        while (getKey(nums)) {
        }
        printf("recover threadId %d. time spent is %lims\n", id, (now() - start).count());
        printf("======key num: %d\n", nums);
        recover(nums);
        HashLog::getInstance().wait_finish();
    }

    int set(KVString &key, KVString &val) {
        if (!recoverFlag) {
            recoverFlag = true;
            if (!HashLog::getInstance().isInStep1) {
                isInStep2 = true;
                isInStep3 = false;
                printf("================== step 2 ==================\n");
            }
            recoverIndex();
        }
        //print
        if (setTimes == 0) {
            this->start = now();
        }


        sendKV(key, val);
        HashLog::getInstance().put(*((uint64_t *) key.Buf()), (id << 28) + nums, id);
        nums++;

        //print
        if (setTimes % 100000 == 0 && setTimes > 0) {
//            printf("ID : %d,  Set : %lu\n", id, *((uint64_t *) key.Buf()));
            printf("client ID : %d. write %d. total time spent is %lims. average time spent is %lims\n", id, setTimes, (now() - start).count(), (((now() - start).count()) / setTimes));
        }
        setTimes++;

        return 1;
    }

    int get(KVString &key, KVString &val) {
        if (!recoverFlag) {
            recoverFlag = true;
            if (!HashLog::getInstance().isInStep1) {
                isInStep2 = false;
                isInStep3 = true;
                printf("================== step 3 ==================\n");
            }
            recoverIndex();
        }

        //print
        if (getTimes == 0) {
            this->start = now();
        }


        uint32_t pos;
        while (!HashLog::getInstance().find(*((uint64_t *) key.Buf()), pos, id)) {
            sleep(5);
            printf("wait to find pos from hash\n");
        }

        //ToDo
        if (!isInStep3 && !isInStep2) {
            get_value_one(pos, val); //校验阶段采用
        } else if (isInStep3) {
            get_value_from_buffer(pos, val); //第三阶段采用
        } else if (isInStep2){
            get_value(pos, val); //第二阶段采用
        }

        //print
        if (getTimes % 100000 == 0 && getTimes > 0) {
//            printf("ID : %d,  Get : %lu,  threadId : %d, pos : %d\n",
//                   id, *((uint64_t *) key.Buf()), pos >> 28, pos & 0x0FFFFFFF);
            printf("client ID : %d. read %d. total time spent is %lims. average time spent is %lims\n", id, getTimes, (now() - start).count(), (((now() - start).count()) / getTimes));
        }
        getTimes++;

        return 1;
    }

private:

    uint32_t id;
    bool recoverFlag;
    bool isInStep2;//判断是否在顺序写+顺序读阶段
    bool isInStep3;//判断是否在随机读阶段

    char *sendBuf;
    char *recvBuf;

    // 随机读 buffer
    char *value_buf_head = nullptr;
    uint32_t value_buf_head_start_index;
    char *value_buf_tail = nullptr;
    uint32_t value_buf_tail_start_index;

    bool random_pre_read = false;


    int fd;
    struct sockaddr_in server_addr;
    uint32_t nums;

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }
    milliseconds start;
    int setTimes, getTimes;

    void sendKV(KVString &key, KVString &val) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + sizeof(uint32_t)+ KEY_SIZE + VALUE_SIZE;
        send_pkt.type = KV_OP_PUT_KV;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        memcpy(send_pkt.buf + sizeof(uint32_t), key.Buf(), KEY_SIZE);
        memcpy(send_pkt.buf + KEY_SIZE + sizeof(uint32_t), val.Buf(), VALUE_SIZE);
        sendPack(fd, sendBuf);
        recv_bytes(fd, recvBuf, 1);
    }

    bool getKey(uint32_t &sum) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + sizeof(uint32_t);
        send_pkt.type = KV_OP_GET_K;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        sendPack(fd, sendBuf);

        recv_bytes(fd, recvBuf, KEY_SIZE * KEY_NUM_TCP);

        for (int i = 0; i < KEY_NUM_TCP; ++i) {
            uint64_t k = *((uint64_t *) (recvBuf + 8 * i));

            if (k != 0 || (k == 0 && *(recvBuf + i * 8) != 0)) {
                HashLog::getInstance().put(k, (id << 28) + sum, id);
                sum++;
            } else
                return false;

        }

        return true;
    }

    int get_value_one(uint32_t pos, KVString &val) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = sizeof(uint32_t) + PACKET_HEADER_SIZE;
        send_pkt.type = KV_OP_GET_ONE_V;
        memcpy(send_pkt.buf, (char *) &pos, sizeof(uint32_t));
        sendPack(fd, sendBuf);

        char *v = new char[VALUE_SIZE];
        recv_bytes(fd, v, VALUE_SIZE);
        val.Reset(v, VALUE_SIZE);

        return 1;
    }

    int get_value(uint32_t pos, KVString &val) {
        if (pos % CLIENT_READ_CACHE_SIZE == 0) {
            sendGetBatchValue(pos);
        }

        char *v = new char[VALUE_SIZE];
        recv_bytes(fd, v, VALUE_SIZE);
        val.Reset(v, VALUE_SIZE);

        return 1;
    }

    void sendGetBatchValue(uint32_t pos) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = sizeof(uint32_t) + PACKET_HEADER_SIZE;
        send_pkt.type = KV_OP_GET_V_BATCH;
        memcpy(send_pkt.buf, (char *) &pos, sizeof(uint32_t));
        sendPack(fd, sendBuf);
    }

    void get_value_from_buffer(uint32_t pos, KVString &val) {
        uint32_t index = pos & 0x0FFFFFFF;

        uint32_t current_buf_no = index / CLIENT_READ_CACHE_SIZE;

        if (current_buf_no != value_buf_head_start_index / CLIENT_READ_CACHE_SIZE &&
            current_buf_no != value_buf_tail_start_index / CLIENT_READ_CACHE_SIZE) {

            if (current_buf_no > value_buf_tail_start_index / CLIENT_READ_CACHE_SIZE) {

                if (value_buf_tail_start_index == UINT32_MAX) {
                    value_buf_tail_start_index = current_buf_no * CLIENT_READ_CACHE_SIZE;
                    uint32_t next_pos = (pos & 0xF0000000) + value_buf_tail_start_index;
                    sendGetBatchValue(next_pos);

                    if (value_buf_tail_start_index + CLIENT_READ_CACHE_SIZE > nums) {
                        recv_bytes(fd, value_buf_tail, (nums - value_buf_tail_start_index) * VALUE_SIZE);
                    } else {
                        recv_bytes(fd, value_buf_tail, CLIENT_READ_CACHE_SIZE * VALUE_SIZE);
                    }

                } else {
                    get_value_one(pos, val);
                    return;
                }
            } else {
                // 交换两个buf
                char *tmp_value_buf = value_buf_tail;
                value_buf_tail = value_buf_head;
                value_buf_head = tmp_value_buf;

                // buf编号直接推进
                value_buf_tail_start_index = value_buf_head_start_index;
                value_buf_head_start_index = current_buf_no * CLIENT_READ_CACHE_SIZE;
                uint32_t next_pos = (pos & 0xF0000000) + value_buf_head_start_index;

                if (!random_pre_read)
                    sendGetBatchValue(next_pos);

                if (value_buf_head_start_index + CLIENT_READ_CACHE_SIZE > nums) {
                    recv_bytes(fd, value_buf_head, (nums - value_buf_head_start_index) * VALUE_SIZE);
                } else {
                    recv_bytes(fd, value_buf_head, CLIENT_READ_CACHE_SIZE * VALUE_SIZE);
                }

                if (value_buf_head_start_index >= CLIENT_READ_CACHE_SIZE) {
                    next_pos = (pos & 0xF0000000) + (value_buf_head_start_index - CLIENT_READ_CACHE_SIZE);
                    random_pre_read = true;
                    sendGetBatchValue(next_pos);
                } else {
                    random_pre_read = false;
                }
            }
        }

        char *current_value_buf;
        if (current_buf_no == value_buf_head_start_index / CLIENT_READ_CACHE_SIZE)
            current_value_buf = value_buf_head;
        else if (current_buf_no == value_buf_tail_start_index / CLIENT_READ_CACHE_SIZE)
            current_value_buf = value_buf_tail;
        else {
            current_value_buf = nullptr;
            printf("error!!! value buf no error\n");
        }

        uint32_t value_buf_offset = pos % CLIENT_READ_CACHE_SIZE;
        char *v = new char[VALUE_SIZE];
        memcpy(v, current_value_buf + value_buf_offset * VALUE_SIZE, VALUE_SIZE);
        val.Reset(v, VALUE_SIZE);
    }


    void reset() {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + sizeof(uint32_t);
        send_pkt.type = KV_OP_RESET_K;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        sendPack(fd, sendBuf);
        recv_bytes(fd, recvBuf, 1);
    }

    void recover(int sum) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + 2 * sizeof(uint32_t);
        send_pkt.type = KV_OP_RECOVER;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        memcpy(send_pkt.buf + sizeof(uint32_t), (char *) &sum, sizeof(uint32_t));
        sendPack(fd, sendBuf);
        recv_bytes(fd, recvBuf, 1);

    }

    void sendPack(int fd, char *buf) {
        auto &send_pkt = *(Packet *) buf;
        if (send(fd, buf, send_pkt.len, 0) == -1) {
            printf("send error\n");
        }
    }

    int recv_bytes(int fd, char *buf, int total) {
        int bytes = 0;

        while (total != bytes) {
            auto b = recv(fd, buf + bytes, total - bytes, 0);
            if (b <= 0) {
                return -1;
            }
            bytes += b;
        }

        return total;
    }

};

#endif //PROJECT_KV_CLIENT_H
