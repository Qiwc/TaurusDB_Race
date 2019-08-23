# 华为云TaurusDB性能挑战赛
### 1 前言

本次比赛参赛队伍是polarDB比赛原班人马，团队名称Watermelon，团队成员分别来自上海交大和浙大：

- [HelloYym](<https://github.com/HelloYym>)

- [Qiwc](<https://github.com/Qiwc>)

- [cyif](<https://github.com/cyif>)

  

[题目链接](<https://developer.huaweicloud.com/competition/competitions/1000001898/circumstances>)

初赛和复赛均采用CPP，均取得第四名。

初赛代码在kvstore目录下，本地测试方法均有说明，因初赛过于简单，后面不讲了

复赛代码在kvstore_se目录下，后面只讲复赛大致思路

### 2 复赛文件架构与缓存架构图



![image](https://github.com/Qiwc/TaurusDB_Race/tree/master/image/file.png)



### 3 复赛网络

复赛网络可以说是很坑了，c++一开始没有说限流，官方一开始也说网络也没任何问题。所以造成大批数据网络传输不可以。。。。所以现在代码里面还是做了主动限流。

计算节点作为客户端，16线程16实例。存储节点作为服务端，16线程，16socket与客户端一一对应，最简单的通信方式。

只是客户端与服务端搞好协议就ok了

### 4 复赛读缓存

复赛读缓存分为顺序读和随机读两种。随机读作的方式比较特别，两个buf来回切换

### 5 大赛总结

日常感谢队友呗！毕业顺利！
