# 第三届阿里中间件性能挑战赛-初赛

## 优化点
### 生产阶段
- 根据测试message的规律充分压缩数据
- 一个bucket包含多个文件，将消费者根据其id来哈希到不同的文件，这样可以降低多个消费者在写同一个bucket时由锁带来的时间开销
- 写使用PrintWriter，并设置其buffer大小

### 消费阶段
- 由于每个topic会由多个消费者消费，所以消费者先轮流消费topic，每次每个topic读一个message，下次换下一个topic，这样能保证各个消费者的消费进度相似，从而提高文件系统里缓存的命中率。由于queue只由绑定的消费者消费，所以消费者等消费完topic list之后再消费queue
- 一个bucket由多个文件保存，消费时轮流消费，提高文件系统里缓存的命中率
- 读用BufferedReader，并设置其buffer大小
- 由于每条message经过压缩，且有规律所寻，所以读取时可以直接操作String的index来解析message，从而降低字符串处理的时间复杂度。


### 走的弯路

- 刚开始使用了比较通用的方法来保存keyvalue结构，力求保存各种value类型的信息，这增加了空间复杂度，比赛快结束了才去研究测试数据的规律，于是进行了针对性优化
- 使用MappedByteBuffer来写数据，在花了较长时间的尝试之后才发现效果并不理想。个人猜测是因为只有在对文件同时进行读写操作，才能反应内存文件映射带来的好处，而比赛的场景是首先由生产者写大量的数据，最后再由消费者消费。由于在写阶段硬盘始终是高负荷写的，所以使用内存映射意义不大。 
- 在写阶段开新的线程写，由于考虑不周，需要开较多的线程，从而带来了巨大的开线程的开销

## 收获

- 对java的io操作有了更深的了解
- 熟悉了多线程环境下的编程
- 深刻理解了接口、继承、多态这些面对对象编程的概念给开发带来的好处

