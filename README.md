# OkCache
一个高性能二级缓存实现, 内存LRU缓存 + 磁盘文件持久化缓存。

* 支持过期(Expiration)清除；
* 支持LRU ~ 如果超过内存缓存容量，最近不常使用的项将被剔除(Eviction)；
* 支持剔除(Eviction)到二级可持久化缓存(BigCache)；
* 支持回写(Write Behind)到后端持久化存储，例如DB。
* BigCache的Key常驻内存，Value可持久化。
* BigCache支持纯磁盘文件，内存映射+磁盘文件，和堆外内存+磁盘文件三种模式。

## 注意

1. 运行BigCache单元测试前，请将TestUtil.TEST_BASE_DIR修改为本地测试目录：
2. 回写到MySQL的代码未开源，需要的自己实现`BehindStorage`接口即可。
