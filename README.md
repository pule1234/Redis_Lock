# Redis_Lock
采用Redis实现的分布式锁
 基于看门狗模式实现了锁的自动延时功能
 加锁成功之后，会自动开启看门狗， watch dog会每隔一段时间对lock进行延时， 在延时之前会判断该锁是否存在，并且是否属于自己
 ![image](https://github.com/pule1234/Redis_Lock/assets/112395669/3c4b9060-0117-43c9-baa0-c593bd911e2b)
 ![image](https://github.com/pule1234/Redis_Lock/assets/112395669/24078089-7bac-4307-82d4-247969a952b3)
![image](https://github.com/pule1234/Redis_Lock/assets/112395669/926b02e0-2259-4626-9b08-51c11fc0f9ff)

基于红锁实现数据弱一致性的问题
只有在3个及三个以上的节点，红锁才有意义



