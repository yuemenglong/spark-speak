# spark-speak
## 0x00 自己动手从零实现spark
目标：
1. 实现spark的功能
2. 采用尽量简单的技术
3. 不关注性能

项目暂定取名Speak 

## 0x01 万事开头难，先把架子搭出来
开始先实现如下功能
```
val conf = new SparkConf().setAppName("Speak").setMaster("local[2]")
var sc = new SparkContext(conf)
sc.parallelize(1 to 10, 2).map(_ * 2).foreach(println)
```

## 0x02 单机版本变为分布式版本
## 0x03 加入shuffle和reduce
![image](http://github.com/yuemenglong/spark-speak/raw/master/pic/shuffle.png)
