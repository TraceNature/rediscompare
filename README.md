# rediscompare

rediscompare 是用来对比redis 数据库数据一致性的命令行工具

![showuse](./docs/images/use.gif)

## 校验机制

rediscompare 通过scan 命令扫描源库中的左右数据依次与目标数据库进行比较，从value长度、value值、ttl等维度进行核对。最后生成result文件，文件中包含数据不一致的key已经原因。
在实际场景中，某些key可能在首次比较中由于传输延迟问题不一致，rediscompare 支持循环多次对比，既根据上次对比中不一致的key重新对比并生成result文件，循环次数可以通过"--comparetimes" 参数指定

## 场景

rediscompare 更具目标以及源的不同类型提供一下场景的对比方案

* single2single
    redis单实例到单实例的比较。用于比较单实例单库的数据一致性

* single2cluster
    用于比较单实例中某库与redis原生cluster的数据一致性

* cluster2cluster
    用于对比redis原生cluster与原生cluster的数据一致性

* multisingle2single
    用于对比多个单实例中多个库的集合与某单实例中某库的数据一致性

* multisingle2cluster
      用于对比多个单实例中多个库集合与单实例某库的数据一致性

## quick start

###  build execute file

```
git clone https://github.com/TraceNature/rediscompare.git

cd rediscompare

go mod tidy
go mod vendor

go build go build -o rediscompare
```

### 使用范例
rediscompare 支持命令行模式和交互模式，交互模式支持命令提示。对比指令支持直接命令输入和yaml定制。一下按照场景分别介绍各个场景下的基本使用。
使用 -i 参数进入交互模式 "rediscompare -i"

#### compare 子命令
* single2single
    * 命令模式

    ```
     rediscompare compare single2single  --saddr "10.0.0.1:6379"    --spassword  "redistest0102"  --taddr "10.0.0.2:6379"   --tpassword  "redistest0102" --comparetimes 3
    ``` 

* single2cluster
    * 命令模式

     ```
     rediscompare compare single2cluster  --saddr  "10.0.0.1:6379"    --spassword  "redistest0102"  --taddr "10.0.1.1:16379,10.0.1.1:16380,10.0.1.2:16379,10.0.1.2:16380,10.0.1.3:16379,10.0.1.3:16380"   --tpassword  "testredis0102" --comparetimes 3
     ```

* cluster2cluster
    * 命令模式

     ```
     rediscompare  compare cluster2cluster  --saddr  "10.0.0.1:36379,10.0.0.2:36379,10.0.0.3:36379"    --spassword  "testredis0102"  --taddr "10.0.1.1:16379,10.0.1.1:16380,10.0.1.2:16379,10.0.1.2:16380,10.0.1.3:16379,10.0.1.3:16380"   --tpassword  "testredis0102" --comparetimes 3
     ``` 

* multisingle2single
    * 执行yaml文件
     ```
     # multisingle2single yaml file
    saddr:
      - addr: "10.0.0.1:6379"
        password: "redistest0102"
        dbs:
          - 0
          - 2
          - 3
      - addr: "10.0.0.2:6379"
        password: "redistest0102"
        dbs:
          - 1
          - 5
          - 9
    taddr: "10.0.0.3:6379"
    tpassword: "redistest0102"
    batchsize: 30
    threads: 2
    ttldiff: 10000
    comparetimes: 3
    report:  true
    scenario: "multisingle2single"
     ```

     ```
    rediscompare compare exec  path/miltisingle2single.yml
     ``` 

* multisingle2cluster
    * 执行yaml文件
     
     ```
     # multisingle2cluster yaml file
     saddr:
       - addr: "10.0.0.1:6379"
         password: "redistest0102"
         dbs:
           - 0
           - 2
           - 3
       - addr: "10.0.0.2:6379"
         password: "redistest0102"
         dbs:
           - 1
           - 5
           - 9
     taddr: "10.0.1.1:16379,10.0.1.1:16380,10.0.1.2:16379,10.0.1.2:16380,10.0.1.3:16379,10.0.1.3:16380"
     tpassword: "testredis0102"
     batchsize: 30
     threads: 2
     ttldiff: 10000
     comparetimes: 3
     report:  true
     scenario: "multisingle2cluster"
     ```

     ```
    rediscompare compare exec  path/miltisingle2cluster.yml
     ``` 

multisingle2single、multisingle2cluster两个场景由于原库映射关系比较复杂命令行不易表示顾目前只支持yaml文件执行；single2single、 single2cluster、cluster2cluster支持命令行和yaml文件模式

#### result 子命令
result 子命令用来格式化.result或.rep文件，文件为对比结果，json明文。result命令将文件转换为二维表格增加可读性

```
rediscompare result parse compare_xxxxxxxx.rep
```

