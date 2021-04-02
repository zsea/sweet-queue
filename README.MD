# sweet-queue

一个以Redis为后端存储的简单队列，实现了消息的入队、消费等功能，可以指定消息的消费者，即点对点消息。

# 安装

```
npm install --save sweet-queue
```

# 使用

## 创建消息队列

```javascript
const Queue=require("sweet-queue")
let q = await Queue.Create(new Queue.Store(6379, "127.0.0.1"), "TEST");

```

## 消息入队

```javascript
await q.Push("aaaaa:" + Date.now(),"q");
```

## 读取一条消息

```javascript
await q.Pop(30,"q");
```

## 确认消费

```javascript
await q.Ack("c99997d73fa9438ebb07f98fb7a337318b11cf0ec6354ebf842bee996c6f879c","q");
```

## 开启超时消息处理
```javascript
q.OpenExpiresServer(async function(){
        //console.log("读取子队列",id,Date.now())
        return ["q1"]
    });
```

## 关闭超时消息处理

```javascript
await q.CloseExpiresServer();
```

# 所有接口

* Queue.Push(msg,to)
* Queue.Pop(expires,to)
* Queue.Ack(id,to)
* Queue.OpenExpiresServer(p2pListCall)
* Queue.CloseExpiresServer()