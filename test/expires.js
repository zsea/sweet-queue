const Queue = require("../lib/index.js");

(async function () {
    let q = await Queue.Create(new Queue.Store(6379, "127.0.0.1"), "TEST");
    let id=Date.now()
    q.OpenExpiresServer(async function(){
        console.log("读取子队列",id,Date.now())
        return ["q39537826"]
    });
    await new Promise(function(resolve){
        setTimeout(resolve,1*60*1000)
    })
    await q.CloseExpiresServer();
    console.log("服务已闭",id);
    process.exit();
})();