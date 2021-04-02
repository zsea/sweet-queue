const Queue = require("../lib/index.js");

(async function () {
    let q = await Queue.Create(new Queue.Store(6379, "127.0.0.1"), "TEST");
    let m = await q.Pop(30,"q39537826");
    console.log(m);
    process.exit();
})();