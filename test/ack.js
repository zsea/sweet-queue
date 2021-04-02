const Queue = require("../lib/index.js");

(async function () {
    let q = await Queue.Create(new Queue.Store(6379, "127.0.0.1"), "TEST");
    let m = await q.Ack("c99997d73fa9438ebb07f98fb7a337318b11cf0ec6354ebf842bee996c6f879c","q39537826")
    console.log(m);
    process.exit();
})();