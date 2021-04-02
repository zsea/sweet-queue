const IORedis = require("ioredis")
    , uuid = require("uuid")
    , sleep = require("./sleep.js")
    ;
const lua_push = `local msg_key="SWEET:MESSAGE:"..KEYS[1]
local set_key="SWEET:SET:"..KEYS[1]
local msg_field=KEYS[2]
local msg_val=ARGV[1]
local score=ARGV[2] 
local exists=redis.call("HSETNX",msg_key,msg_field,msg_val)
if tonumber(exists)==0 then
    return 0
end
local count=redis.call("ZADD",set_key,score,msg_field)
return count
`,
    lua_pop = `local set_key="SWEET:SET:"..KEYS[1]
    local msg_key="SWEET:MESSAGE:"..KEYS[1]
local process_key="SWEET:PROCESSING:"..KEYS[1]
local pmap_key="SWEET:PMAP:"..KEYS[1]
local score_key="SWEET:SCORE:"..KEYS[1]
local process_id=KEYS[2]
local expirse=ARGV[1]
local kv=redis.call('ZRANGEBYSCORE',set_key,'-inf','+inf','WITHSCORES','LIMIT',0,1)
if #kv==0 then
    return
end
redis.call("ZADD",process_key,expirse,process_id)
redis.call("ZREM",set_key,kv[1])
redis.call("HSET",score_key,process_id,kv[2])
redis.call("HSET",pmap_key,process_id,kv[1])
local msg=redis.call("HGET",msg_key,kv[1])
return {process_id,msg}
`,
    lua_ack = `local process_key="SWEET:PROCESSING:"..KEYS[1]
local pmap_key="SWEET:PMAP:"..KEYS[1]
local set_key="SWEET:SET:"..KEYS[1] 
local msg_key="SWEET:MESSAGE:"..KEYS[1]
local score_key="SWEET:SCORE:"..KEYS[1]
local process_id=ARGV[1]
local msgid=redis.call("HGET",pmap_key,process_id)
if not msgid then
    return false
end
redis.call("HDEL",pmap_key,process_id)
redis.call("HDEL",msg_key,msgid)
redis.call("ZREM",process_key,process_id)
redis.call("HDEL",score_key,process_id)
redis.call("ZREM",set_key,msgid)
return true
`,
    lua_expires = `local set_key="SWEET:SET:"..KEYS[1]
local process_key="SWEET:PROCESSING:"..KEYS[1]
local pmap_key="SWEET:PMAP:"..KEYS[1]
local score_key="SWEET:SCORE:"..KEYS[1]
local expires=ARGV[1]
local expiresList=redis.call('ZRANGEBYSCORE',process_key,'-inf',expires)
if #expiresList==0 then
    return
end
for i=1,#expiresList do
    local opid=expiresList[i]
    local msgid=redis.call("HGET",pmap_key,opid)
    local score=0
    if msgid then
        score=redis.call("HGET",score_key,opid)
        if not score then
            score=0
        end
    end
    redis.call("ZADD",set_key,score,msgid)
    redis.call("ZREM",process_key,opid)
    redis.call("HDEL",pmap_key,opid)
    redis.call("HDEL",score_key,opid)
end
`
    ;

function Queue(store, name, push, pop, ack, expires) {
    let redis = store,open_expires_server=true,locker_expires=15,stop_expires_callback=null;
    this.Push = async function (message, to) {
        let q_name = name;
        if (to) q_name = q_name + ":p2p:" + to;
        while (true) {
            let id = uuid.v4().replace(/\-/ig, ''), score = Date.now();
            let count = await redis.evalsha(push, 2, q_name, id, message, score);
            if (count > 0) {
                return id;
            }
        }
    }
    this.Pop = async function (expires, to) {
        let q_name = name;
        if (to) q_name = q_name + ":p2p:" + to;
        let id = (uuid.v4() + "-" + uuid.v4()).replace(/\-/ig, '');
        let _expires = expires || 30;
        if (_expires < 0) _expires = 30;
        _expires = Date.now() + _expires * 1000;
        let message = await redis.evalsha(pop, 2, q_name, id, _expires, "");
        if (message) {
            return {
                id: message[0],
                content: message[1]
            }
        }
    }
    this.Ack = async function (id, to) {
        let q_name = name;
        if (to) q_name = q_name + ":p2p:" + to;
        return !!await redis.evalsha(ack, 1, q_name, id);
    }
    this.OpenExpiresServer = async function (p2pListCall) {
        let key = "SWEET:LOCKER:" + name;
        let id = (uuid.v4() + "-" + uuid.v4()).replace(/\-/ig, '');
        //加锁
        while (open_expires_server) {
            let locker = await redis.set(key, id, "EX", locker_expires, "NX");
            if (locker) break;
            await sleep(5 * 1000);
        }

        while (open_expires_server) {
            await redis.expire(key,locker_expires);
            let q_name = name;
            await redis.evalsha(expires, 1, q_name, Date.now());
            if (p2pListCall && typeof p2pListCall === "function") {
                let p2p = await p2pListCall();
                if (p2p.length) {
                    for (let i = 0; i < p2p.length; i++) {
                        let p_name = q_name + ":p2p:" + p2p[i];
                        await redis.evalsha(expires, 1, p_name, Date.now());
                    }
                }
            }
            await sleep(5 * 1000);
        }
        if(stop_expires_callback&&typeof stop_expires_callback==="function"){
            stop_expires_callback();
        }
    }
    this.CloseExpiresServer=async function(){
        open_expires_server=false;
        await new Promise(function(resolve){
            stop_expires_callback=resolve;
        })
    }
}
async function Create(store, name) {
    let redis = store;
    if (typeof store === "undefined") {
        redis = new IORedis(store)
    }
    let push = await redis.script("load", lua_push);
    let pop = await redis.script("load", lua_pop);
    let ack = await redis.script("load", lua_ack);
    let expires = await redis.script("load", lua_expires);
    return new Queue(redis, name, push, pop, ack, expires);
}
module.exports = {
    Store: IORedis,
    Create: Create
}