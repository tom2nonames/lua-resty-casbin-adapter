```lua
local Enforcer = require("casbin")
local Adapter  = require("casbin.postgres")

local pg_conf = {
    timeout = 1200,
    connect_config = {
        host = "127.0.0.1",
        port = 5432,
        database = "izw",
        user = "tom",
        password = "",
        max_packet_size = 1024 * 1024,
        charset = "utf8",
        application_name = "iot",

        ssl = false,
        ssl_required = nil,
        socket_type = "nginx",  -- "luasocket"
        application_name = "iot",
    },
    pool_config = {
        max_idle_timeout = 20000, -- 20s
        pool_size = 50 -- connection pool size
    }
}

local a = Adapter:new(pg_conf, "casbin_rules") -- hostname, port are optional
local e = Enforcer:new("/path/to/model.conf", a) --reates a new Casbin enforcer with the model.conf file and the database
```