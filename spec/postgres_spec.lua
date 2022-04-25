local pg      = require("lib.utils.postgres")
local Adapter = require("casbin.postgres")
local Enforcer = require("casbin")
local path = os.getenv("PWD") or io.popen("cd"):read()

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

local function initDB()


    local db = pg:new(pg_conf)
    local sql =  [[
DELETE FROM :table_name;
INSERT INTO :table_name (ptype, v0, v1, v2) VALUES ( 'p', 'alice', 'data1', 'read');
INSERT INTO :table_name (ptype, v0, v1, v2) VALUES ( 'p', 'bob', 'data2', 'write');
INSERT INTO :table_name (ptype, v0, v1, v2) VALUES ( 'p', 'data2_admin', 'data2', 'read');
INSERT INTO :table_name (ptype, v0, v1, v2) VALUES ( 'p', 'data2_admin', 'data2', 'write');
INSERT INTO :table_name (ptype, v0, v1)     VALUES ( 'g', 'alice', 'data2_admin');
COMMIT
]]
    local params = {
        table_name = { flag = "raw", value = "casbin_rules" }
    }
    local res, err = db:multi_query(sql, params)
    
    local a = Adapter:new(pg_conf, "casbin_rules")

    return a
end

local function getEnforcer()
    local e = Enforcer:new(path .. "/spec/rbac_model.conf", path .. "/spec/empty_policy.csv")
    local a = initDB()
    e.adapter = a
    e:loadPolicy()
    return e
end

describe("Casbin PostgreSQL Adapter tests", function ()
    it("Load Policy test", function ()
        local e = getEnforcer()
        assert.is.True(e:enforce("alice", "data1", "read"))
        assert.is.False(e:enforce("bob", "data1", "read"))
        assert.is.True(e:enforce("bob", "data2", "write"))
        assert.is.True(e:enforce("alice", "data2", "read"))
        assert.is.True(e:enforce("alice", "data2", "write"))
    end)

    it("Load Filtered Policy test", function ()
        local e = getEnforcer()
        e:clearPolicy()
        assert.is.Same({}, e:GetPolicy())

        assert.has.error(function ()
            local filter = {"alice", "data1"}
            e:loadFilteredPolicy(filter)
        end)

        local filter = {
            ["v0"] = "bob"
        }
        e:loadFilteredPolicy(filter)
        assert.is.Same({{"bob", "data2", "write"}}, e:GetPolicy())
        e:clearPolicy()

        filter = {
            ["v2"] = "read"
        }
        e:loadFilteredPolicy(filter)
        assert.is.Same({
            {"alice", "data1", "read"},
            {"data2_admin", "data2", "read"}
        }, e:GetPolicy())
        e:clearPolicy()

        filter = {
            ["v0"] = "data2_admin",
            ["v2"] = "write"
        }
        e:loadFilteredPolicy(filter)
        assert.is.Same({{"data2_admin", "data2", "write"}}, e:GetPolicy())
    end)

    it("Add Policy test", function ()
        local e = getEnforcer()
        assert.is.False(e:enforce("eve", "data3", "read"))
        e:AddPolicy("eve", "data3", "read")
        assert.is.True(e:enforce("eve", "data3", "read"))
    end)

    it("Add Policies test", function ()
        local e = getEnforcer()
        local policies = {
            {"u1", "d1", "read"},
            {"u2", "d2", "read"},
            {"u3", "d3", "read"}
        }
        e:clearPolicy()
        e.adapter:savePolicy(e.model)
        assert.is.Same({}, e:GetPolicy())

        e:AddPolicies(policies)
        e:clearPolicy()
        e:loadPolicy()
        assert.is.Same(policies, e:GetPolicy())
    end)

    it("Save Policy test", function ()
        local e = getEnforcer()
        assert.is.False(e:enforce("alice", "data4", "read"))

        e.model:clearPolicy()
        e.model:addPolicy("p", "p", {"alice", "data4", "read"})
        e.adapter:savePolicy(e.model)
        e:loadPolicy()

        assert.is.True(e:enforce("alice", "data4", "read"))
    end)

    it("Remove Policy test", function ()
        local e = getEnforcer()
        assert.is.True(e:HasPolicy("alice", "data1", "read"))
        e:RemovePolicy("alice", "data1", "read")
        assert.is.False(e:HasPolicy("alice", "data1", "read"))
    end)

    it("Remove Policies test", function ()
        local e = getEnforcer()
        local policies = {
            {"alice", "data1", "read"},
            {"bob", "data2", "write"},
            {"data2_admin", "data2", "read"},
            {"data2_admin", "data2", "write"}
        }
        assert.is.Same(policies, e:GetPolicy())
        e:RemovePolicies({
            {"data2_admin", "data2", "read"},
            {"data2_admin", "data2", "write"}
        })

        policies = {
            {"alice", "data1", "read"},
            {"bob", "data2", "write"}
        }
        assert.is.Same(policies, e:GetPolicy())
    end)

    it("Update Policy test", function ()
        local e = getEnforcer()
        local policies = {
            {"alice", "data1", "read"},
            {"bob", "data2", "write"},
            {"data2_admin", "data2", "read"},
            {"data2_admin", "data2", "write"}
        }
        assert.is.Same(policies, e:GetPolicy())

        e:UpdatePolicy(
            {"bob", "data2", "write"},
            {"bob", "data2", "read"}
        )
        policies = {
            {"alice", "data1", "read"},
            {"bob", "data2", "read"},
            {"data2_admin", "data2", "read"},
            {"data2_admin", "data2", "write"}
        }

        assert.is.Same(policies, e:GetPolicy())
    end)

    it("Update Policies test", function ()
        local e = getEnforcer()
        local policies = {
            {"alice", "data1", "read"},
            {"bob", "data2", "write"},
            {"data2_admin", "data2", "read"},
            {"data2_admin", "data2", "write"}
        }
        assert.is.Same(policies, e:GetPolicy())

        e:UpdatePolicies(
                {{"alice", "data1", "read"},{"bob", "data2", "write"}},
                {{"alice", "data1", "write"},{"bob", "data2", "read"}}
        )
        policies = {
            {"alice", "data1", "write"},
            {"bob", "data2", "read"},
            {"data2_admin", "data2", "read"},
            {"data2_admin", "data2", "write"}
        }

        assert.is.Same(policies, e:GetPolicy())
    end)

    it("Update Filtered Policies test", function ()
        local e = getEnforcer()
        assert.is.True(e:enforce("alice", "data1", "read"))
        e:UpdateFilteredPolicies({{"alice", "data1", "write"}},1, {"data1"})
        assert.is.False(e:enforce("alice", "data1", "read"))
        assert.is.True(e:enforce("alice", "data1", "write"))

        assert.is.True(e:enforce("bob", "data2", "write"))
        assert.is.True(e:enforce("alice", "data2", "read"))
        assert.is.True(e:enforce("alice", "data2", "write"))

        e:UpdateFilteredPolicies({{"bob", "data2","read"},{"admin", "data2","read"}},1, {"data2","write"})

        assert.is.False(e:enforce("bob", "data2", "write"))
        assert.is.True(e:enforce("alice", "data2", "read"))
        assert.is.False(e:enforce("alice", "data2", "write"))
        assert.is.True(e:enforce("bob", "data2","read"))
        assert.is.True(e:enforce("admin", "data2","read"))

    end)

    it("Remove Filtered Policy test", function ()
        local e = getEnforcer()
        assert.is.True(e:enforce("alice", "data1", "read"))
        e:RemoveFilteredPolicy(1, "data1")
        assert.is.False(e:enforce("alice", "data1", "read"))

        assert.is.True(e:enforce("bob", "data2", "write"))
        assert.is.True(e:enforce("alice", "data2", "read"))
        assert.is.True(e:enforce("alice", "data2", "write"))

        e:RemoveFilteredPolicy(1, "data2", "read")

        assert.is.True(e:enforce("bob", "data2", "write"))
        assert.is.False(e:enforce("alice", "data2", "read"))
        assert.is.True(e:enforce("alice", "data2", "write"))

        e:RemoveFilteredPolicy(1, "data2")

        assert.is.False(e:enforce("bob", "data2", "write"))
        assert.is.False(e:enforce("alice", "data2", "read"))
        assert.is.False(e:enforce("alice", "data2", "write"))
    end)
end)