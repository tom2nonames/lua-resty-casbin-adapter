--Copyright 2021 The casbin Authors. All Rights Reserved.
--
--Licensed under the Apache License, Version 2.0 (the "License");
--you may not use this file except in compliance with the License.
--You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
--Unless required by applicable law or agreed to in writing, software
--distributed under the License is distributed on an "AS IS" BASIS,
--WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--See the License for the specific language governing permissions and
--limitations under the License.
local setmetatable = setmetatable

local Adapter = require("Adapter")
local rdb = require("lib.utils.postgres")

local _M = {}
Adapter.__index = Adapter
setmetatable(_M, Adapter)

local sql = [[
CREATE TABLE IF NOT EXISTS :table_name (
    id bigserial NOT NULL,
    ptype varchar(255) NOT NULL,
    v0 varchar(255) DEFAULT NULL,
    v1 varchar(255) DEFAULT NULL,
    v2 varchar(255) DEFAULT NULL,
    v3 varchar(255) DEFAULT NULL,
    v4 varchar(255) DEFAULT NULL,
    v5 varchar(255) DEFAULT NULL,
    PRIMARY KEY (id)
    )
]]

function _M:new(conf, table_name)

    local params = {
        table_name = { flag = "raw", value = table_name }
    }

    local db = rdb:new(conf)
    local result, err = db:query(sql, params)

    local o = {}
    o.table_name = table_name or "casbin_rule"
    o.db = db

    self.__index = self
    setmetatable(o, self)

    return o
end

return _M