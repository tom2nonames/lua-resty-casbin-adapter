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
local error = error
local tostring = tostring

local format    = string.format
local ssub      = string.sub

local t_insert  = table.insert
local t_concat  = table.concat

local Adapter = require("src.persist.Adapter")
local Util = require("src.util.Util")

local _M = {}

-- Filter for filtered policies
local Filter = {
    ptype = "",
    v0 = "",
    v1 = "",
    v2 = "",
    v3 = "",
    v4 = "",
    v5 = ""
}

--[[
    * loadPolicy loads all policy rules from the storage.
]]
function _M:loadPolicy(model)
    
    local sql = "SELECT concat_ws(', ', ptype, v0, v1, v2, v3, v4, v5) as line FROM :table_name"
    local params = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    for i, row in ipairs(result) do
    
        local line = Util.trim(row.line)
        
        Adapter.loadPolicyLine(line, model)
    end
    return true
end

function _M:savePolicyLine(ptype, rule)
    local row = "'" .. ptype .. "'"
    for _, v in pairs(rule) do
        row = row .. ", '" .. v .. "'"
    end

    local cols = "ptype"
    for k = 0, #rule-1 do
        cols = cols .. ", " .. "v" .. tostring(k)
    end

    local sql_fmt = "INSERT INTO :table_name  (%s) VALUES (%s)"
    local sql = format(sql_fmt, cols, row)

    local params = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    return true
end

--[[
    * savePolicy saves all policy rules to the storage.
]]
function _M:savePolicy(model)
    local sql = "DELETE FROM :table_name"
    local params = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)

    if model.model["p"] then
        for ptype, ast in pairs(model.model["p"]) do
            for _, rule in pairs(ast.policy) do
                self:savePolicyLine(ptype, rule)
            end
        end
    end

    if model.model["g"] then
        for ptype, ast in pairs(model.model["g"]) do
            for _, rule in pairs(ast.policy) do
                self:savePolicyLine(ptype, rule)
            end
        end
    end
end

--[[
    * addPolicy adds a policy rule to the storage.
]]
function _M:addPolicy(_, ptype, rule)
    return self:savePolicyLine(ptype, rule)
end

--[[
    * addPolicies adds policy rules to the storage.
]]
function _M:addPolicies(_, ptype, rules)
    local rows = ""
    local cols = "ptype"
    for k = 0, 5 do
        cols = cols .. ", " .. "v" .. tostring(k)
    end

    for _, rule in pairs(rules) do
        rows = rows .. "("
        local row = "'" .. ptype .. "'"
        for _, v in pairs(rule) do
            row = row .. ", '" .. v .. "'"
        end
        for k = #rule, 5 do
            row = row .. ", NULL"
        end

        rows = rows .. row .. "), "
    end

    if rules == "" then return true end

    rows = ssub(rows, 1, -3)

    local sql_fmt = "INSERT INTO :table_name  (%s) VALUES %s"
    local sql = format(sql_fmt, cols, rows)
    local params = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    return true
end

--[[
    * removePolicy removes a policy rule from the storage.
]]
function _M:removePolicy(_, ptype, rule)
    local condition = {"ptype = '" .. ptype .. "'"}

    for k=0, #rule-1 do
        local c = "v" .. tostring(k) .. " = '" .. rule[k+1] .. "'"
        t_insert(condition, c)
    end
    
    local sql_fmt = "DELETE FROM :table_name WHERE %s"
    local where = Util.trim(t_concat(condition, " AND "))
    local sql = format(sql_fmt, where)
    local params = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    return true
end

--[[
    * removePolicies removes policy rules from the storage.
]]
function _M:removePolicies(_, ptype, rules)
    for _, rule in pairs(rules) do
        local _, err = self:removePolicy(_, ptype, rule)
        if err then
            return false, err
        end
    end

    return true
end

--[[
    * updatePolicy updates a policy rule from the storage
]]
function _M:updatePolicy(_, ptype, oldRule, newRule)
    local update = {"ptype = '" .. ptype .. "'"}

    for k=0, #newRule-1 do
        local c = "v" .. tostring(k) .. " = '" .. newRule[k+1] .. "'"
        t_insert(update, c)
    end

    local condition = {"ptype = '" .. ptype .. "'"}

    for k=0, #oldRule-1 do
        local c = "v" .. tostring(k) .. " = '" .. oldRule[k+1] .. "'"
        t_insert(condition, c)
    end

    local sql_fmt = "UPDATE :table_name SET %s WHERE %s"
    local update  = Util.trim(t_concat(update, ", "))
    local where   = Util.trim(t_concat(condition, " AND "))
    local sql     = format(sql_fmt, update, where)
    local params  = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    return true
end

--[[
    * updatePolicies updates policy rules from the storage
]]
function _M:updatePolicies(_, ptype, oldRules, newRules)
    if #oldRules == #newRules then
        for i = 1, #oldRules do
            local _, err = self:updatePolicy(_, ptype, oldRules[i], newRules[i])
            if err then
                return false, err
            end
        end
        return true
    end
    return false
end

function _M:updateFilteredPolicies(_, ptype, newRules, fieldIndex, fieldValues)
    return self:removeFilteredPolicy(_, ptype, fieldIndex, fieldValues) and self:addPolicies(_, ptype, newRules)

end


--[[
    * loadFilteredPolicy loads the policy rules that match the filter from the storage.
]]
function _M:loadFilteredPolicy(model, filter)
    local values = {}

    for col, val in pairs(filter) do
        if not Filter[col] then
            error("Invalid filter column " .. col)
        end
        if Util.trim(val) ~= "" then
            t_insert(values, col .. " = '" .. Util.trim(val) .. "'")
        end
    end
    local sql_fmt ="SELECT concat_ws(', ', ptype, v0, v1, v2, v3, v4, v5) as line FROM :table_name WHERE %s"
    local where = t_concat(values, " AND ")
    local sql = format(sql_fmt, where)

    local params  = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    for i, row in ipairs(result) do
        local line = Util.trim(row.line)
        Adapter.loadPolicyLine(line, model)
    end

    self.isFiltered = true
    return true
end

--[[
    * removeFilteredPolicy removes the policy rules that match the filter from the storage.
]]
function _M:removeFilteredPolicy(_, ptype, fieldIndex, fieldValues)
    local values = {}
    t_insert(values, "ptype = '" .. ptype .. "'")
    local i = fieldIndex + 1
    for j = 1, #fieldValues do
        if Util.trim(fieldValues[j]) ~= "" then
            t_insert(values, "v" .. tostring(i-1) .. " = '" .. Util.trim(fieldValues[j]) .. "'")
        end
        i = i + 1
    end

    local sql_fmt = "DELETE FROM :table_name WHERE %s"
    local where = Util.trim(t_concat(values, " AND "))
    local sql = format(sql_fmt, where)
    local params = {
        table_name = { flag = "raw", value = self.table_name }
    }
    local result, err = self.db:query(sql, params)
    if err then
        return false, err
    end

    return true
end

return _M
