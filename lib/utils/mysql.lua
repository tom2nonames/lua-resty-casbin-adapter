local ngx = ngx

local ngx_re_gsub = ngx.re.gsub
local ngx_re_gmatch = ngx.re.gmatch
local ngx_quote_sql_str = ngx.quote_sql_str

local type  = type
local pairs = pairs
local setmetatable = setmetatable
local pcall = pcall
local error = error
local rawget = rawget

local t_new    = table.new
local t_concat = table.concat
local t_insert = table.insert

local ssub = string.sub

local ok, mysql = pcall(require, 'resty.mysql')
if not ok then error('resty.mysql module required') end

local cjson = require("cjson")

local log   = require("lib.utils.log")

local err_code_t = {
    sql_parse_error = "sql parse error! please check",
}

local  _M = {}

local function is_array(t)
    if type(t) ~= 'table' then
        return false
    end

    for k,_ in pairs(t) do
        if type(k) ~= 'number' then
            return false
        end
    end

    return true
end

function  _M:new(conf)
    conf = conf 
    local instance = {}
    instance.conf = conf
    setmetatable(instance, { __index = self})
    return instance
end

function _M:getconn(opts)

    local conf = opts or self.conf
    local timeout = conf.timeout
    local charset = conf.connect_config.charset
    local connect_config   = conf.connect_config

    local conn = rawget(self, "_conn")

    if conn then
        return conn
    end

    local conn, err = mysql:new()
    if not conn then
		--error('failed to instantiate mysql:' .. err)
        log.error( "failed to instantiate mysql: ", err)
        return nil, err
	end

    conn:set_timeout(timeout or 5000)

    local ok, err, errno, sqlstate = conn:connect(connect_config)
    if not ok then
        log.error( "failed to connect: ", err, ": ", errno, " ", sqlstate)
        return nil, err, errno, sqlstate
    end

    log.debug( "connected to mysql, reused_times:", conn:get_reused_times())

    conn:query("SET NAMES " .. charset)
    conn:query("set character_set_client=" .. charset)
    conn:query("set character_set_results=" .. charset)

    return conn

end

function  _M:exec(sql)
    if not sql then
        log.error( "sql parse error! please check")
        return nil, "sql parse error! please check"
    end

    local conf = self.conf
    local max_idle_timeout = conf.pool_config.max_idle_timeout
    local pool_size        = conf.pool_config.pool_size

    local db, err, errno, sqlstate  = rawget(self, "_conn")
    if not db then
        db, err, errno, sqlstate = self:getconn(conf)
        if not db then
            log.error( "failed to connect: ", err, ": ", errno, " ", sqlstate)
            return nil, err
        end
    end

    local res, err, errno, sqlstate = db:query(sql)
    if not res then
        log.error( "bad result: ", err, ": ", errno, ": ", sqlstate, ".")
        if self._istrans then error(err) end
        return nil, err
    end

    if not self._conn then
        local ok, err = db:set_keepalive(max_idle_timeout, pool_size)
        if not ok then
            log.error("failed to set keepalive: ", err)
        end
    end

    return res, err, errno, sqlstate
end

function  _M:multi_exec(sql)

    local arr_res = {}
    local conf = self.conf
    local max_idle_timeout = conf.pool_config.max_idle_timeout
    local pool_size        = conf.pool_config.pool_size

    if not sql then
        log.error( "sql parse error! please check")
        return nil, "sql parse error! please check"
    end

    local db, err, errno, sqlstate  = rawget(self, "_conn")
    if not db then
        db, err, errno, sqlstate = self:getconn(conf)
        if not db then
            log.error( "failed to connect: ", err, ": ", errno, " ", sqlstate)
            return nil, err
        end
    end

    local res, err, errno, sqlstate = db:query(sql)
    if not res then
        log.error( "bad result: ", err, ": ", errno, ": ", sqlstate, ".")
        if self._istrans then error(err) end
        return nil, err
    end

    arr_res[1] = res
    log.debug("result #1: ", cjson.encode(res), " err:", err, " errno:", errno, " sqlstate:", sqlstate)
    local errcode
    local i = 2
    while err == "again" do
        res, err, errcode, sqlstate = db:read_result()
        if not res then
            log.error( "bad result #", i, ": ", err, ": ", errcode, ": ", sqlstate, ".")
        end

        log.debug("result #", i, ": ", cjson.encode(res), " err:", err, " errno:", errno, " sqlstate:", sqlstate)
        arr_res[i] = res
        i = i + 1
    end

    if not self._conn then
        local ok, err = db:set_keepalive(max_idle_timeout, pool_size)
        if not ok then
            log.error("failed to set keepalive: ", err)
        end
    end

    return arr_res, err, errno, sqlstate
end

function  _M:multi_query(sql, params)
    if not params or is_array(params) then
        sql = self:parse_sql(sql, params)
    else
        sql = self:parse_sql_bind_params(sql, params)
    end
    return self:multi_exec(sql)
end

function  _M:query(sql, params)
    if not params or is_array(params) then
        sql = self:parse_sql(sql, params)
    else
        sql = self:parse_sql_bind_params(sql, params)
    end
    return self:exec(sql)
end

function  _M:select(sql, params)
    return self:query(sql, params)
end

function  _M:insert(sql, params)
    local res, err, errno, sqlstate = self:query(sql, params)
    if res and not err then
        return  res.insert_id, err
    else
        return res, err, errno, sqlstate
    end
end

function  _M:update(sql, params)
    return self:query(sql, params)
end

function  _M:delete(sql, params)
    local res, err, errno, sqlstate = self:query(sql, params)
    if res and not err then
        return res.affected_rows, err, errno, sqlstate
    else
        return res, err, errno, sqlstate
    end
end

local function split(str, delimiter)
    if str == nil or str == '' or delimiter == nil then
        return nil
    end

    local result = {}
    local str1      =  str .. delimiter
    local regex_str = "(.-)" .. delimiter

    for match in str1:gmatch(regex_str) do
        t_insert(result, match)
    end
    
    return result
end

local function compose(t, params, cnt)
    if t == nil or params == nil or type(t) ~= "table" or 
       type(params) ~= "table" or #t ~= #params + 1 or #t == 0 then
        return nil
    else
        local t_cnt = cnt + cnt + 1
        local tab = t_new(t_cnt, 0)

        for i = 1, t_cnt do
            if i % 2 == 0 then
                tab[i] = params[ i / 2 ]
            else
                tab[i] = t[ ( i + 1 ) / 2 ]
            end
        end

        local result = t_concat(tab)
        return result
    end
end

function  _M:parse_sql(sql, params)
    if not params or not is_array(params) or #params == 0 then
        return sql
    end

    local new_params = {}

    local _, cnt = ngx_re_gsub(sql, [[\?]], '')

    for  i = 1, cnt do

        local v = params[i]
        if v and type(v) == "string" then
            v = ngx_quote_sql_str(v)
        end

        if not v then
            v = 'null'
        end

        t_insert(new_params, v)
    end

    local t = split(sql, "?")
    local sql = compose(t, new_params, cnt)

    return sql
end

function  _M:parse_sql_bind_params(sql, params)
    local new_params = {}
    for  k, v in pairs(params) do
        local val = v
        if v and type(v) == "table"  then
            if v.flag == "raw" then
            val = v.value
            end
        end

        if v and type(v) == "string" then
            val = ngx_quote_sql_str(v)
        end

        if not v then
            val = 'null'
        end

        new_params[k] = val
    end

    local regex = [[(?<str>:(?<param>([1-9]|[a-z]|[A-Z]|_){1,}))]]
    local it,err = ngx_re_gmatch(sql, regex)
    if not it then
        log.error( "gmatch error: ", err)
        return
    end
    local parse_params = {}

    while true do
        local m, err = it()
        if err then
            log.error( "gmatch error: ", err)
            return
        end

        if not m then
            break
        end

        -- found a match
        t_insert( parse_params, { str = m['str'], val = m['param'] } )
    end

    for _,v in pairs(parse_params) do
        local newstr, _, err = ngx_re_gsub(sql, v['str'],
                               new_params[v['val']] or ngx_quote_sql_str('null'), "u")
        if newstr then
            sql = newstr
        else
            log.error( "error: ", err)
            return
        end
    end

    return sql

end

function  _M.sql_fuzzy_query(sql, params, value, is_direct)
    if is_direct then
        sql = sql .. ' WHERE '
    else
        sql = sql .. ' AND '
    end
    sql = sql .. 'instr(CONCAT_WS(\',\',  '
    for k,v in pairs(params) do
        if k > 1 then
            sql = sql .. ' , '
        end
        sql = sql .. v
    end
    sql = sql .. ') , \'' .. value .. '\') > 0 '
    return sql
end

function  _M.upd_param(data, match_tb)

    local pos_param = ""

    if (match_tb == nil or type(match_tb) ~= "table") then
        return nil;
    end

    for key, val in pairs(data) do

        local tmp = match_tb[key]

        if tmp ~= nil then
            if (type(val) == "string") then
                pos_param = pos_param .. ", " .. tmp .. "= " .. ngx_quote_sql_str(val)
            else
                pos_param = pos_param .. ", " .. tmp .. "= " .. val
            end
        end
    end

    if #pos_param ~= 0 then
        pos_param = ssub(pos_param, 2)
    end

    return pos_param
end

function _M:begin(opts)
    local conn = self:getconn(opts)
    if not conn then
        return nil
    end

    self._conn = conn

    return _M:exec("BEGIN")
end

function _M:rollback()
    local max_idle_timeout = self.conf.pool_config.max_idle_timeout
    local pool_size        = self.conf.pool_config.pool_size

    _M:exec("ROLLBACK")

    if self._conn then
        local ok, err = self._conn:set_keepalive(max_idle_timeout, pool_size)
        if not ok then
            log.error("failed to set keepalive: ", err)
        end
        self._conn = nil
    end
end

function _M:commit()
    local max_idle_timeout = self.conf.pool_config.max_idle_timeout
    local pool_size        = self.conf.pool_config.pool_size

    _M:exec("COMMIT")

    if self._conn then
        local ok, err = self._conn:set_keepalive(max_idle_timeout, pool_size)
        if not ok then
            log.error("failed to set keepalive: ", err)
        end
        self._conn = nil
    end
end

function _M:trans(fn, ...)
    self._istrans = true
    _M.begin(self)
    local ok, res = pcall(fn, ...)
    if ok then
        _M.commit(self)
    else
        _M.rollback(self)
    end
    self._istrans = nil

    return ok, res
end

return  _M