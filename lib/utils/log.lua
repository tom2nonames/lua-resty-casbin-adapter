local ngx = ngx
local ngx_log  = ngx.log
local require  = require
local setmetatable = setmetatable

local _M = {version = 0.3}


local log_levels = {
    stderr = ngx.STDERR,
    emerg  = ngx.EMERG,
    alert  = ngx.ALERT,
    crit   = ngx.CRIT,
    error  = ngx.ERR,
    warn   = ngx.WARN,
    notice = ngx.NOTICE,
    info   = ngx.INFO,
    debug  = ngx.DEBUG,
}


local cur_level = ngx.config.subsystem == "http" and
                  require "ngx.errlog" .get_sys_filter_level()
local do_nothing = function() end
setmetatable(_M, {__index = function(self, cmd)
    local log_level = log_levels[cmd]

    local method
    if cur_level and (log_level > cur_level)
    then
        method = do_nothing
    else
        method = function(...)
            return ngx_log(log_level, ...)
        end
    end

    -- cache the lazily generated method in our
    -- module table
    _M[cmd] = method
    return method
end})

_M.levels = log_levels

return _M