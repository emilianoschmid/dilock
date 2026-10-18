local digest = require "openssl.digest"

local M = {}

local function valid_utf8(value)
    local i, n = 1, #value
    while i <= n do
        local a = value:byte(i)
        if a <= 0x7f then i = i + 1
        elseif a >= 0xc2 and a <= 0xdf then
            local b = value:byte(i + 1)
            if not b or b < 0x80 or b > 0xbf then return false end
            i = i + 2
        elseif a == 0xe0 then
            local b, c = value:byte(i + 1, i + 2)
            if not b or not c or b < 0xa0 or b > 0xbf or c < 0x80 or c > 0xbf then return false end
            i = i + 3
        elseif (a >= 0xe1 and a <= 0xec) or (a >= 0xee and a <= 0xef) then
            local b, c = value:byte(i + 1, i + 2)
            if not b or not c or b < 0x80 or b > 0xbf or c < 0x80 or c > 0xbf then return false end
            i = i + 3
        elseif a == 0xed then
            local b, c = value:byte(i + 1, i + 2)
            if not b or not c or b < 0x80 or b > 0xbf or c < 0x80 or c > 0x9f then return false end
            i = i + 3
        elseif a == 0xf0 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x90 or b > 0xbf or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        elseif a >= 0xf1 and a <= 0xf3 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x80 or b > 0xbf or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        elseif a == 0xf4 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x90 or b > 0x8f or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        else return false end
    end
    return true
end

local function u32be(value)
    return string.char(math.floor(value / 16777216) % 256, math.floor(value / 65536) % 256,
        math.floor(value / 256) % 256, value % 256)
end

function M.resource_id(namespace, resource)
    if type(namespace) ~= "string" or #namespace < 1 or #namespace > 64
        or not namespace:match("^[a-z0-9][a-z0-9_.%-]*$") then
        return nil, "invalid_namespace"
    end
    if type(resource) ~= "string" or #resource < 1 or #resource > 1024
        or resource:find("%z") or not valid_utf8(resource) then
        return nil, "invalid_resource"
    end
    local hash = digest.new("sha256")
    hash:update("dilock.lock.identity\0" .. u32be(#namespace) .. namespace .. u32be(#resource) .. resource)
    local raw = hash:final()
    return (raw:gsub(".", function(byte) return string.format("%02x", byte:byte()) end))
end

M.valid_utf8 = valid_utf8
return M
