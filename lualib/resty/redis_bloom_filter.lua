local _M = {size = 0, precision = 0, name = ""}
local resty_sha1 = require "resty.sha1"
local redis_helper = require "resty.redis_cluster_helper"
local str = require "resty.string"

local SIZE = 100000
local PRECISION = 0.01
local NAME = "channelid"

local maxk_table = {}
local bits_table = {}
local k_table = {}

local count_temp = 0
local last_clock = 0

for i = 1, 30 do
    table.insert(maxk_table, math.floor(0.693147180 * math.floor((SIZE * math.log(PRECISION * math.pow(0.5, i))) / -0.480453013) / SIZE))
    table.insert(bits_table, math.floor((SIZE * math.log(PRECISION * math.pow(0.5, i))) / -0.480453013))
    table.insert(k_table, math.floor(0.693147180 * bits_table[i] / SIZE))
end

local function hash_string(data)
    local sha1 = resty_sha1:new()
    sha1:update("" .. data)
    local digest = sha1:final()

    return str.to_hex(digest)
end

function _M:new()
    self.size      = SIZE
    self.precision = PRECISION
    self.name      = NAME

    return self
end

function _M:bf_check(data)
    local begin_time = os.clock()
    local cluster = redis_helper.qt_get_redis_cluster()

    local diff0 = os.clock() - begin_time

    local running = cluster:get(self.name .. ':running')
    if not running or running == ngx.null then
        return 1
    end

    local diff1 = os.clock() - begin_time

    local count_diff = os.time() - last_clock
    if count_temp == 0 or last_clock == 0 or count_diff > 43200 then
        count_temp = cluster:get(self.name .. ':count')
        if not count_temp or count_temp == ngx.null then
            return 0
        end
        last_clock = os.time()
    end

    local diff2 = os.clock() - begin_time

    index = math.ceil(count_temp / self.size)

    local hash = hash_string(data)
    local h = { }
    h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
    h[1] = tonumber(string.sub(hash, 8 , 16), 16)
    h[2] = tonumber(string.sub(hash, 16, 24), 16)
    h[3] = tonumber(string.sub(hash, 24, 32), 16)

    local maxk = maxk_table[index] or math.floor(0.693147180 * math.floor((self.size * math.log(self.precision * math.pow(0.5, index))) / -0.480453013) / self.size)

    local b = { }
    for i=1, maxk do
        table.insert(b, h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)])
        end
        for n=1, index do
            local key_index = os.time() % 3

            local key   = self.name .. key_index .. ':' .. n
            local found = true
            local bits = bits_table[n] or math.floor((self.size * math.log(self.precision * math.pow(0.5, n))) / -0.480453013)
            local k = k_table[n] or math.floor(0.693147180 * bits / self.size)

            local diff3 = os.clock() - begin_time

            for i=1, k do
                if cluster:getbit(key, b[i] % bits) == 0 then
                    found = false
                    break
                end
            end

            local diff4 = os.clock() - begin_time

            if found then
                local end_time = os.clock()
                local diff = end_time - begin_time
                if diff >= 3 then
                    ngx.log(ngx.WARN, "bloom filter check found final duration: " .. diff)
                    ngx.log(ngx.WARN, "bloom filter check found diff0 duration: " .. diff0)
                    ngx.log(ngx.WARN, "bloom filter check found diff1 duration: " .. diff1)
                    ngx.log(ngx.WARN, "bloom filter check found diff2 duration: " .. diff2 .. " count_temp: " .. count_temp)
                    ngx.log(ngx.WARN, "bloom filter check found diff3 duration: " .. diff3)
                    ngx.log(ngx.WARN, "bloom filter check found diff4 duration: " .. diff4)
                end
                return 1
            end
        end
        local end_time = os.clock()
        local diff = end_time - begin_time
        if diff >= 3 then
            ngx.log(ngx.WARN, "bloom filter check not found final duration: " .. diff)
            ngx.log(ngx.WARN, "bloom filter check not found diff0 duration: " .. diff0)
            ngx.log(ngx.WARN, "bloom filter check not found diff1 duration: " .. diff1)
            ngx.log(ngx.WARN, "bloom filter check not found diff2 duration: " .. diff2 .. " count_temp: " .. count_temp)
        end
    return 0

end

function _M:bf_add(data)
    local cluster = redis_helper.qt_get_redis_cluster()

    local index = math.ceil(cluster:incr(self.name .. ":count")/self.size)
    local key0 = self.name .. "0:" .. index
    local key1 = self.name .. "1:" .. index
    local key2 = self.name .. "2:" .. index

    local bits = bits_table[index] or math.floor(-(self.size * math.log(self.precision * math.pow(0.5, index))) / 0.480453013)
    local k = k_table[index] or math.floor(0.693147180 * bits / self.size)
    local hash = hash_string(data)

    local h = { }
    h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
    h[1] = tonumber(string.sub(hash, 8 , 16), 16)
    h[2] = tonumber(string.sub(hash, 16, 24), 16)
    h[3] = tonumber(string.sub(hash, 24, 32), 16)
    for i=1, k do
        cluster:setbit(key0, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, 1)
        cluster:setbit(key1, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, 1)
        cluster:setbit(key2, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, 1)
    end
end

function _M:bf_reset(is_done)
    local cluster = redis_helper.qt_get_redis_cluster()

    if is_done then
        local re = cluster:set(self.name .. ":running", true)
        ngx.say(re)
    else
        redis_helper.qt_redis_cluster_purge_dir(self.name)
        ngx.say("OK")
    end

end

return _M

