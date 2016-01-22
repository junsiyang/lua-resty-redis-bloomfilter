local _M = { }
local resty_sha1 = require "resty.sha1"
local redis = require "resty.redis"
local str = require "resty.string"

local SIZE = 1000000
local PRECISION = 0.1
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

function _M:bf_check(redis_client, data)
    local count_diff = os.time() - last_clock
    if count_temp == 0 or last_clock == 0 or count_diff > 43200 then
        count_temp = redis_client:get(NAME .. ':count')
        if not count_temp or count_temp == ngx.null then
            return 0
        end
        last_clock = os.time()
    end

    index = math.ceil(count_temp / SIZE)

    local hash = hash_string(data)
    local h = { }
    h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
    h[1] = tonumber(string.sub(hash, 8 , 16), 16)
    h[2] = tonumber(string.sub(hash, 16, 24), 16)
    h[3] = tonumber(string.sub(hash, 24, 32), 16)

    local maxk = maxk_table[index] or math.floor(0.693147180 * math.floor((SIZE * math.log(PRECISION * math.pow(0.5, index))) / -0.480453013) / SIZE)

    local b = { }
    for i=1, maxk do
        table.insert(b, h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)])
        end
        for n=1, index do
            local key   = NAME .. ':' .. n
            local found = true
            local bits = bits_table[n] or math.floor((SIZE * math.log(PRECISION * math.pow(0.5, n))) / -0.480453013)
            local k = k_table[n] or math.floor(0.693147180 * bits / SIZE)

            for i=1, k do
                if redis_client:getbit(key, b[i] % bits) == 0 then
                    found = false
                    break
                end
            end

            if found then
                return 1
            end
        end
    return 0

end

function _M:bf_add(redis_client, data)
    local index = math.ceil(redis_client:incr(NAME .. ":count")/SIZE)

    local key   = NAME .. ':' .. index 

    local bits = bits_table[index] or math.floor(-(SIZE * math.log(PRECISION * math.pow(0.5, index))) / 0.480453013)
    local k = k_table[index] or math.floor(0.693147180 * bits / SIZE)
    local hash = hash_string(data)

    local h = { }
    h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
    h[1] = tonumber(string.sub(hash, 8 , 16), 16)
    h[2] = tonumber(string.sub(hash, 16, 24), 16)
    h[3] = tonumber(string.sub(hash, 24, 32), 16)

    for i=1, k do
        redis_client:setbit(key, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, 1)
    end
end

return _M

