# lua-resty-redis-bloomfilter

##Overview
You can know the introduction of Bloom Filter from wikipedia <https://en.wikipedia.org/wiki/Bloom_filter>.

About the hash algorithm i used, please refer to the algorithm from <https://github.com/erikdubbelboer/redis-lua-scaling-bloom-filter>.

In order to reduce runtime computation, i pre-computed a lot of calculation and cache them globally.

##Configuration
you can find three configuration variables, **"SIZE"**, **"PRECISION"**, **"NAME"**.

SIZE means the size of bit array.</br>
PRECISION means the missing tolerance of bloom filter.</br>
NAME means the prefix of redis key.

Modify them to be perfect for your program.

##Usage
1. bf_check(redis_client, data) return 0/1 (0: not found, 1: found)	
2. bf_add(redis_client, data) return "OK"

redis_client: please new a redis object and make it connected using lua-resty-redis.</br>
data: the data stored in bloom filter.


######*Last, i am glad and open to receive any advise for this function. Thanks for reading*
