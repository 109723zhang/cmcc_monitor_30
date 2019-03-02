package cn.sheep.cmcc.utils

import redis.clients.jedis.{Jedis, JedisPool}

/**
  * sheep.Old @ 64341393
  * Created 2018/5/3
  */
object Jpools {

    private val jedisPool = new JedisPool(AppParams.redisHost)


    def getJedis: Jedis = {
        val jedis = jedisPool.getResource
        jedis.select(AppParams.selectDBIndex)
        jedis
    }
}
