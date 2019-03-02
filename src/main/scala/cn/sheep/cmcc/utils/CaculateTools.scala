package cn.sheep.cmcc.utils

import org.apache.commons.lang3.time.FastDateFormat

/**
  * sheep.Old @ 64341393
  * Created 2018/5/3
  */
object CaculateTools {

    // 线程安全的DateFormat
    private val format: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

    /**
      * 计算时间差
      */
    def caculateTime(starTime: String, endTime: String) = {
        val start = starTime.substring(0, 17)
        format.parse(endTime).getTime - format.parse(start).getTime
    }


}
