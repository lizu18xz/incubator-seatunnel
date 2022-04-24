package org.apache.seatunnel.udf.func;

import java.util.UUID;
import org.apache.seatunnel.udf.constants.UdfFunction;

/**
 * @author lizu
 * @since 2022/4/24
 */
public class StringFunction {

    /**
     *
     */
    private StringFunction()
    {
        throw new IllegalStateException("Utility class");
    }

    /**
     * 生成UUID字符串
     *
     * @return
     */
    @UdfFunction("UUID_lz_udf")
    public static String uuid()
    {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 判断字符串 a中包含字符串b
     *
     * @param a 目标字符串
     * @param b 包含的字符串
     * @return true表示包含，false表示不包含
     */
    @UdfFunction("contain_lz_udf")
    public static boolean container(String a, String b)
    {
        return (a == null || b == null) ? false : a.contains(b);
    }

}
