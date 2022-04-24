/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.spark.udf.auto;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.seatunnel.udf.constants.SupportType;
import org.apache.seatunnel.udf.constants.UdfName;
import org.apache.seatunnel.udf.constants.UdfSupport;
import org.apache.seatunnel.udf.generator.UdfUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF10;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.api.java.UDF8;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UdfSupport(type = SupportType.SPARK, packageName = "org.apache.seatunnel.spark.udf.auto.func")
public class SparkUdfRegedit {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkUdfRegedit.class);

    private SparkSession sparkSession;

    public SparkUdfRegedit(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public void registerUdf() {
        try {
            UdfUtil udfUtil = new UdfUtil();
            List<String> strings = udfUtil.loadUdfClass();
            for (String next : strings) {
                Class cls = Class.forName(next);
                regedit(cls);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void regedit(Class<?> cls)
        throws IllegalAccessException,
        InstantiationException {
        if (!cls.isAnnotationPresent(UdfName.class)) {
            return;
        }
        UDFRegistration udf = sparkSession.udf();
        UdfName name = cls.getAnnotation(UdfName.class);
        DataType dt = getDataType(cls);
        LOGGER.info("regedit udf {},{}", name.value(), cls.getName());
        if (UDF0.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF0) (cls.newInstance()), dt);
        } else if (UDF1.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF1) (cls.newInstance()), dt);
        } else if (UDF2.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF2) (cls.newInstance()), dt);
        } else if (UDF3.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF3) (cls.newInstance()), dt);
        } else if (UDF4.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF4) (cls.newInstance()), dt);
        } else if (UDF5.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF5) (cls.newInstance()), dt);
        } else if (UDF6.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF6) (cls.newInstance()), dt);
        } else if (UDF7.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF7) (cls.newInstance()), dt);
        } else if (UDF8.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF8) (cls.newInstance()), dt);
        } else if (UDF9.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF9) (cls.newInstance()), dt);
        } else if (UDF10.class.isAssignableFrom(cls)) {
            udf.register(name.value(), (UDF10) (cls.newInstance()), dt);
        }
    }

    private DataType getDataType(Class cls) {
        Type[] all = cls.getGenericInterfaces();
        ParameterizedType aa = (ParameterizedType) (all[all.length - 1]);
        all = aa.getActualTypeArguments();
        return JavaTypeInference.inferDataType((Class) all[all.length - 1])._1;
    }

}
