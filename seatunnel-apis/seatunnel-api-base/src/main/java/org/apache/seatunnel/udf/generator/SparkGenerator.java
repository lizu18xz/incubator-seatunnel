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

package org.apache.seatunnel.udf.generator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.udf.constants.UdfFunction;

/**
 * @author lizu
 */
public class SparkGenerator extends AbstractGenerator {

    /**
     * 动态生成的包路径
     */
    private String packageName;

    private Messager messager;

    private Map<String, String> typeMap = Maps.newHashMap();

    private int classIndex = 0;

    private int paramMaxLen = 10;

    public SparkGenerator(String packageName, Messager messager) {
        this.packageName = packageName;
        this.messager = messager;

        typeMap.put("int", "Integer");
        typeMap.put("boolean", "Boolean");
        typeMap.put("short", "Short");
        typeMap.put("byte", "Byte");
        typeMap.put("long", "Long");
        typeMap.put("float", "Float");
        typeMap.put("double", "Double");
        typeMap.put(String.class.getName(), "String");
        typeMap.put(Object.class.getName(), "Object");
    }


    @Override
    protected String generalCode(Class<?> cls, Method method) {
        UdfFunction fu = method.getAnnotation(UdfFunction.class);
        Class<?>[] para = method.getParameterTypes();
        Class<?> rt = method.getReturnType();
        //动态生成的类名称
        String name = "SeaTunnelUDF" + classIndex++;
        //注册到spark中的函数名称
        String udfName = StringUtils.isNotBlank(fu.value()) ? fu.value() : method.getName();
        //返回类型
        int len = para.length;
        String output = getTypeClass(rt);
        List<String> typeList = Lists.newArrayList();
        String type = "";
        List<String> paramList = Lists.newArrayList();
        List<String> valueList = Lists.newArrayList();
        if (para.length != 0) {
            for (Class<?> one : para) {
                typeList.add(getTypeClass(one));
            }
            type = StringUtils.join(typeList, ",") + ",";
            for (int i = 0; i < para.length; i++) {
                Class<?> one = para[i];
                valueList.add("o" + (i + 1));
                paramList.add(getTypeClass(one) + " " + "o" + (i + 1));
            }
        }
        //生成的类的动态表达式
        String methodName = cls.getSimpleName() + "." + method.getName();
        String expression = methodName + "(" + StringUtils.join(valueList, ",") + ")";
        String code = getTemplate(expression, null)
            .replace("${udfName}", udfName)
            .replace("${name}", name)
            .replace("${len}", String.valueOf(len))
            .replace("${type}", type)
            .replace("${output}", output)
            .replace("${para}", StringUtils.join(paramList, ","));
        messager.printMessage(Diagnostic.Kind.NOTE, "generate code:" + code);
        return code;
    }


    @Override
    protected void genJavaFile(Class<?> cls, List<String> list, Filer filer) {
        try {
            String name = cls.getSimpleName() + "UDF";
            JavaFileObject sourceFile = filer.createSourceFile(packageName + "." + name);
            try (Writer writer = sourceFile.openWriter()) {
                writer.write("package org.apache.seatunnel.spark.udf.auto.func;");
                writer.write("import org.apache.seatunnel.udf.func.*;");
                writer.write("import org.apache.seatunnel.udf.constants.UdfName;");
                for (int i = 0; i < paramMaxLen; i++) {
                    writer.write("import org.apache.spark.sql.api.java.UDF" + i + ";");
                }
                writer.write("public class " + name + "{");
                for (String code : list) {
                    writer.write(code);
                }
                writer.write("}");
                writer.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    protected String getTypeClass(Class<?> src) {
        return typeMap.getOrDefault(src.getName(), src.getName());
    }

    /**
     * spark UDF模板
     */
    private String getTemplate(String expression, List<String> extraPackagesStrList) {
        StringBuffer sb = new StringBuffer();
        if (extraPackagesStrList != null) {
            for (String extraPackagesStr : extraPackagesStrList) {
                if (StringUtils.isNotEmpty(extraPackagesStr)) {
                    sb.append(extraPackagesStr);
                }
            }
        }

        sb.append("@UdfName(\"${udfName}\")");
        sb.append("public static class ${name} implements UDF${len}<${type}${output}>").append("{");
        sb.append("public ${output} call(${para}) throws Exception {");
        sb.append("return ");
        sb.append(expression).append(";");
        sb.append("}}");
        return sb.toString();
    }


}
