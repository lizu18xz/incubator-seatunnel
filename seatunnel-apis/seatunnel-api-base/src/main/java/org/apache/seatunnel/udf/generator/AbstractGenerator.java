package org.apache.seatunnel.udf.generator;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.processing.Filer;
import org.apache.seatunnel.udf.constants.SupportType;
import org.apache.seatunnel.udf.constants.UdfFunction;
import org.apache.seatunnel.udf.func.StringFunction;

/**
 * @author lizu
 * @since 2022/5/7
 */
public abstract class AbstractGenerator {

    public void generateCode(Filer filer) {
        processOneClass(StringFunction.class, filer);
    }

    private void processOneClass(Class<?> cls, Filer filer) {
        List<String> list = new LinkedList<>();
        Method[] declaredMethods = cls.getDeclaredMethods();
        for (Method method : declaredMethods) {
            // 必须是public static的函数
            boolean isPublicStatic = Modifier.isStatic(method.getModifiers())
                && Modifier.isPublic(method.getModifiers());
            if (isPublicStatic && isUdfMethod(method)) {
                list.add(generalCode(cls, method));
            }
        }
        genJavaFile(cls, list, filer);
    }

    protected abstract String generalCode(Class<?> cls, Method method);

    protected abstract void genJavaFile(Class<?> cls, List<String> list, Filer filer);

    protected boolean isUdfMethod(Method method) {
        UdfFunction fu = method.getAnnotation(UdfFunction.class);
        return fu != null && isSupport(fu.type());
    }

    protected boolean isSupport(SupportType type) {
        return SupportType.ALL == type || SupportType.SPARK == type;
    }

}
