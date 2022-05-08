package org.apache.seatunnel.udf.generator;

import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.processing.Filer;

/**
 * @author lizu
 * @since 2022/5/8
 */
public class FlinkGenerator extends AbstractGenerator {

    @Override
    protected String generalCode(Class<?> cls, Method method) {
        return null;
    }

    @Override
    protected void genJavaFile(Class<?> cls, List<String> list, Filer filer) {

    }
}
