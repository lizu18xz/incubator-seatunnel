package org.apache.seatunnel.udf.generator;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @author lizu
 * @since 2022/4/24
 */
public class UdfUtil {

    private static final String PACKAGE = "org.apache.seatunnel.spark.udf.auto.func";
    private static final String PATH = "org/apache/seatunnel/spark/udf/auto/func";

    private ClassLoader classLoader;

    public UdfUtil() {
        this.classLoader = Thread.currentThread().getContextClassLoader();
    }

    public List<String> loadUdfClass() {
        List<String> classNames = new ArrayList<String>();
        try {
            Enumeration<URL> resources = classLoader.getResources(PATH);

            while (resources.hasMoreElements()) {
                URL next = resources.nextElement();
                String protocol = next.getProtocol();
                if ("file".equalsIgnoreCase(protocol)) {
                    String dest = URLDecoder.decode(next.getFile(), "UTF-8");
                    File src = new File(dest);
                    if (src.isDirectory()) {
                        File[] all = src.listFiles();
                        for (File one : all) {
                            if (one.isFile() && isClassFile(one.getName())) {
                                classNames.add(PACKAGE + "." + getClassName(one.getName()));
                            }
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return classNames;
    }

    private String getClassName(String name) {
        return name.substring(0, name.length() - 6);
    }


    private boolean isClassFile(String name) {
        return name.endsWith(".class");
    }

}
