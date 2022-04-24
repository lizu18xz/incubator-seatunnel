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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import org.apache.seatunnel.udf.constants.SupportType;
import org.apache.seatunnel.udf.constants.UdfSupport;

@AutoService(Processor.class)
@SupportedAnnotationTypes("org.apache.seatunnel.udf.constants.UdfSupport")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class GeneralUdfClassProcess extends AbstractProcessor {

    private Filer mfile;

    private Messager messager;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        mfile = processingEnv.getFiler();
        messager = processingEnv.getMessager();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<? extends Element> set = roundEnv.getElementsAnnotatedWith(UdfSupport.class);
        if (set.isEmpty())
        {
            return false;
        }
        try
        {
            messager.printMessage(Diagnostic.Kind.NOTE, "begin to generate code");
            for (Element next : set)
            {
                UdfSupport fun = next.getAnnotation(UdfSupport.class);
                if (SupportType.FLINK == fun.type())
                {
                }
                else if (SupportType.SPARK == fun.type())
                {
                    new SparkGenerator(fun.packageName(),messager).generateCode(mfile);
                }
                else
                {
                    throw new IllegalArgumentException("unknow type " + fun.type());
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return true;
    }

}
