package io.delta.flink.source;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;

public class VariableArgumentsProvider implements ArgumentsProvider,
    AnnotationConsumer<VariableSource> {

    private String variableName;

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context)
        throws Exception {
        return context.getTestClass()
            .map(this::getMethod)
            .map(this::invoke)
            .orElseThrow(() ->
                new IllegalArgumentException("Failed to load test arguments"));
    }

    @Override
    public void accept(VariableSource variableSource) {
        variableName = variableSource.value();
    }

    private Method getMethod(Class<?> clazz) {
        try {
            Method declaredMethod = clazz.getDeclaredMethod(variableName);
            declaredMethod.setAccessible(true);
            return declaredMethod;
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Stream<Arguments> invoke(Method method) {
        Object value = null;
        try {
            Object instance = method.getDeclaringClass().newInstance();
            value = method.invoke(instance);
        } catch (Exception ignored) {
            // ignore
        }

        return value == null ? null : (Stream<Arguments>) value;
    }
}
