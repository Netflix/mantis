/*
 * Copyright 2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.common.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public final class FilteredDeserializer {

    private static final String JAVA_LANG_PACKAGE = "java.lang";
    private static final String JAVA_UTIL_PACKAGE = "java.util";

    private static final ObjectInputFilter STRING_MAP_FILTER = classFilter(FilteredDeserializer::isAllowedInStringMap);

    private static final ObjectInputFilter THROWABLE_FILTER = classFilter(FilteredDeserializer::isAllowedInThrowable);

    private FilteredDeserializer() {
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> readStringMap(byte[] bytes) {
        return (Map<String, String>) readFiltered(bytes, STRING_MAP_FILTER, Map.class);
    }

    public static Throwable readThrowable(byte[] bytes) {
        return (Throwable) readFiltered(bytes, THROWABLE_FILTER, Throwable.class);
    }

    private static Object readFiltered(byte[] bytes, ObjectInputFilter filter, Class<?> expectedType) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            in.setObjectInputFilter(filter);
            Object value = in.readObject();
            return expectedType.cast(value);
        } catch (IOException | ClassNotFoundException | ClassCastException e) {
            throw new RuntimeException(e);
        }
    }

    private static ObjectInputFilter classFilter(Predicate<Class<?>> isAllowed) {
        return info -> {
            Class<?> serialClass = info.serialClass();
            if (serialClass == null) {
                return ObjectInputFilter.Status.UNDECIDED;
            }
            return isAllowed.test(elementTypeOf(serialClass))
                ? ObjectInputFilter.Status.ALLOWED
                : ObjectInputFilter.Status.REJECTED;
        };
    }

    private static Class<?> elementTypeOf(Class<?> type) {
        Class<?> elementType = type;
        while (elementType.isArray()) {
            elementType = elementType.getComponentType();
        }
        return elementType;
    }

    private static boolean isAllowedInStringMap(Class<?> type) {
        return type.isPrimitive()
            || isInPackage(type, JAVA_LANG_PACKAGE)
            || isJavaUtilImplementationOf(Map.class, type)
            || isJavaUtilSerializationSupport(type);
    }

    private static boolean isAllowedInThrowable(Class<?> type) {
        return type.isPrimitive()
            || isInPackage(type, JAVA_LANG_PACKAGE)
            || Throwable.class.isAssignableFrom(type)
            || isJavaUtilImplementationOf(List.class, type)
            || isJavaUtilSerializationSupport(type);
    }

    private static boolean isJavaUtilImplementationOf(Class<?> parentType, Class<?> type) {
        return isInPackage(type, JAVA_UTIL_PACKAGE) && parentType.isAssignableFrom(type);
    }

    private static boolean isJavaUtilSerializationSupport(Class<?> type) {
        return type == Map.Entry.class
            || type.getName().equals("java.util.CollSer")
            || type.getName().equals("java.util.Collections$UnmodifiableCollection");
    }

    private static boolean isInPackage(Class<?> type, String packageName) {
        return type.getPackageName().equals(packageName);
    }
}
