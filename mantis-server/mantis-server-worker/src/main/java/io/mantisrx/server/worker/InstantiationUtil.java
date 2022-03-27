/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.server.worker;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

/**
 * This code is a subset of InstantiationUtil class in Flink.
 * {@see <a href="https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/InstantiationUtil.java#L419">InstantiationUtil</a>}
 */
public class InstantiationUtil {
    /**
     * Creates a new instance of the given class.
     *
     * @param <T>   The generic type of the class.
     * @param clazz The class to instantiate.
     * @return An instance of the given class.
     * @throws RuntimeException Thrown, if the class could not be instantiated. The exception
     *                          contains a detailed message about the reason why the instantiation failed.
     */
    public static <T> T instantiate(Class<T> clazz) {
        if (clazz == null) {
            throw new NullPointerException();
        }

        // try to instantiate the class
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException iex) {
            // check for the common problem causes
            checkForInstantiation(clazz);

            // here we are, if non of the common causes was the problem. then the error was
            // most likely an exception in the constructor or field initialization
            throw new RuntimeException(
                    "Could not instantiate type '"
                            + clazz.getName()
                            + "' due to an unspecified exception: "
                            + iex.getMessage(),
                    iex);
        } catch (Throwable t) {
            String message = t.getMessage();
            throw new RuntimeException(
                    "Could not instantiate type '"
                            + clazz.getName()
                            + "' Most likely the constructor (or a member variable initialization) threw an exception"
                            + (message == null ? "." : ": " + message),
                    t);
        }
    }

    /**
     * Creates a new instance of the given class name and type using the provided {@link
     * ClassLoader}.
     *
     * @param className   of the class to load
     * @param targetType  type of the instantiated class
     * @param classLoader to use for loading the class
     * @param <T>         type of the instantiated class
     * @return Instance of the given class name
     * @throws Exception if the class could not be found
     */
    public static <T> T instantiate(
            final String className, final Class<T> targetType, final ClassLoader classLoader)
            throws Exception {
        final Class<? extends T> clazz;
        try {
            clazz = Class.forName(className, false, classLoader).asSubclass(targetType);
        } catch (ClassNotFoundException e) {
            throw new Exception(
                    String.format(
                            "Could not instantiate class '%s' of type '%s'. Please make sure that this class is on your class path.",
                            className, targetType.getName()),
                    e);
        }

        return instantiate(clazz);
    }

    /**
     * Performs a standard check whether the class can be instantiated by {@code
     * Class#newInstance()}.
     *
     * @param clazz The class to check.
     * @throws RuntimeException Thrown, if the class cannot be instantiated by {@code
     *                          Class#newInstance()}.
     */
    public static void checkForInstantiation(Class<?> clazz) {
        final String errorMessage = checkForInstantiationError(clazz);

        if (errorMessage != null) {
            throw new RuntimeException(
                    "The class '" + clazz.getName() + "' is not instantiable: " + errorMessage);
        }
    }

    public static String checkForInstantiationError(Class<?> clazz) {
        if (!isPublic(clazz)) {
            return "The class is not public.";
        } else if (clazz.isArray()) {
            return "The class is an array. An array cannot be simply instantiated, as with a parameterless constructor.";
        } else if (!isProperClass(clazz)) {
            return "The class is not a proper class. It is either abstract, an interface, or a primitive type.";
        } else if (isNonStaticInnerClass(clazz)) {
            return "The class is an inner class, but not statically accessible.";
        } else if (!hasPublicNullaryConstructor(clazz)) {
            return "The class has no (implicit) public nullary constructor, i.e. a constructor without arguments.";
        } else {
            return null;
        }
    }

    /**
     * Checks, whether the given class is public.
     *
     * @param clazz The class to check.
     * @return True, if the class is public, false if not.
     */
    public static boolean isPublic(Class<?> clazz) {
        return Modifier.isPublic(clazz.getModifiers());
    }

    /**
     * Checks, whether the class is a proper class, i.e. not abstract or an interface, and not a
     * primitive type.
     *
     * @param clazz The class to check.
     * @return True, if the class is a proper class, false otherwise.
     */
    public static boolean isProperClass(Class<?> clazz) {
        int mods = clazz.getModifiers();
        return !(Modifier.isAbstract(mods)
                || Modifier.isInterface(mods)
                || Modifier.isNative(mods));
    }

    /**
     * Checks, whether the class is an inner class that is not statically accessible. That is
     * especially true for anonymous inner classes.
     *
     * @param clazz The class to check.
     * @return True, if the class is a non-statically accessible inner class.
     */
    public static boolean isNonStaticInnerClass(Class<?> clazz) {
        return clazz.getEnclosingClass() != null
                && (clazz.getDeclaringClass() == null || !Modifier.isStatic(clazz.getModifiers()));
    }

    /**
     * Checks, whether the given class has a public nullary constructor.
     *
     * @param clazz The class to check.
     * @return True, if the class has a public nullary constructor, false if not.
     */
    public static boolean hasPublicNullaryConstructor(Class<?> clazz) {
        Constructor<?>[] constructors = clazz.getConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getParameterTypes().length == 0
                    && Modifier.isPublic(constructor.getModifiers())) {
                return true;
            }
        }
        return false;
    }

}
