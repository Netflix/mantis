/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.runtime.executor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.ParameterUtils;
import io.mantisrx.runtime.parameter.type.EnumCSVParameter;
import io.mantisrx.runtime.parameter.type.EnumParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import org.junit.Assert;
import org.junit.Test;


public class ParameterDefinitionTest {

    @Test
    public void emptyCheck() {

        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        Map<String, Parameter> parameters = new HashMap<>();

        ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingRequiredParameter() {

        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        parameterDefinitions.put("foo", new StringParameter()
                .name("foo")
                .required()
                .validator(Validators.<String>alwaysPass())
                .build());

        Map<String, Parameter> parameters = new HashMap<>();
        ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
    }

    @Test
    public void singleRequiredParameterCheck() {

        Map<String, Object> parameterState = new HashMap<>();
        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        parameterDefinitions.put("foo", new StringParameter()
                .name("foo")
                .required()
                .validator(Validators.<String>alwaysPass())
                .build());

        Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("foo", new Parameter("foo", "test"));
        parameterState = ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
        Assert.assertEquals(1, parameterState.size());
        Assert.assertEquals(true, parameterState.containsKey("foo"));
        Assert.assertEquals("test", parameterState.get("foo"));
    }

    @Test
    public void singleRequiredSingleNonRequiredParameterCheck() {

        Map<String, Object> parameterState = new HashMap<>();
        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        parameterDefinitions.put("required", new StringParameter()
                .name("required")
                .required()
                .validator(Validators.<String>alwaysPass())
                .build());
        parameterDefinitions.put("nonRequired", new StringParameter()
                .name("nonRequired")
                .validator(Validators.<String>alwaysPass())
                .build());

        Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("required", new Parameter("required", "test"));
        parameterState = ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
        Assert.assertEquals(1, parameterState.size());
        Assert.assertEquals(true, parameterState.containsKey("required"));
        Assert.assertEquals("test", parameterState.get("required"));
    }

    @Test
    public void testEnumParameter() {
        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        parameterDefinitions.put("foo", new EnumParameter<>(TestEnum.class)
                .name("foo")
                .required()
                .validator(Validators.alwaysPass())
                .build());

        Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("foo", new Parameter("foo", "A"));
        Map<String, Object> parameterState = ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
        Assert.assertEquals(TestEnum.A, parameterState.get("foo"));
    }

    ;

    @Test
    public void testEnumCSVParameter() {
        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        parameterDefinitions.put("foo", new EnumCSVParameter<>(TestEnum.class)
                .name("foo")
                .required()
                .validator(Validators.alwaysPass())
                .build());

        Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("foo", new Parameter("foo", "   A   ,  C   "));
        Map<String, Object> parameterState = ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
        EnumSet<TestEnum> foo = (EnumSet<TestEnum>) parameterState.get("foo");
        Assert.assertTrue(foo.contains(TestEnum.A));
        Assert.assertTrue(foo.contains(TestEnum.C));
        Assert.assertFalse(foo.contains(TestEnum.B));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyEnumCSVListValidation() {
        Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();
        parameterDefinitions.put("foo", new EnumCSVParameter<>(TestEnum.class)
                .name("foo")
                .required()
                .validator(Validators.notNullOrEmptyEnumCSV())
                .build());

        Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("foo", new Parameter("foo", "  "));
        ParameterUtils.checkThenCreateState(parameterDefinitions, parameters);
    }

    enum TestEnum {A, B, C}
}
