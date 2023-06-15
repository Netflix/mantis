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

package io.mantisrx.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MachineDefinitionTest {
  @Test
  public void testMachineDefinition() {
    MachineDefinition md1 = new MachineDefinition(2, 2, 2, 2, 2);
    MachineDefinition md2 = new MachineDefinition(1, 1, 1, 1, 1);
    assertTrue(md1.canFit(md2));
    assertFalse(md2.canFit(md1));

    MachineDefinition md3 = new MachineDefinition(2, 2, 2, 2, 2);
    MachineDefinition md4 = new MachineDefinition(3, 1, 1, 1, 1);
    assertFalse(md3.canFit(md4));
    assertFalse(md4.canFit(md3));

    MachineDefinition md5 = new MachineDefinition(2, 2, 2, 2, 2);
    MachineDefinition md6 = new MachineDefinition(2, 3, 1, 1, 1);
    assertFalse(md5.canFit(md6));
    assertFalse(md6.canFit(md5));

    MachineDefinition md7 = new MachineDefinition(2, 2, 2, 2, 2);
    MachineDefinition md8 = new MachineDefinition(2, 2, 2, 2, 2);
    assertTrue(md7.canFit(md8));
    assertTrue(md8.canFit(md7));
  }
}
