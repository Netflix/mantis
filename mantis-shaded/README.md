## Working with IntelliJ

If you are using IntelliJ, you will need to manually add a dependency to the shadow jar due to this issue:

https://github.com/johnrengelman/shadow/issues/264

First build the jar under `mantis-shaded`. The jar file should be created under `mantis-shaded/build/libs`. Then in IntelliJ under `Project Settings` -> `Libraries`, add a dependency to the jar file.

