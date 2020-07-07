If you are using IntelliJ, you will need to manually add the dependency to the shadow jar due to this issue:

https://github.com/johnrengelman/shadow/issues/264

Under `Module Settings` for `mantis-common/main`, add a dependency to the folder `mantis/mantis-shaded/build/libs`.

