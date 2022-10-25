# Mantis-runtime-loader

This module is used to host loader classes used by mantis worker (e.g. TaskExecutor) to load given job instance's runtime code and user code.
It's important to keep the isolation between worker process classes and the runtime classes to avoid dependency conflicts.