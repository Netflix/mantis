# Running MantisWorker locally in IntelliJ IDE

1. Add project lombok plugin to IntelliJ
2. use VirtualMachineWorkerServiceLocalImpl instead of VirualMachineWorkerServiceMesosImpl in MantisWorker class
3. add sine-function libs(mantis-sdk/examples/sine-function/build/distributions/sine-function-1.0/lib/) to mantis-server-worker_main Module classpath
4. Bring up master in docker cluster with docker-compose
5. use the MantisWorker runConfiguration from mantisoss repo to run MantisWorker
