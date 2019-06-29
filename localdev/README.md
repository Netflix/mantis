## Run the mantis cluster locally on a vagrant VM.

Currently supported linux versions include:

*	CentOS 7.0

## Setup instructions
1. Install Vagrant(https://www.vagrantup.com/) and VirtualBox(https://www.virtualbox.org/)
2. git clone the Mantis repo to ~/Projects (can change clone directory in <path to mantis source>/mantis-localdev/vagrant/centos/Vagrantfile to point to a different location)
3. cd <path to mantis source>/mantis-localdev/vagrant/centos
4. vagrant up
5. add "192.168.34.10 mantis-node1" to your /etc/hosts (This allows mesos UI to fetch console logs for any mantis jobs running on mesos-slave)

The above steps will
- spin up a CentOS VM in VirtualBox
- install zookeeper
- install and run mesos
- run the mantis-master (logs can be found at /home/vagrant/mantis-master.log after 'vagrant ssh')
- build the sine-function example Mantis job and submit it to running mantis master (the sine-function output can be stream via SSE using 'curl http://192.168.43.10:31002')

Once the VM is provisioned, you can access the mantis-master API at http://192.168.34.10:7101/help and mesos UI at http://192.168.34.10:5050/

## Vagrant useful commands

1. SSH into the VM
> vagrant ssh

2. suspend the VM
> vagrant suspend

3. Resume a suspended VM / make updates to bootstrap.sh take effect
> vagrant reload --provision

(Note: `vagrant resume` is not fully supported yet, need to add a service manager like runit to manage the mantis-master process)
