# Abstract

In order to easily read the code of OceanBase, we suggest using one IDE which is easily index the symbols of OceanBase. In Windows, we recommend `Souce Insight` can be used, and in Mac or Linux, we recommend that `VSCode + ccls` can be used to read the oceanbase code. Due to it is very easy to use `Source Ingisht`, so this document skip introduction how to use `Souce Insight`.

This document introduce how to setup `VSCode + ccls`, which is very convenient to read the code of OceanBase. [ccls](https://github.com/MaskRay/ccls) is based on [cquery](https://github.com/jacobdufault/cquery), which is one of C/C++/Objective-C  [LSP](https://en.wikipedia.org/wiki/Language_Server_Protocol)s (In one word, LSP is used to provide programming language-specific features like code completion, syntax highlighting and marking of warnings and errors, as well as refactoring routines. ).

The number of OceanBase code is pretty huge and OceanBase can't be compiled under Mac or Windows, so we recommend that download the code on the remote server, and start VSCode to access the code under the remote server. 

# config ccls on remote server

**Attention**
The following `/path/to` just means the path example, please replace it with your real path. 

## Introduction

In the C/C++ LSP domain, the famous tools are clangd and ccls. Here we recommend ccls, because:

1. The speed of building index of ccls is slower than that of clangd, but after building, the speed of accessing index of ccls is faster than that of clangd. 
2. Unity building doesn't be supported by clangd, but OceanBase is being built by unity, failed to build index through compile_commands.json by clangd. 

**NOTE**:

1. Here are how to setup ccls in CentOS7, please refer to [ccls build wiki](https://github.com/MaskRay/ccls/wiki/Build) for more details.
2. If there are no special version, please do as the following instructions. 

# ccls Installation

Install ccls on centos

> NOTE: if you don't have the permission for `yum`, please use `sudo yum ...` instead.

```bash
yum install epel-release
yum install snapd # On centos8: yum install snapd --nobest 
systemctl enable --now snapd.socket
ln -s /var/lib/snapd/snap /snap
snap install ccls --classic
```

And then add the command below into your env source file, such as '~/.bashrc' or '~/.bash_profile'

```bash
export PATH=/var/lib/snapd/snap/bin:$PATH
```

Now, refresh your environment like this:

```bash
source ~/.bashrc   # or
source ~/.bash_profile
```

Install ccls on centos8

TODO

Install ccls on ubuntu

```bash
apt-get -y install ccls
```

> NOTE: If you don't have the permission, please use `sudo` as the command prefix.

check the installation

You can run the command below to check whether the installation was success.

```bash
ccls --version
```

# VSCode configuration

## Remote Plugin

Once the source code has been located in the remote machine, it is easy to setup debugging environment in remote machine. At the same time, the application can be run faster because remote machine is more powerful.  User can easily access the source code on the remote machine even when something is wrong with the network, just wait reload after reconnect the remote server. 

### installation

Download and install the Remote plugin from the VSCode extension store.

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672226833170-8961a1e0-f3e0-46c4-9f06-43cb9c2f77ca.png)

### Usage

**NOTE**：Make sure the connection between the local machine and the remote machine is fine. 
After installation the plugin, there is one icon in the left bottom corner of VSCode. 

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672226909238-32d44036-7899-4657-a4b7-beedd8ae5ca4.png)

Press the icon and select `Connect to Host`, or press shortkey `ctrl+shift+p` and select `Remote-SSH:Connect to Host`:

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672226980519-43e24803-1339-4f74-8535-17f74745aa66.png)

Input user@remote_ip in the input frame, VSCode will create one new window, please input password in the new window:

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370147275-84803eae-af92-4fbd-98c6-109874d9c976.png)

After input the password, VSCode will connect to the remote server, and it is ready to open the remote machine's file or directory. 

If you want to use the specific port, please choose `Add New SSH Host`, then input ssh command, then choose one configuration file to store the ssh configuration. 

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370296656-9e259c94-e273-4c0b-ab8d-b4e5ab81e4fd.png)

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370232586-90efcd62-03d8-4465-b18c-cc8c009f98cf.png)
After that, the configured machines can be found in the `Connect to Host`. 

Password need to be input everytime. If you want to skip this action, please configure SSH security login with credential. 

## C/C++ plugin

C/C++ plugin can be download and installed in VSCode extension store in the case of simple scenarios:

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370378234-b1d264f7-f34a-47e0-b7aa-5b352af9e3dd.png)
C/C++ plugin can automatically code completion and syntax highlighting, but this plugin failed to build index for OceanBase, it is hard to jump the symbol of OceanBase. 

## ccls plugin

### Install ccls plugin

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370482529-0007fae8-8e24-42ed-b14b-e163f89d40a0.png)

> if ccls will be used, it suggest to uninstall C/C++ plugin. 

### Configure ccls plugin

1. press the setting icond and choose **Extension Settings**

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370602812-710cb0aa-dc18-44f2-8b65-7291412758b8.png)

2. Set config ccls.index.threads. CCLS uses 80% of the system cpu cores as the parallelism in default. We can search `threads` in vscode config page and set the number like below.

> As default, oceanbase built in unity mode and it costs more memory than usual case. The system maybe hangs if the parallelism is too high such as 8C 16G system.

![image](https://github.com/oceanbase/oceanbase/assets/5187215/b9fb4dff-79e8-4785-902b-dea8436f2f64)

## Usage

1. git clone the source code from [https://github.com/oceanbase/oceanbase](https://github.com/oceanbase/oceanbase)
2. Run the command below to generate `compile_commands.json`
   
   ```bash
   bash build.sh ccls --init
   ```

After that, compile_commands.json can be found in the directory of code_path_of_oceanbase.

After finish previous steps, please restart VSCode, the building index precedure can be found at the bottom of VSCode:

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370872215-a995a681-7720-43bb-b690-dab8a11d7fc7.png)

After finish building index, the function's reference and class member can be easily found for any opened file as the following example:

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1672370997746-11beb619-56c5-46ad-88a3-12ead1780109.png?x-oss-process=image%2Fresize%2Cw_674%2Climit_0)

Recommend ccls shortkey settings:

![image](https://cdn.nlark.com/yuque/0/2022/png/106206/1672371098121-0ab7249c-a492-4031-a665-15c0e4e53539.png?x-oss-process=image%2Fresize%2Cw_674%2Climit_0)

![image](https://cdn.nlark.com/yuque/0/2022/png/106206/1672371282152-9295a833-b8b4-4a40-ac60-9a88490007b8.png)
