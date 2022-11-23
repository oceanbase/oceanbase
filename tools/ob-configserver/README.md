# ob-configserver

## What is ob-configserver
Ob-configserver is a web application provides oceanbase metadata storage and query.

## How to build

To build ob-configserver requires go 1.16 or above

### build binary
You can build ob-configserver using the commands list below
```bash
# build debug version
make build

# build release version
make build-release

```
You will get the compiled binary file in folder bin

### build rpm
You can build a rpm package using the following command
```
cd {project_home}/rpm
bash ob-configserver-build.sh {project_home} ob-configserver 1
```

## How to run ob-configserver

### run binary directly
* copy the config.yaml file from etc/config.yaml and modify it to match the real environment

* start ob-configserver with the following command
```bash
bin/ob-configserver -c path_to_config_file
```

### install rpm package

* install rpm package
```bash
rpm -ivh ob-configserver-xxx-x.el7.rpm
```

after installation, the directory looks like this
```bash
.
├── bin
│   └── ob-configserver
├── conf
│   └── config.yaml
├── log
└── run
```

* modify config file

* start ob-configserver
```bash
bin/ob-configserver -c conf/config.yaml
```


## How to use ob-configserver

### config oceanbase to use ob-configserver
* config ob-configserver when observer startup
```bash
add obconfig_url='http://{vip_address}:{vip_port}/services?Action=ObRootServiceInfo&ObCluster={ob_cluster_name}' in start command, specify with -o
```

* config ob-configserver when observer already starts using sql
```sql
# run the following sql using root user in tenant sys
alter system set obconfig_url = 'http://{vip_address}:{vip_port}/services?Action=ObRootServiceInfo&ObCluster={ob_cluster_name}'

```

### config obproxy to use ob-configserver
* config ob-configserver when obproxy startup 
```bash
add obproxy_config_server_url='http://{vip_address}:{vip_port}/services?Action=GetObProxyConfig' in start command specify with -o
```

* config ob-configserver when obproxy already starts using sql
```sql
# run the following sql using root@proxysys
alter proxyconfig set obproxy_config_server_url='http://{vip_address}:{vip_port}/services?Action=GetObProxyConfig'

```

## API reference
[api reference](doc/api_reference.md)

