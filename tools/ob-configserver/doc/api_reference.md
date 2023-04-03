# ob-configserver api refenence

For compatibility consideration, ob-configserver uses parameter `Action` to distinguish different type of requests

## Register OceanBase rootservice list

- request url: http://{vip_address}:{vip_port}/services
- request method: POST
- request parameters:

| name | type | required | typical value | description |
| --- | --- | --- | --- | --- |
| Action | String | Yes | ObRootServiceInfo |  |
| ObCluster | String | No | obcluster | ob cluster name |
| ObClusterId | int64 | No | 1 | ob cluster id |
| ObRegion | String | No | obcluster | ob cluster name, old format |
| ObRegionId | int64 | No | 1 | ob cluster id, old format |
| version | int | No | 1 | version supports 1 or 2, 2 means with standby ob cluster support |

request body:
```json
{
	"ObClusterId": 1,
	"ObRegionId": 1,
	"ObCluster": "obcluster",
	"ObRegion": "obcluster",
	"ReadonlyRsList": [],
	"RsList": [{
		"address": "1.1.1.1:2882",
		"role": "LEADER",
		"sql_port": 2881
	}],
	"Type": "PRIMARY",
	"timestamp": 1652419587417171
}
```

- response example:
```json
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": "successful",
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}
```

## Query Oceanbase rootservice list

- request url: http://{vip_address}:{vip_port}/services
- request method: GET
- request parameters:

| name | type | required | typical value | description |
| --- | --- | --- | --- | --- |
| Action | String | Yes | ObRootServiceInfo | |
| ObCluster | String | No | obcluster | ob cluster name |
| ObClusterId | int64 | No | 1 | ob cluster id |
| ObRegion | String | No | obcluster | ob cluster name, old format |
| ObRegionId | int64 | No | 1 | ob cluster id, old format |
| version | int | No | 1 | version supports 1 or 2, 2 means with standby ob cluster support |

- response example:
```json
# return one item, when version=1 or version=2 and ObClusterId specified
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": {
		"ObClusterId": 1,
		"ObRegionId": 1,
		"ObCluster": "obcluster",
		"ObRegion": "obcluster",
		"ReadonlyRsList": [],
		"RsList": [{
			"address": "1.1.1.1:2882",
			"role": "LEADER",
			"sql_port": 2881
		}],
		"Type": "PRIMARY",
		"timestamp": 1652419587417171
	},
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}

# return a list when version=2 and ObClusterId not specified
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": [{
		"ObClusterId": 1,
		"ObRegionId": 1,
		"ObCluster": "obcluster",
		"ObRegion": "obcluster",
		"ReadonlyRsList": [],
		"RsList": [{
			"address": "1.1.1.1:2882",
			"role": "LEADER",
			"sql_port": 2881
		}],
		"Type": "PRIMARY",
		"timestamp": 1652419587417171
	}, {
		"ObClusterId": 2,
		"ObRegionId": 2,
		"ObCluster": "obcluster",
		"ObRegion": "obcluster",
		"ReadonlyRsList": [],
		"RsList": [{
			"address": "2.2.2.2:2882",
			"role": "LEADER",
			"sql_port": 2881
		}],
		"Type": "STANDBY",
		"timestamp": 1652436572067984
	}],
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}
```
## Delete OceanBase rootservice info

- request url: http://{vip_address}:{vip_port}/services
- request method: DELETE
- request parameters:

| name | type | required | typical value | description |
| --- | --- | --- | --- | --- |
| Action | String | Yes | ObRootServiceInfo | |
| ObCluster | String | No | obcluster | ob cluster name |
| ObClusterId | int64 | No | 1 | ob cluster id |
| ObRegion | String | No | obcluster | ob cluster name, old format |
| ObRegionId | int64 | No | 1 | ob cluster id, old format |
| version | int | No | 1 | only version=2 is supported, and don't forget to specify ObClusterId |

- response example:
```json
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": "successful",
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}
```

## Query rootservice info of all OceanBase clusters

- request url: http://{vip_address}:{vip_port}/services
- request method: GET/POST
- request parameters:

| name | type | required | typical value | description |
| --- | --- | --- | --- | --- |
| Action | String | Yes | GetObProxyConfig | |
| VersionOnly | Boolean | No | false | only return version |

- response example:
```json
# return all info
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": {
		"ObProxyBinUrl": "http://1.1.1.1:8080/client?Action=GetObProxy",
		"ObProxyDatabaseInfo": {
			"DataBase": "***",
			"MetaDataBase": "http://1.1.1.1:8080/services?Action=ObRootServiceInfo&User_ID=alibaba&UID=admin&ObRegion=obdv1",
			"Password": "***",
			"User": "***"
		},
		"ObRootServiceInfoUrlList": [{
			"ObRegion": "obcluster",
			"ObRootServiceInfoUrl": "http://1.1.1.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster"
		}],
		"Version": "07c5563d293278097dc84e6b64ef6341"
	},
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}

# return version only
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": {
		"Version": "07c5563d293278097dc84e6b64ef6341"
	},
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}
```

## Query rootservice info of all OceanBase clusters in template format

- request url: http://{vip_address}:{vip_port}/services
- request method: GET/POST
- request parameters:

| name | type | required | typical value | description |
| --- | --- | --- | --- | --- |
| Action | String | Yes | GetObRootServiceInfoUrlTemplate | |
| VersionOnly | Boolean | No | false | only return version |

- response example:
```json
# return all info
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": {
		"ObProxyBinUrl": "http://1.1.1.1:8080/client?Action=GetObProxy",
		"ObProxyDatabaseInfo": {
			"DataBase": "***",
			"MetaDataBase": "http://1.1.1.1:8080/services?Action=ObRootServiceInfo&User_ID=alibaba&UID=admin&ObRegion=obdv1",
			"Password": "***",
			"User": "***"
		},
		"Version": "b34e6381994003c5d758890ededb82a4",
		"ObClusterList": ["obcluster"],
		"ObRootServiceInfoUrlTemplate": "http://1.1.1.1:8080/services?Action=ObRootServiceInfo&ObRegion=${ObRegion}",
		"ObRootServiceInfoUrlTemplateV2": "http://1.1.1.1:8080/services?Action=ObRootServiceInfo&version=2&ObCluster=${ObCluster}&ObClusterId=${OBClusterId}"
	},
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}

# versiononly
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": {
		"Version": "b34e6381994003c5d758890ededb82a4"
	},
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}
```

## Query idc and region info (empty implementation, just for compatibility)

- request url: http://{vip_address}:{vip_port}/services
- request method: GET/POST
- request parameters:

| name | type | required | typical value | description |
| --- | --- | --- | --- | --- |
| Action | String | Yes | ObIDCRegionInfo |  |
| ObCluster | String | No | obcluster | ob cluster name |
| ObClusterId | int64 | No | 1 | ob cluster id |
| ObRegion | String | No | obcluster | ob cluster name, old format |
| ObRegionId | int64 | No | 1 | ob cluster id, old format |
| version | int | No | 1 | version supports 1 or 2, 2 means with standby ob cluster support |


- response example:
```json
# return single item when version=1 or version=2 and ObClusterId specified
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": {
		"ObRegion": "obcluster",
		"ObRegionId": 2,
		"IDCList": [],
		"ReadonlyRsList": ""
	},
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}


# return a list when version=2 and ObClusterId not specified
{
	"Code": 200,
	"Message": "successful",
	"Success": true,
	"Data": [{
		"ObRegion": "obcluster",
		"ObRegionId": 1,
		"IDCList": [],
		"ReadonlyRsList": ""
	}, {
		"ObRegion": "obcluster",
		"ObRegionId": 2,
		"IDCList": [],
		"ReadonlyRsList": ""
	}],
	"Trace": "xxxx",
	"Server": "1.1.1.1",
	"Cost": 1
}
```
