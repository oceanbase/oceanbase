1. 基本的使用命令行为
./ob_admin -h127.0.0.1 -p28204 -sintl (-ssm) -mbkmi(-mlocal) your_command
其中(-s 指的就是ssl模式)，-sintl指的是采用的是国际加密协议，-ssm指的是采用的国密加密协议(暂不支持),
(-m指的是加密证书，私钥等的获取方式), -mbkmi指的是通过bkmi获取CA证书，证书和私钥，-mlocal指的是本地文件模式
2.使用方式
根据OB集群的加密方式来使用，
(1)当采用本地文件模式时，将observer工作目录的 wallet/ 文件夹拷贝至ob_admin的同级目录下，执行
./ob_admin -h127.0.0.1 -p28204 -sintl (-ssm) -mlocal your_command
即可
(2)当采用bkmi模式时，新建一个名为obadmin_ssl_bkmi.cfg 的文件放在ob_admin同级目录下，将bkmi模式的相关配置内容粘贴到
文件中，取两个花括号间内容, 见本目录下的 obadmin_ssl_bkmi.cfg 文件，然后执行
./ob_admin -h127.0.0.1 -p28204 -sintl (-ssm) -mbkmi your_command
即可,拷贝到obadmin_ssl_bkmi.cfg文件内的内容如下，需要包括两个花括号：
{
"ssl_mode":"bkmi",
"kms_host":"",
"root_cert":"",
"private_key":"",
"PRIVATE_KEY_PHRASE":"123456",
"SCENE":"ANT",
"CALLER":"ob_ssl_crypt",
"CERT_NAME":"ob_yanhua_test1",
"PRIVATE_KEY_NAME":"ob_yanhua_test1-app-id-authn-RSA-2048-private",
"KEY_VERSION":"1"
}
