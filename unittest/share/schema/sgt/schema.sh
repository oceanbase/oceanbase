
./a.out 100  > $$.sql 2>&1
grep -v ^# $$.sql  > $$_1.sql
sys="root@sys"
tenant="tu$$@t$$"
my="mysql -c -h 10.125.224.5 -P 11301"
zone_name='zone1';

$my -uroot -e "set global ob_query_timeout=1000000000;"
cu="CREATE RESOURCE UNIT u$$ max_cpu=2, memory_size=5368709120;"
cr="CREATE RESOURCE POOL p$$ unit = 'u$$', unit_num = 1, zone_list =('$zone_name');"
ct="CREATE TENANT t$$ replica_num = 1, primary_zone='$zone_name', resource_pool_list=('p$$') set ob_tcp_invited_nodes='%';"
$my -u$sys -e "$cu"
$my -u$sys -e "$cr"
$my -u$sys -e "$ct"
$my -uroot@t$$ -e "GRANT ALL ON *.* TO tu$$ IDENTIFIED BY 'tu$$'"
for ((i=0; i<3; i++))
do
$my -utu$$@t$$ -ptu$$ < $$_1.sql
$my -uroot -e "alter system major freeze;"
done

$my -u$sys -e "DROP TENANT t$$"
$my -u$sys -e "DROP RESOURCE POOL p$$"
$my -u$sys -e "DROP RESOURCE UNIT u$$"
