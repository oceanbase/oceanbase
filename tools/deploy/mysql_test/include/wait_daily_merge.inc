#--disable_query_log
#--disable_result_log
connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;
let $__i__= 600;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__forzen_version__ = query_get_value(select value from oceanbase.__all_zone where name = 'frozen_version', value, 1);
    let $__merged_version__ = query_get_value(select value from oceanbase.__all_zone where name = 'last_merged_version' and zone = '', value, 1);
    if($__forzen_version__ == $__merged_version__)
    {
        let $__i__ = -5;
    }
}

###判断集群状态，先注释掉
#let $__j__= 180;
#while($__j__ > 0)
#{
#    sleep 1;
#    dec $__j__;
#    let $__global_stat__ = query_get_value(select comment from oceanbase.__all_cluster_stat where name='global_stat',comment, 1);
#    if($__global_stat__ == NORMAL)
#    {
#        let $__j__ = -5;
#    }
#}

#if($__j__ != -5)
#{
#    --echo major freeze failed
#}

if($__i__ != -5)
{
    --echo major freeze failed
}
sleep 2;

disconnect obsys_ydm;
connection default;
--enable_query_log
--enable_result_log
