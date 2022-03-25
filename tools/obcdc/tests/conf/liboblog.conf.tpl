cluster_db_name=oceanbase
log_level=ALL.*:INFO;SHARE.SCHEMA:WARN
#log_level=ALL.*:DEBUG;SHARE.SCHEMA:WARN
#log_level=ALL.*:DEBUG;TLOG.FETCHER:INFO;TLOG.FORMATTER:INFO;SHARE.SCHEMA:WARN

tb_white_list=*.*.*
tb_white_list=sys.oblog*.*|oblog_tt.*.*
tb_black_list=*.*.*_t|*.*.*_[0-9][a-z]
#tablegroup_white_list=tpch.TPCH_TG_1000G_LINEITEM_ORDER_GROUP
start_log_id_locator_locate_count=1

cluster_password=
cluster_user=

need_verify_ob_trace_id=0
ob_trace_id=obstress_trace_id1
instance_index=0
instance_num=1
formatter_thread_num=25
formatter_batch_stmt_count=30000
storager_thread_num=20
reader_thread_num=20
stream_max_partition_count=500
#skip_ob_version_compat_check=1
sort_trans_participants=1
#drc_message_factory_binlog_record_type=BinlogRecordImpl
#test_mode_on=1
#test_mode_ignore_redo_count=10
#enable_verify_mode=1
#print_participant_not_serve_info=1
#enable_output_hidden_primary_key=1
