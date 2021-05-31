include $(top_srcdir)/unittest/ut_template.am
LDADD += $(top_builddir)/unittest/storage/mockcontainer/libmockcontainer.a
#
bin_PROGRAMS = test_save_storage_info \
               test_range_iterator \
               test_ob_col_map \
               test_placement_hashmap\
               test_parallel_external_sort\
               test_i_store\
               test_sstable_merge_info_mgr\
							 test_sstable_single_get\
							 test_sstable_single_scan\
							 test_sstable_multi_get\
							 test_sstable_multi_scan\
							 test_sstable_single_exist\
							 test_sstable_multi_exist\
							 test_interm_macro_mgr\
							 test_multi_version_sstable_single_get\
							 test_multi_version_sstable_single_scan\
							 test_row_sample_iterator\
							 test_all_micro_block_range_iterator\
							 test_multi_version_sstable_multi_get\
							 test_multi_version_sstable_multi_scan\
							 test_micro_block_row_scanner\
							 test_all_micro_block_range_iterator\
							 test_migrate_logical_row_writer \
							 test_migrate_rpc\
							 test_table_store\
							 test_table_store_stat_mgr\
							 test_join_mv_merge\
							 test_dag_size\
							 test_split_version_range\
							 test_handle_cache\
							 test_sparse_sstable\
							 test_hash_performance\
							 test_partition_migrator_table_key_mgr

test_save_storage_info_SOURCES=test_save_storage_info.cpp
test_range_iterator_SOURCES=test_range_iterator.cpp
test_ob_col_map_SOURCES=test_ob_col_map.cpp
test_placement_hashmap_SOURCES=test_placement_hashmap.cpp
test_parallel_external_sort_SOURCES=test_parallel_external_sort.cpp
test_sstable_merge_info_mgr_SOURCES=test_sstable_merge_info_mgr.cpp
test_sstable_single_get_SOURCES=test_sstable_single_get.cpp
test_sstable_single_scan_SOURCES=test_sstable_single_scan.cpp
test_sstable_multi_get_SOURCES=test_sstable_multi_get.cpp
test_sstable_multi_scan_SOURCES=test_sstable_multi_scan.cpp
test_sstable_single_exist_SOURCES=test_sstable_single_exist.cpp
test_sstable_multi_exist_SOURCES=test_sstable_multi_exist.cpp
test_interm_macro_mgr_SOURCES=test_interm_macro_mgr.cpp
test_multi_version_sstable_single_get_SOURCES=test_multi_version_sstable_single_get.cpp
test_multi_version_sstable_single_scan_SOURCES=test_multi_version_sstable_single_scan.cpp
test_multi_version_sstable_multi_get_SOURCES=test_multi_version_sstable_multi_get.cpp
test_multi_version_sstable_multi_scan_SOURCES=test_multi_version_sstable_multi_scan.cpp
test_migrate_logical_row_writer_SOURCES=test_migrate_logical_row_writer.cpp
test_migrate_rpc_SOURCES=test_migrate_rpc.h test_migrate_rpc.cpp
test_table_store_SOURCES=test_table_store.cpp
test_table_store_stat_mgr_SOURCES=test_table_store_stat_mgr.cpp
test_i_store_SOURCES=test_i_store.cpp
test_join_mv_merge_SOURCES=test_join_mv_merge.cpp
#test_logic_row_producer_SOURCES=test_logic_row_producer.cpp
test_micro_block_row_scanner_SOURCES=test_micro_block_row_scanner.cpp
test_split_version_range_SOURCES=test_split_version_range.cpp
test_handle_cache_SOURCES=test_handle_cache.cpp
test_hash_performance_SOURCES=test_hash_performance.cpp
test_partition_migrator_table_key_mgr_SOURCES=test_partition_migrator_table_key_mgr.cpp
#test_log_replay_engine_SOURCES=replayengine/test_log_replay_engine.cpp

#include replayengine/Makemodule.am

SUBDIRS = mockcontainer transaction blocksstable
clean-local:
	-rm -fr tmp *.log *.log.wf sstable
