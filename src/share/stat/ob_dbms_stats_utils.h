/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_STATS_UTILS_H
#define OB_DBMS_STATS_UTILS_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_table_stat_cache.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/schema/ob_part_mgr_util.h"

namespace oceanbase {
namespace common {

struct ObStatHashMapIndexGetter
{
  template <typename T>
  int operator()(const T &, const int64_t idx, int64_t &value) const
  {
    value = idx;
    return OB_SUCCESS;
  }
};

struct ObStatHashContainerAcceptAll
{
  template <typename T>
  bool operator()(const T &, const int64_t) const
  {
    return true;
  }
};

template <typename T>
struct ObStatHashIdentityGetter
{
  int operator()(const T &item, const int64_t, T &value) const
  {
    value = item;
    return OB_SUCCESS;
  }
};

class ObDbmsStatsUtils
{
public:

  template <typename Item,
            typename Key,
            typename Value,
            typename KeyGetter,
            typename ValueGetter,
            typename ItemFilter>
  static int build_hash_map_if(const ObIArray<Item> &items,
                               ObStatHashMap<Key, Value> &hash_map,
                               const KeyGetter &key_getter,
                               const ValueGetter &value_getter,
                               const ItemFilter &item_filter,
                               const char *bucket_label,
                               const char *node_label,
                               const uint64_t tenant_id,
                               const bool ignore_duplicate = false)
  {
    int ret = OB_SUCCESS;
    const int64_t bucket_num = std::max<int64_t>(items.count(), 1);
    if (OB_FAIL(hash_map.create(bucket_num, bucket_label, node_label, tenant_id))) {
      COMMON_LOG(WARN, "failed to create hash map", K(ret), K(bucket_num), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (item_filter(items.at(i), i)) {
        Key key;
        Value value;
        if (OB_FAIL(key_getter(items.at(i), i, key))) {
          COMMON_LOG(WARN, "failed to get hash map key", K(ret), K(i));
        } else if (OB_FAIL(value_getter(items.at(i), i, value))) {
          COMMON_LOG(WARN, "failed to get hash map value", K(ret), K(i));
        } else {
          const int tmp_ret = hash_map.set_refactored(key, value);
          if (OB_SUCCESS != tmp_ret && !(OB_HASH_EXIST == tmp_ret && ignore_duplicate)) {
            ret = tmp_ret;
            COMMON_LOG(WARN, "failed to set hash map", K(ret), K(i));
          }
        }
      }
    }
    return ret;
  }

  template <typename Item, typename Key, typename Value, typename KeyGetter, typename ValueGetter>
  static int build_hash_map(const ObIArray<Item> &items,
                            ObStatHashMap<Key, Value> &hash_map,
                            const KeyGetter &key_getter,
                            const ValueGetter &value_getter,
                            const char *bucket_label,
                            const char *node_label,
                            const uint64_t tenant_id,
                            const bool ignore_duplicate = false)
  {
    return build_hash_map_if(items,
                             hash_map,
                             key_getter,
                             value_getter,
                             ObStatHashContainerAcceptAll(),
                             bucket_label,
                             node_label,
                             tenant_id,
                             ignore_duplicate);
  }

  template <typename Item, typename Value, typename ValueGetter, typename ItemFilter>
  static int append_hash_set_if(const ObIArray<Item> &items,
                                ObStatHashSet<Value> &hash_set,
                                const ValueGetter &value_getter,
                                const ItemFilter &item_filter,
                                const bool ignore_duplicate = true)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      if (item_filter(items.at(i), i)) {
        Value value;
        if (OB_FAIL(value_getter(items.at(i), i, value))) {
          COMMON_LOG(WARN, "failed to get hash set value", K(ret), K(i));
        } else {
          const int tmp_ret = hash_set.set_refactored(value, 0 /* do not overwrite */);
          if (OB_SUCCESS != tmp_ret && !(OB_HASH_EXIST == tmp_ret && ignore_duplicate)) {
            ret = tmp_ret;
            COMMON_LOG(WARN, "failed to set hash set", K(ret), K(i));
          }
        }
      }
    }
    return ret;
  }

  template <typename Item, typename Value, typename ValueGetter>
  static int append_hash_set(const ObIArray<Item> &items,
                             ObStatHashSet<Value> &hash_set,
                             const ValueGetter &value_getter,
                             const bool ignore_duplicate = true)
  {
    return append_hash_set_if(items,
                              hash_set,
                              value_getter,
                              ObStatHashContainerAcceptAll(),
                              ignore_duplicate);
  }

  template <typename Item, typename Value, typename ValueGetter, typename ItemFilter>
  static int build_hash_set_if(const ObIArray<Item> &items,
                               ObStatHashSet<Value> &hash_set,
                               const ValueGetter &value_getter,
                               const ItemFilter &item_filter,
                               const int64_t bucket_num,
                               const char *bucket_label,
                               const char *node_label,
                               const uint64_t tenant_id,
                               const bool ignore_duplicate = true)
  {
    int ret = OB_SUCCESS;
    const int64_t actual_bucket_num = bucket_num > 0 ? bucket_num : 1;
    if (OB_FAIL(hash_set.create(actual_bucket_num, bucket_label, node_label, tenant_id))) {
      COMMON_LOG(WARN, "failed to create hash set", K(ret), K(actual_bucket_num), K(tenant_id));
    } else if (OB_FAIL(append_hash_set_if(items,
                                          hash_set,
                                          value_getter,
                                          item_filter,
                                          ignore_duplicate))) {
      COMMON_LOG(WARN, "failed to append hash set", K(ret));
    }
    return ret;
  }

  template <typename Item, typename Value, typename ValueGetter>
  static int build_hash_set(const ObIArray<Item> &items,
                            ObStatHashSet<Value> &hash_set,
                            const ValueGetter &value_getter,
                            const int64_t bucket_num,
                            const char *bucket_label,
                            const char *node_label,
                            const uint64_t tenant_id,
                            const bool ignore_duplicate = true)
  {
    return build_hash_set_if(items,
                             hash_set,
                             value_getter,
                             ObStatHashContainerAcceptAll(),
                             bucket_num,
                             bucket_label,
                             node_label,
                             tenant_id,
                             ignore_duplicate);
  }

  static int init_table_stats(ObIAllocator &allocator,
                            int64_t cnt,
                            ObIArray<ObOptTableStat *> &table_stats);

  static int init_col_stats(ObIAllocator &allocator,
                            int64_t col_cnt,
                            ObIArray<ObOptColumnStat *> &col_stats);

  static int assign_col_param(const ObIArray<ObColumnStatParam> *src_col_params,
                              int64_t start,
                              int64_t end,
                              ObIArray<ObColumnStatParam> &target_col_params);

  static int check_range_skew(ObHistType hist_type,
                              const ObHistogram::Buckets &bkts,
                              int64_t standard_cnt,
                              bool &is_even_distributed);

  static int split_batch_write(sql::ObExecContext &ctx,
                               ObIArray<ObOptTableStat*> &table_stats,
                               ObIArray<ObOptColumnStat*> &column_stats,
                               const bool is_index_stat = false,
                               const bool is_online_stat = false);

  static int split_batch_write(sql::ObExecContext &ctx,
                               sqlclient::ObISQLConnection *conn,
                               ObIArray<ObOptTableStat*> &table_stats,
                               ObIArray<ObOptColumnStat*> &column_stats,
                               const bool is_index_stat = false,
                               const bool is_online_stat = false);

  static int split_batch_write_with_trx_lock_timeout(sql::ObExecContext &ctx,
                                                      sqlclient::ObISQLConnection *conn,
                                                      ObIArray<ObOptTableStat*> &table_stats,
                                                      ObIArray<ObOptColumnStat*> &column_stats,
                                                      const bool is_index_stat = false,
                                                      const bool is_online_stat = false);

  static int split_batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                               sql::ObSQLSessionInfo *session_info,
                               common::ObMySQLProxy *sql_proxy,
                               ObIArray<ObOptTableStat*> &table_stats,
                               ObIArray<ObOptColumnStat*> &column_stats,
                               const bool is_index_stat = false,
                               const bool is_online_stat = false);

  static int split_batch_write(sqlclient::ObISQLConnection *conn,
                               share::schema::ObSchemaGetterGuard *schema_guard,
                               sql::ObSQLSessionInfo *session_info,
                               ObIArray<ObOptTableStat*> &table_stats,
                               ObIArray<ObOptColumnStat*> &column_stats,
                               const bool is_index_stat = false,
                               const bool is_online_stat = false);

  static int cast_number_to_double(const number::ObNumber &src_val, double &dst_val);

  static int check_table_read_write_valid(const uint64_t tenant_id, bool &is_valid);

  static int check_is_stat_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                 const uint64_t tenant_id,
                                 const int64_t table_id,
                                 bool need_index_table,
                                 bool &is_valid);

  static int check_is_sys_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                   const uint64_t tenant_id,
                                   const int64_t table_id,
                                   bool &is_valid);

  static bool is_no_stat_virtual_table(const int64_t table_id);

  static bool is_virtual_index_table(const int64_t table_id);

  static int parse_granularity(const ObString &granularity, ObGranularityType &granu_type);

  // Entries with an invalid first_part_id_ are not subpartitions and are skipped.
  static int generate_subpart_id_to_first_map(const ObIArray<PartInfo> &all_subpart_infos,
                                              ObStatInt64Map &subpart_id_to_first_map,
                                              const uint64_t tenant_id);

  static int generate_part_id_to_idx_map(const ObIArray<PartInfo> &part_infos,
                                         ObStatInt64Map &part_id_to_idx_map,
                                         const uint64_t tenant_id);

  static int generate_part_id_set(const ObIArray<PartInfo> &part_infos,
                                  ObStatInt64Set &part_id_set,
                                  const uint64_t tenant_id,
                                  const bool ignore_duplicate = true);

  static int generate_int64_to_idx_map(const ObIArray<int64_t> &ids,
                                       ObStatInt64Map &id_to_idx_map,
                                       const uint64_t tenant_id);

  static int generate_tablet_id_to_part_id_map(const ObIArray<PartInfo> &partition_infos,
                                               ObStatInt64Map &tablet_id_to_part_id_map,
                                               const uint64_t tenant_id);

  static int generate_tablet_id_to_idx_map(const ObIArray<PartInfo> &partition_infos,
                                           ObStatInt64Map &tablet_id_to_idx_map,
                                           const uint64_t tenant_id);

  static int generate_partition_stat_id_to_idx_map(
      const ObIArray<ObPartitionStatInfo> &partition_stat_infos,
      ObStatInt64Map &partition_stat_id_to_idx_map,
      const uint64_t tenant_id);

  static int get_subpart_ids(const ObIArray<PartInfo> &partition_infos,
                             const int64_t partition_id,
                             ObIArray<int64_t> &sub_part_ids);
  static int get_no_need_collect_part_ids(const ObTableStatParam &param,
                                          const int64_t partition_id,
                                          ObIArray<int64_t> &no_collect_subpart_ids);

  static int get_valid_duration_time(const int64_t start_time,
                                     const int64_t max_duration_time,
                                     int64_t &valid_duration_time);

  static int calssify_opt_stat(const ObIArray<ObOptStat> &opt_stats,
                               ObIArray<ObOptTableStat *> &table_stats,
                               ObIArray<ObOptColumnStat*> &column_stats);
  static int merge_tab_stats(
    const ObTableStatParam &param,
    const TabStatIndMap &table_stats,
    common::ObIArray<ObOptTableStat*> &old_tab_stats,
    common::ObIArray<ObOptTableStat*> &dst_tab_stats);

  static int merge_col_stats(
    const ObTableStatParam &param,
    const ColStatIndMap &column_stats,
    common::ObIArray<ObOptColumnStat*> &old_col_stats,
    common::ObIArray<ObOptColumnStat*> &dst_col_stats);

  static bool is_part_id_valid(const ObTableStatParam &param, const ObObjectID part_id);

  static int get_part_infos(const ObTableSchema &table_schema,
                            ObIAllocator &allocator,
                            ObIArray<PartInfo> &part_infos,
                            ObIArray<PartInfo> &subpart_infos,
                            ObIArray<int64_t> &part_ids,
                            ObIArray<int64_t> &subpart_ids,
                            OSGPartMap *part_map = NULL);

  static int get_subpart_infos(const share::schema::ObTableSchema &table_schema,
                               const share::schema::ObPartition *part,
                               ObIAllocator &allocator,
                               ObIArray<PartInfo> &subpart_infos,
                               ObIArray<int64_t> &subpart_ids,
                               OSGPartMap *part_map = NULL);

  static int truncate_string_for_opt_stats(const ObObj *old_obj,
                                           ObIAllocator &alloc,
                                           ObObj *&new_obj);

  static int truncate_string_for_opt_stats(ObObj &obj, ObIAllocator &allocator);

  static int64_t get_truncated_str_len(const ObString &str, const ObCollationType cs_type);

  static int64_t check_text_can_reuse(const ObObj &obj, bool &can_reuse);

  static int error_code_wrapper(int ret);
  static int get_current_opt_stats(const ObTableStatParam &param,
                                   ObIArray<ObOptTableStatHandle> &cur_tab_handles,
                                   ObIArray<ObOptColumnStatHandle> &cur_col_handles);

  static int get_current_opt_stats(ObIAllocator &allocator,
                                   sqlclient::ObISQLConnection *conn,
                                   const ObTableStatParam &param,
                                   ObIArray<ObOptTableStat *> &table_stats,
                                   ObIArray<ObOptColumnStat *> &column_stats);

  static int get_part_ids_and_column_ids(const ObTableStatParam &param,
                                         ObIArray<int64_t> &part_ids,
                                         ObIArray<uint64_t> &column_ids,
                                         bool need_stat_column = false);

  static int erase_stat_cache(const uint64_t tenant_id,
                              const uint64_t table_id,
                              const ObIArray<int64_t> &part_ids,
                              const ObIArray<uint64_t> &column_ids);

  static bool find_part(const ObIArray<PartInfo> &part_infos,
                        const ObString &part_name,
                        bool is_sensitive_compare,
                        PartInfo &part);

  static int prepare_gather_stat_param(const ObTableStatParam &param,
                                       StatLevel stat_level,
                                       const PartitionIdBlockMap *partition_id_block_map,
                                       const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                       bool is_split_gather,
                                       int64_t gather_vectorize,
                                       bool use_column_store,
                                       ObOptStatGatherParam &gather_param);

  static int merge_split_gather_tab_stats(ObIArray<ObOptTableStat *> &all_tstats,
                                          ObIArray<ObOptTableStat *> &cur_all_tstats);

  static int check_all_cols_range_skew(const ObIArray<ObColumnStatParam> &column_params,
                                       ObIArray<ObOptStat> &opt_stats);

  static int implicit_commit_before_gather_stats(sql::ObExecContext &ctx);

  static int scale_col_stats(const uint64_t tenant_id,
                             const common::ObIArray<ObOptTableStat*> &tab_stats,
                             common::ObIArray<ObOptColumnStat*> &col_stats);

  static int scale_col_stats(const uint64_t tenant_id,
                             const TabStatIndMap &table_stats,
                             common::ObIArray<ObOptColumnStat*> &col_stats);

  static int get_sys_online_estimate_percent(sql::ObExecContext &ctx,
                                             const uint64_t tenant_id,
                                             const uint64_t table_id,
                                             double &percent);
  static int check_can_async_gather_stats(sql::ObExecContext &ctx);

  static int cancel_async_gather_stats(sql::ObExecContext &ctx);

  static int build_index_part_to_table_part_maps(share::schema::ObSchemaGetterGuard *schema_guard,
                                                 uint64_t tenant_id,
                                                 uint64_t index_table_id,
                                                 ObStatObjectIDMap &part_id_map);

  static int deduce_index_column_stat_to_table(share::schema::ObSchemaGetterGuard *schema_guard,
                                               uint64_t tenant_id,
                                               uint64_t index_table_id,
                                               uint64_t data_table_id,
                                               ObPartitionLevel part_level,
                                               ObIArray<ObOptColumnStat *> &all_column_stats);

  static int get_prefix_index_substr_length(const share::schema::ObColumnSchemaV2 &col,
                                            int64_t &length);

  static int get_prefix_index_text_pairs(share::schema::ObSchemaGetterGuard *schema_guard,
                                         uint64_t tenant_id,
                                         uint64_t data_table_id,
                                         ObIArray<uint64_t> &func_idxs,
                                         ObIArray<uint64_t> &ignore_cols,
                                         ObIArray<PrefixColumnPair> &pairs);
  static int get_all_prefix_index_text_pairs(const share::schema::ObTableSchema &table_schema,
                                             ObIArray<uint64_t> &filter_cols,
                                             ObIArray<PrefixColumnPair> &filter_pairs);

  static int copy_local_index_prefix_stats_to_text(ObIAllocator &allocator,
                                                   const ObIArray<ObOptColumnStat*> &column_stats,
                                                   const ObIArray<PrefixColumnPair> &pairs,
                                                   ObIArray<ObOptColumnStat*> &copy_stats);

  static int set_trx_lock_timeout(sqlclient::ObISQLConnection *conn,
                                  int64_t trx_lock_timeout,
                                  int64_t &old_trx_lock_timeout,
                                  bool &need_restore);
  static int copy_global_index_prefix_stats_to_text(share::schema::ObSchemaGetterGuard *schema_guard,
                                                    ObIAllocator &allocator,
                                                    const ObIArray<ObOptColumnStat*> &column_stats,
                                                    const ObIArray<PrefixColumnPair> &pairs,
                                                    uint64_t tenant_id,
                                                    uint64_t data_table_id,
                                                    ObIArray<ObOptColumnStat *> &all_column_stats);
  static int copy_prefix_column_stat_to_text(ObIAllocator &allocator,
                                             const ObOptColumnStat &col_stat,
                                             const ObObjMeta &text_col_meta,
                                             ObOptColumnStat *&text_column_stat);
  static int deep_copy_string(char *buf, const int64_t buf_len, int64_t &pos,
                              const ObString &str, ObString &dst);

  static int get_max_work_area_size(uint64_t tenant_id, int64_t &max_wa_memory_size);


  static int get_table_index_infos(share::schema::ObSchemaGetterGuard *schema_guard,
                                   const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   uint64_t *index_tid_arr,
                                   int64_t &index_count);

  static int dbms_stat_set_names(ObSQLSessionInfo *session_info,
                                 ObCharsetType client_charset_type,
                                 ObCharsetType connection_charset_type,
                                 ObCharsetType result_charset_type,
                                 ObCollationType collation_type);

  static int find_column_param_by_column_id(const ObIArray<ObColumnStatParam> &column_params,
                                            const uint64_t column_id,
                                            bool &find_it,
                                            const ObColumnStatParam *&column_param);
  static int get_all_part_ids(const ObTableSchema &table_schema,
                              ObIArray<int64_t> &part_ids);


private:
  static int batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                         const uint64_t tenant_id,
                         sqlclient::ObISQLConnection *conn,
                         ObIArray<ObOptTableStat *> &table_stats,
                         ObIArray<ObOptColumnStat*> &column_stats,
                         const int64_t current_time,
                         const bool is_index_stat,
                         const bool is_online_stat = false,
                         const ObObjPrintParams &print_params = ObObjPrintParams());

  static int fetch_need_cancel_async_gather_stats_task(ObIAllocator &allocator,
                                                       sql::ObExecContext &ctx,
                                                       ObIArray<ObString> &task_ids);
  static int build_sub_part_maps(const ObTableSchema* table_schema,
                                 const ObTableSchema* index_schema,
                                 const ObPartition *index_part,
                                 const ObPartition *table_part,
                                 ObCheckPartitionMode mode,
                                 ObStatObjectIDMap &part_id_map);

};

}
}

#endif // OB_DBMS_STATS_UTILS_H
