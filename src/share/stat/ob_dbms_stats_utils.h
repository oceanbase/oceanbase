/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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

class ObDbmsStatsUtils
{
public:
  static int init_col_stats(ObIAllocator &allocator,
                            int64_t col_cnt,
                            ObIArray<ObOptColumnStat *> &col_stats);

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
                                 bool &is_valid);

  static int check_is_sys_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                   const uint64_t tenant_id,
                                   const int64_t table_id,
                                   bool &is_valid);

  static bool is_no_stat_virtual_table(const int64_t table_id);

  static bool is_virtual_index_table(const int64_t table_id);

  static int parse_granularity(const ObString &granularity, ObGranularityType &granu_type);

  static bool is_subpart_id(const ObIArray<PartInfo> &partition_infos,
                            const int64_t partition_id,
                            int64_t &part_id);
  
  static int get_valid_duration_time(const int64_t start_time,
                                     const int64_t max_duration_time,
                                     int64_t &valid_duration_time);

  static int get_dst_partition_by_tablet_id(sql::ObExecContext &ctx,
                                            const uint64_t tablet_id,
                                            const ObIArray<PartInfo> &partition_infos,
                                            int64_t &partition_id);

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

  static int remove_stat_gather_param_partition_info(int64_t reserved_partition_id,
                                                     ObOptStatGatherParam &param);

  static int64_t check_text_can_reuse(const ObObj &obj, bool &can_reuse);

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
                                           double &percent);
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

};

}
}

#endif // OB_DBMS_STATS_UTILS_H
