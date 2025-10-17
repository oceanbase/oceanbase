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

#ifndef OB_DBMS_STATS_EXECUTOR_H
#define OB_DBMS_STATS_EXECUTOR_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase {
namespace observer
{
class ObInnerSQLConnection;
}
using namespace sql;
namespace common {

struct ObOptStatRunningMonitor;
struct GatherHelper
{
  explicit GatherHelper(ObOptStatRunningMonitor &running_monitor) :
    is_split_gather_(false),
    maximum_gather_part_cnt_(1),
    maximum_gather_col_cnt_(1),
    is_approx_gather_(false),
    gather_vectorize_(DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE),
    running_monitor_(running_monitor),
    use_column_store_(false),
    use_single_part_(false),
    is_split_column_(false),
    is_all_col_gathered_(false),
    batch_part_size_(OPT_DEFAULT_AUTO_COLLECT_BATCH_SIZE),
    sepcify_scn_(0),
    lasted_collect_parts_()
  {}
  bool is_split_gather_;
  int64_t maximum_gather_part_cnt_;
  int64_t maximum_gather_col_cnt_;
  bool is_approx_gather_;
  int64_t gather_vectorize_;
  ObOptStatRunningMonitor &running_monitor_;
  bool use_column_store_;
  bool use_single_part_;

  bool is_split_column_;

  bool is_all_col_gathered_;

  int64_t batch_part_size_;

  uint64_t sepcify_scn_;

  ObSEArray<PartInfo, 4> lasted_collect_parts_; // need recollect stats if timeout

  TO_STRING_KV(K(is_split_gather_),
               K(maximum_gather_part_cnt_),
               K(maximum_gather_col_cnt_),
               K(is_approx_gather_),
               K(gather_vectorize_),
               K(running_monitor_),
               K(use_column_store_),
               K(use_single_part_),
               K(is_split_column_),
               K(is_all_col_gathered_),
               K(batch_part_size_),
               K(sepcify_scn_),
               K(lasted_collect_parts_));
};

struct GatherPartInfos {
  explicit GatherPartInfos() :
    sub_part_infos_(),
    part_infos_(),
    approx_gather_(false),
    gather_global_(false)
  {}

  ObSEArray<PartInfo, 4> sub_part_infos_;
  ObSEArray<PartInfo, 4> part_infos_;

  bool approx_gather_;
  bool gather_global_;

  bool is_invalid_task() {
    return sub_part_infos_.empty() && part_infos_.empty() && !gather_global_;
  }

  TO_STRING_KV(K(approx_gather_),
               K(gather_global_),
               K(sub_part_infos_),
               K(part_infos_));
};

struct TaskColumnParam {
  TaskColumnParam() : column_params_(), start(0), end(0)
  {}

  // [start, end)
  const ObIArray<oceanbase::common::ObColumnStatParam> *column_params_;
  int32_t start;
  int32_t end;

  TO_STRING_KV(K(*column_params_),K(start), K(end));
};

class ObDbmsStatsExecutor
{
public:
  ObDbmsStatsExecutor();

  static int gather_table_stats(ObExecContext &ctx,
                                const ObTableStatParam &param,
                                ObOptStatRunningMonitor &running_monitor);

  static int gather_index_stats(ObExecContext &ctx,
                                const ObTableStatParam &param);

  static int set_table_stats(ObExecContext &ctx,
                             const ObSetTableStatParam &param);

  static int set_column_stats(ObExecContext &ctx,
                              const ObSetColumnStatParam &param);

  static int delete_table_stats(ObExecContext &ctx,
                                const ObTableStatParam &param,
                                const bool cascade_columns);

  static int delete_column_stats(ObExecContext &ctx,
                                 const ObTableStatParam &param,
                                 const bool only_histogram);

  static int update_online_stat(ObExecContext &ctx,
                                ObTableStatParam &param,
                                share::schema::ObSchemaGetterGuard *schema_guard,
                                const TabStatIndMap &online_table_stats,
                                const ColStatIndMap &online_column_stats,
                                const ObIArray<ObOptDmlStat *> *dml_stats = nullptr /*for_direct_load*/);

  static int cancel_gather_stats(ObExecContext &ctx, ObString &task_id);

  static int gather_system_stats(ObExecContext &ctx, int64_t tenant_id);

  static int delete_system_stats(ObExecContext &ctx, int64_t tenant_id);

  static int set_system_stats(ObExecContext &ctx, const ObSetSystemStatParam &param);


private:

  static int prepare_gather_stats(ObExecContext &ctx,
                                  const ObTableStatParam &param,
                                  PartitionIdBlockMap &partition_id_block_map,
                                  PartitionIdSkipRateMap &partition_id_skip_rate_map,
                                  GatherHelper &gather_helper);
  static int do_gather_stats_with_retry(ObExecContext &ctx,
                                        ObMySQLTransaction &trans,
                                        StatLevel stat_level,
                                        const ObIArray<PartInfo> &gather_partition_infos,
                                        const PartitionIdBlockMap *partition_id_block_map,
                                        const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                        GatherHelper &gather_helper,
                                        ObTableStatParam &derive_param,
                                        ObIArray<ObOptTableStat *> &all_tstats);

  static int do_gather_stats(ObExecContext &ctx,
                             ObMySQLTransaction &trans,
                             ObOptStatGatherParam &param,
                             const ObIArray<PartInfo> &gather_partition_infos,
                             const ObIArray<ObColumnStatParam> &gather_column_params,
                             bool is_all_columns_gather,
                             ObOptStatGatherAudit &audit,
                             ObIArray<ObOptStat> &opt_stats,
                             ObIArray<ObOptTableStat *> &all_tstats,
                             ObIArray<ObOptColumnStat *> &all_cstats);

  static int do_set_table_stats(const ObSetTableStatParam &param,
                                ObOptTableStat *table_stat);

  static int do_set_column_stats(ObIAllocator &allocator,
                                 const ObDataTypeCastParams &dtc_params,
                                 const ObSetColumnStatParam &param,
                                 ObOptColumnStat *&column_stat);

  static int reset_table_locked_state(ObExecContext &ctx,
                                      const ObTableStatParam &param,
                                      const ObIArray<int64_t> &no_stats_partition_ids,
                                      const ObIArray<uint64_t> &part_stattypes);

  static int check_need_split_gather(const ObTableStatParam &param,
                                     GatherHelper &gather_helper);

  static int prepare_conn_and_store_session_for_online_stats(sql::ObSQLSessionInfo *session,
                                                             common::ObMySQLProxy *sql_proxy,
                                                             share::schema::ObSchemaGetterGuard *schema_guard,
                                                             sql::ObSQLSessionInfo::StmtSavedValue &saved_value,
                                                             int64_t &nested_count,
                                                             int64_t &old_trx_lock_timeout,
                                                             bool &need_restore_session,
                                                             bool &need_reset_trx_lock_timeout,
                                                             sqlclient::ObISQLConnection *&conn);

  static int restore_session_for_online_stat(sql::ObSQLSessionInfo *session,
                                             sql::ObSQLSessionInfo::StmtSavedValue &saved_value,
                                             int64_t nested_count,
                                             int64_t old_trx_lock_timeout,
                                             bool need_reset_trx_lock_timeout);
  static int merge_split_gather_tab_stats(ObIArray<ObOptTableStat *> &all_tstats,
                                          ObIArray<ObOptTableStat *> &cur_all_tstats);

  static int fetch_gather_table_snapshot_read(common::sqlclient::ObISQLConnection *conn,
                                              uint64_t tenant_id,
                                              uint64_t &current_scn);

 static int fetch_gather_task_addr(ObCommonSqlProxy *sql_proxy,
                                    ObIAllocator &allcoator,
                                    uint64_t tenant_id,
                                    const ObString &task_id,
                                    char *&svr_ip,
                                    int32_t &svr_port);
  static bool is_async_gather_partition_id(const int64_t partition_id,
                                           const ObIArray<int64_t> *async_partition_ids);

  static int determine_auto_sample_table(ObExecContext &ctx, ObTableStatParam &param);

  static int try_use_prefix_index_refine_min_max(ObExecContext &ctx, ObTableStatParam &param);

  static int check_use_single_partition_gather(const PartitionIdBlockMap &partition_id_block_map,
                                               const ObTableStatParam &param,
                                               bool &need_split_part);

  static int get_skip_rate_sample_count(ObMySQLProxy *mysql_proxy,
                                        const uint64_t tenant_id,
                                        const uint64_t table_id,
                                        int64_t &sample_count);

  static int collect_executed_part_ids(const ObTableStatParam &stat_param, ObIArray<int64_t> &part_ids);
  static int update_dml_modified_info(sqlclient::ObISQLConnection *conn, const ObTableStatParam &param);

  static int split_table_part_param(const ObTableStatParam &stat_param,
                                    const GatherHelper &gather_helper,
                                    ObIArray<GatherPartInfos> &batch_part_infos);
  static int add_L0_L1_part_to_task(uint64_t part_id,
                                    bool need_collect_global,
                                    const hash::ObHashMap<int64_t, PartInfo*> &part_id_to_approx_part_map,
                                    GatherPartInfos &gather_info);

  static int construct_part_to_subpart_map(const ObTableStatParam &stat_param,
                                           hash::ObHashMap<int64_t, PartInfo*> &part_id_to_approx_part_map,
                                           hash::ObHashMap<int64_t, ObArray<PartInfo*>> &part_id_to_subpart_map);

  static int split_column_param(const ObTableStatParam &stat_param,
                                GatherHelper &gather_helper,
                                ObIArray<TaskColumnParam> &batch_task_col_infos);

  static int do_split_gather_stats(ObExecContext &ctx,
                                   ObMySQLTransaction &trans,
                                   GatherPartInfos &taskInfo,
                                   ObIArray<TaskColumnParam> &batch_task_col_infos,
                                   ObTableStatParam &derive_param,
                                   const PartitionIdBlockMap *partition_id_block_map,
                                   const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                   GatherHelper &gather_helper);

  static int gather_partition_stats(ObExecContext &ctx,
                                    const ObTableStatParam &param,
                                    const PartitionIdBlockMap *partition_id_block_map,
                                    const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                    GatherHelper &gather_helper,
                                    ObIArray<int64_t> &failed_part_ids);

  static int collect_last_part_and_global_if_timeout(ObExecContext &ctx,
                                                     const ObTableStatParam &origin_param,
                                                     const PartitionIdBlockMap *partition_id_block_map,
                                                     const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                                     GatherHelper &gather_helper,
                                                     ObIArray<int64_t> &failed_part_ids);

  static int collect_last_parts(const ObTableStatParam &param, GatherHelper &gather_helper);

  static int get_stats_collect_batch_size(ObMySQLProxy *mysql_proxy,
                                          const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          int64_t &batch_part_size);

  static int mark_internal_stat(const int64_t global_part_id,
                                ObIArray<ObOptTableStat *> &all_tstats,
                                ObIArray<ObOptColumnStat *> &all_cstats);
};
} // end of sql
} // end of namespace

#endif // OB_DBMS_STATS_EXECUTOR_H
