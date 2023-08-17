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

#ifndef OB_BASIC_STATS_ESTIMATOR_H
#define OB_BASIC_STATS_ESTIMATOR_H

#include "share/stat/ob_stats_estimator.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

struct EstimateBlockRes
{
  EstimateBlockRes() : part_id_(), macro_block_count_(0), micro_block_count_(0), sstable_row_count_(0), memtable_row_count_(0) {}
  ObObjectID part_id_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  int64_t sstable_row_count_;
  int64_t memtable_row_count_;
  TO_STRING_KV(K(part_id_),
               K(macro_block_count_),
               K(micro_block_count_),
               K(sstable_row_count_),
               K(memtable_row_count_));
};

class ObBasicStatsEstimator : public ObStatsEstimator
{
public:
  explicit ObBasicStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  static int estimate_block_count(ObExecContext &ctx,
                                  const ObTableStatParam &param,
                                  PartitionIdBlockMap &id_block_map);

  static int estimate_modified_count(ObExecContext &ctx,
                                     const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     int64_t &result,
                                     const bool need_inc_modified_count = true);

  static int estimate_row_count(ObExecContext &ctx,
                                const uint64_t tenant_id,
                                const uint64_t table_id,
                                int64_t &row_cnt);
  static int get_gather_table_duration(ObExecContext &ctx,
                                       const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       int64_t &last_gather_duration);

  static int estimate_stale_partition(ObExecContext &ctx,
                                      const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      const ObIArray<PartInfo> &partition_infos,
                                      const double stale_percent_threshold,
                                      const ObIArray<ObPartitionStatInfo> &partition_stat_infos,
                                      ObIArray<int64_t> &no_regather_partition_ids,
                                      int64_t &no_regather_first_part_cnt);

  static int update_last_modified_count(ObExecContext &ctx,
                                        const ObTableStatParam &param);

  static int check_table_statistics_state(ObExecContext &ctx,
                                          const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          const int64_t global_part_id,
                                          bool &is_locked,
                                          ObIArray<ObPartitionStatInfo> &partition_stat_infos);

  static int check_partition_stat_state(const int64_t partition_id,
                                        const ObIArray<PartInfo> &partition_infos,
                                        const int64_t inc_mod_count,
                                        const double stale_percent_threshold,
                                        const ObIArray<ObPartitionStatInfo> &partition_stat_infos,
                                        ObIArray<int64_t> &no_regather_partition_ids,
                                        int64_t &no_regather_first_part_cnt);

  static int gen_tablet_list(const ObTableStatParam &param,
                             ObSqlString &tablet_list);

  static int do_estimate_block_count_and_row_count(ObExecContext &ctx,
                                                   const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   const ObIArray<ObTabletID> &tablet_ids,
                                                   const ObIArray<ObObjectID> &partition_ids,
                                                   ObIArray<EstimateBlockRes> &estimate_res);

  static int get_tablet_locations(ObExecContext &ctx,
                                  const uint64_t ref_table_id,
                                  const ObIArray<ObTabletID> &tablet_ids,
                                  const ObIArray<ObObjectID> &partition_ids,
                                  ObCandiTabletLocIArray &candi_tablet_locs);

  static int stroage_estimate_block_count_and_row_count(ObExecContext &ctx,
                                                        const ObAddr &addr,
                                                        const obrpc::ObEstBlockArg &arg,
                                                        obrpc::ObEstBlockRes &result);

  static int get_all_tablet_id_and_object_id(const ObTableStatParam &param,
                                             ObIArray<ObTabletID> &tablet_ids,
                                             ObIArray<ObObjectID> &partition_ids);

  static int get_need_stats_tables(ObExecContext &ctx,
                                   const int64_t tenant_id,
                                   ObIArray<int64_t> &table_ids,
                                   int64_t &slice_cnt);

  static int get_need_stats_table_cnt(ObExecContext &ctx,
                                      const int64_t tenant_id,
                                      int64_t &task_table_count);

  int estimate(const ObTableStatParam &param,
               const ObExtraParam &extra,
               ObIArray<ObOptStat> &dst_opt_stats);

  template <class T>
  int add_stat_item(const T &item);

private:

  static int generate_first_part_idx_map(const ObIArray<PartInfo> &all_part_infos,
                                         hash::ObHashMap<int64_t, int64_t> &first_part_idx_map);

  int refine_basic_stats(const ObTableStatParam &param,
                         const ObExtraParam &extra,
                         ObIArray<ObOptStat> &dst_opt_stats);

  int check_stat_need_re_estimate(const ObTableStatParam &origin_param,
                                  const ObExtraParam &origin_extra,
                                  ObOptStat &opt_stat,
                                  bool &need_re_estimate,
                                  ObTableStatParam &new_param,
                                  ObExtraParam &new_extra);

  int fill_hints(common::ObIAllocator &alloc, const ObString &table_name);
};

}
}

#endif // OB_BASIC_STATS_ESTIMATOR_H
