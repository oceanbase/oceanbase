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
  EstimateBlockRes() :
    part_id_(),
    macro_block_count_(0),
    micro_block_count_(0),
    sstable_row_count_(0),
    memtable_row_count_(0),
    cg_macro_cnt_arr_(),
    cg_micro_cnt_arr_()
  {}
  ObObjectID part_id_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  int64_t sstable_row_count_;
  int64_t memtable_row_count_;
  ObArray<int64_t> cg_macro_cnt_arr_;
  ObArray<int64_t> cg_micro_cnt_arr_;
  TO_STRING_KV(K(part_id_),
               K(macro_block_count_),
               K(micro_block_count_),
               K(sstable_row_count_),
               K(memtable_row_count_),
               K(cg_macro_cnt_arr_),
               K(cg_micro_cnt_arr_));
};

class ObBasicStatsEstimator : public ObStatsEstimator
{
public:
  explicit ObBasicStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  static int estimate_block_count(ObExecContext &ctx,
                                  const ObTableStatParam &param,
                                  PartitionIdBlockMap &id_block_map,
                                  bool &use_column_store,
                                  bool &use_split_part);

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
                                      const int64_t global_part_id,
                                      const ObIArray<PartInfo> &partition_infos,
                                      const double stale_percent_threshold,
                                      ObIArray<ObPartitionStatInfo> &partition_stat_infos);

  static int update_last_modified_count(ObExecContext &ctx,
                                        const ObTableStatParam &param);

  static int update_last_modified_count(sqlclient::ObISQLConnection *conn,
                                        const ObTableStatParam &param);

  static int check_table_statistics_state(ObExecContext &ctx,
                                          const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          const int64_t global_part_id,
                                          bool &is_locked,
                                          ObIArray<ObPartitionStatInfo> &partition_stat_infos);

  static int check_partition_stat_state(const int64_t partition_id,
                                        const int64_t inc_mod_count,
                                        const double stale_percent_threshold,
                                        ObIArray<ObPartitionStatInfo> &partition_stat_infos);

  static int gen_tablet_list(const ObTableStatParam &param,
                             ObSqlString &tablet_list,
                             bool &is_all_update);

  static int do_estimate_block_count(ObExecContext &ctx,
                                     const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const ObIArray<ObTabletID> &tablet_ids,
                                     const ObIArray<ObObjectID> &partition_ids,
                                     const ObIArray<uint64_t> &column_group_ids,
                                     ObIArray<EstimateBlockRes> &estimate_res);

  static int do_estimate_block_count_and_row_count(ObExecContext &ctx,
                                                   const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   const ObIArray<ObTabletID> &tablet_ids,
                                                   const ObIArray<ObObjectID> &partition_ids,
                                                   const ObIArray<uint64_t> &column_group_ids,
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
                                   const int64_t last_table_id,
                                   const int64_t slice_cnt,
                                   ObIArray<int64_t> &table_ids);

  int estimate(const ObOptStatGatherParam &param,
               ObIArray<ObOptStat> &dst_opt_stats);

  template <class T>
  int add_stat_item(const T &item);

  int fill_hints(common::ObIAllocator &alloc,
                 const ObString &table_name,
                 int64_t gather_vectorize,
                 bool use_column_store);

private:

  static int generate_first_part_idx_map(const ObIArray<PartInfo> &all_part_infos,
                                         hash::ObHashMap<int64_t, int64_t> &first_part_idx_map);

  int refine_basic_stats(const ObOptStatGatherParam &param,
                         ObIArray<ObOptStat> &dst_opt_stats);

  int check_stat_need_re_estimate(const ObOptStatGatherParam &origin_param,
                                  ObOptStat &opt_stat,
                                  bool &need_re_estimate,
                                  ObOptStatGatherParam &new_param);

  int fill_hints(common::ObIAllocator &alloc, const ObString &table_name);

  static int generate_column_group_ids(const ObTableStatParam &param,
                                       ObIArray<uint64_t> &column_group_ids);

  static int check_can_use_column_store_and_split_part_gather(const int64_t sstable_row_cnt,
                                                              const int64_t memtable_row_cnt,
                                                              const int64_t cg_cnt,
                                                              const int64_t part_cnt,
                                                              const int64_t degree,
                                                              bool &use_column_store,
                                                              bool &use_split_part);

  static int get_gather_table_type_list(ObSqlString &gather_table_type_list);
};

}
}

#endif // OB_BASIC_STATS_ESTIMATOR_H
