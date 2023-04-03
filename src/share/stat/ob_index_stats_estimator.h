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

#ifndef OB_INDEX_STATS_ESTIMATOR_H
#define OB_INDEX_STATS_ESTIMATOR_H

#include "share/stat/ob_basic_stats_estimator.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

class ObIndexStatsEstimator : public ObBasicStatsEstimator
{
public:
  explicit ObIndexStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObTableStatParam &param,
               const ObExtraParam &extra,
               ObIArray<ObOptStat> &dst_opt_stats);

  static int fast_gather_index_stats(ObExecContext &ctx,
                                     const ObTableStatParam &data_param,
                                     const ObTableStatParam &index_param,
                                     bool &is_fast_gather);
private:
  int fill_index_info(common::ObIAllocator &alloc,
                      const ObString &table_name,
                      const ObString &index_name);

  int fill_index_group_by_info(ObIAllocator &allocator,
                               const ObTableStatParam &param,
                               const ObExtraParam &extra,
                               ObString &calc_part_id_str);

  int fill_partition_condition(ObIAllocator &allocator,
                               const ObTableStatParam &param,
                               const ObExtraParam &extra,
                               const int64_t dst_partition_id);

  static int fast_get_index_avg_len(const int64_t data_partition_id,
                                    const ObTableStatParam &data_param,
                                    const ObTableStatParam &index_param,
                                    bool &is_fast_get,
                                    int64_t &avg_len);

  static int get_all_need_gather_partition_ids(const ObTableStatParam &data_param,
                                               const ObTableStatParam &index_param,
                                               ObIArray<int64_t> &gather_part_ids);

  static int get_index_part_id(const int64_t data_tab_partition_id,
                               const ObTableStatParam &data_param,
                               const ObTableStatParam &index_param,
                               int64_t &index_partition_id);
};

}
}

#endif // OB_BASIC_STATS_ESTIMATOR_H
