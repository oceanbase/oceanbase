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

#ifndef OB_STATS_ESTIMATOR_H
#define OB_STATS_ESTIMATOR_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

enum CopyStatType
{
  COPY_ALL_STAT,
  COPY_HYBRID_HIST_STAT
};

class ObStatsEstimator
{
public:
  explicit ObStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator);

protected:

  int gen_select_filed();

  int64_t get_item_size() const { return stat_items_.count(); }

  int decode(ObIAllocator &allocator);

  int add_result(ObObj &obj)  { return results_.push_back(obj); }

  int do_estimate(uint64_t tenant_id,
                  const ObString &raw_sql,
                  CopyStatType copy_type,
                  ObOptStat &src_opt_stat,
                  ObIArray<ObOptStat> &dst_opt_stats);

  int pack(ObSqlString &raw_sql_str);

  int add_from_table(const ObString &db_name,
                     const ObString &table_name)
  {
    db_name_ = db_name;
    from_table_ = table_name;
    return OB_SUCCESS;
  }

  int add_partition_hint(const ObString &partition);

  int fill_sample_info(common::ObIAllocator &alloc,
                       double est_percent,
                       bool block_sample,
                       ObString &sample_hint);

  int fill_sample_info(common::ObIAllocator &alloc,
                       const ObAnalyzeSampleInfo &sample_info);

  int fill_parallel_info(common::ObIAllocator &alloc,
                         int64_t degree);

  int fill_query_timeout_info(common::ObIAllocator &alloc,
                              const int64_t duration_timeout);

  int fill_partition_info(ObIAllocator &allocator,
                          const ObTableStatParam &param,
                          const ObExtraParam &extra);

  int add_hint(const ObString &hint_str,
               common::ObIAllocator &alloc);

  int fill_group_by_info(ObIAllocator &allocator,
                         const ObTableStatParam &param,
                         const ObExtraParam &extra,
                         ObString &calc_part_id_str);

  void reset_select_items() { stat_items_.reset(); select_fields_.reset(); }

private:
  int copy_opt_stat(ObOptStat &src_opt_stat,
                    ObIArray<ObOptStat> &dst_opt_stats);

  int copy_hybrid_hist_stat(ObOptStat &src_opt_stat,
                            ObIArray<ObOptStat> &dst_opt_stats);

  int copy_col_stats(const int64_t cur_row_cnt,
                     const int64_t total_row_cnt,
                     ObIArray<ObOptColumnStat *> &src_col_stats,
                     ObIArray<ObOptColumnStat *> &dst_col_stats);

protected:

  ObExecContext &ctx_;
  ObIAllocator &allocator_;

  ObString db_name_;
  ObString from_table_;
  ObString partition_hint_;
  ObSqlString select_fields_;
  ObString sample_hint_;
  ObString other_hints_;
  ObString partition_string_;
  ObString group_by_string_;
  ObString where_string_;

  ObArray<ObStatItem *> stat_items_;
  ObArray<ObObj> results_;
  double sample_value_;
};


}
}

#endif // OB_STATS_ESTIMATOR_H
