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

#ifndef OB_MIN_MAX_ESTIMATOR_H
#define OB_MIN_MAX_ESTIMATOR_H

#include "share/stat/ob_stat_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/stat/ob_basic_stats_estimator.h"

namespace oceanbase
{
namespace common
{

class ObStatMinMaxSubquery : public ObStatColItem
{
public:
  ObStatMinMaxSubquery() {}
  ObStatMinMaxSubquery(const ObColumnStatParam *param,
                       ObOptColumnStat *stat,
                       const ObString &db_name,
                       const ObString &from_table,
                       const ObString &partition,
                       bool is_min) :
    ObStatColItem(param, stat),
    db_name_(db_name),
    from_table_(from_table),
    partition_string_(partition),
    is_min_(is_min)
  {}
  virtual ~ObStatMinMaxSubquery() {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
  virtual bool is_needed() const override { return col_param_ != NULL && col_param_->need_refine_min_max(); }
private:
  ObString db_name_;
  ObString from_table_;
  ObString partition_string_;
  bool is_min_;
};

class ObMinMaxEstimator : public ObBasicStatsEstimator
{
public:
  explicit ObMinMaxEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObOptStatGatherParam &param,
               ObOptStat &opt_stat);

private:
  int add_min_max_stat_items(ObIAllocator &allocator,
                             const ObOptStatGatherParam &param,
                             const ObIArray<ObColumnStatParam> &column_params,
                             ObOptStat &opt_stat);

  int pack_sql(ObSqlString &raw_sql_str);
};

} // end of namespace common
} // end of namespace oceanbase

#endif /*#endif OB_MIN_MAX_ESTIMATOR_H */
