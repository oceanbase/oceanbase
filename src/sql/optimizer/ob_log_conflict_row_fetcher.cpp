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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/ob_log_conflict_row_fetcher.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
int ObLogConflictRowFetcher::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  sharding_info_.set_location_type(OB_TBL_LOCATION_ALL);
  return ret;
}

int ObLogConflictRowFetcher::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  UNUSED(type);
  int ret = OB_SUCCESS;
  return ret;
}

uint64_t ObLogConflictRowFetcher::hash(uint64_t seed) const
{
  seed = do_hash(table_id_, seed);
  seed = do_hash(index_tid_, seed);
  seed = ObOptimizerUtil::hash_exprs(seed, conflict_exprs_);
  seed = ObOptimizerUtil::hash_exprs(seed, access_exprs_);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogConflictRowFetcher::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); i++) {
    OZ(raw_exprs.append(static_cast<ObRawExpr*>(access_exprs_.at(i))));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_exprs_.count(); i++) {
    OZ(raw_exprs.append(static_cast<ObRawExpr*>(conflict_exprs_.at(i))));
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
