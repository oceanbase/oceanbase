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

#define USING_LOG_PREFIX SQL_PC
#include "ob_plan_cache_callback.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_cache.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

void ObPlanCacheAtomicOp::operator()(PlanCacheKV& entry)
{
  if (NULL != entry.second) {
    entry.second->inc_ref_count(ref_handle_);
    pcv_set_ = entry.second;
    SQL_PC_LOG(DEBUG, "succ to get pcv_set", "ref_count", pcv_set_->get_ref_count());
  } else {
    // if no pcv set found, no need to do anything now
  }
}

// get pcvs and lock
int ObPlanCacheAtomicOp::get_value(ObPCVSet*& pcvs)
{
  int ret = OB_SUCCESS;
  pcvs = NULL;
  if (OB_ISNULL(pcv_set_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "invalid argument", K(pcv_set_));
  } else if (OB_SUCC(lock(*pcv_set_))) {
    pcvs = pcv_set_;
  } else {
    if (NULL != pcv_set_) {
      pcv_set_->dec_ref_count(ref_handle_);
    }
    SQL_PC_LOG(DEBUG, "failed to get read lock of plan cache value", K(ret));
  }
  return ret;
}

void ObCacheObjAtomicOp::operator()(ObjKV& entry)
{
  if (NULL != entry.second) {
    cache_obj_ = entry.second;
    cache_obj_->inc_ref_count(ref_handle_);
    SQL_PC_LOG(DEBUG, "succ to get plan", "ref_count", cache_obj_->get_ref_count());
  } else {
    // do nothing
  }
}
