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

void ObLibCacheAtomicOp::operator()(LibCacheKV &entry)
{
  if (NULL != entry.second) {
    entry.second->inc_ref_count(ref_handle_);
    cache_node_ = entry.second;
    SQL_PC_LOG(DEBUG, "succ to get cache_node", "ref_count", cache_node_->get_ref_count());
  } else {
    // if no cache node found, no need to do anything now
  }
}

//get cache node and lock
int ObLibCacheAtomicOp::get_value(ObILibCacheNode *&cache_node)
{
  int ret = OB_SUCCESS;
  cache_node = NULL;
  if (OB_ISNULL(cache_node_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "invalid argument", K(cache_node_));
  } else if (OB_SUCC(lock(*cache_node_))) {
    cache_node = cache_node_;
  } else {
    if (NULL != cache_node_) {
      cache_node_->dec_ref_count(ref_handle_);
    }
    SQL_PC_LOG(DEBUG, "failed to get read lock of lib cache value", K(ret));
  }
  return ret;
}

/*
worker thread:                   |  evict thread
                                 |  get all plan id array(contains plan id x)
deleting .... remove plan id x   |
from map                         |
dec ref cnt => ref_cnt=0         |
                                 | ref plan id x. inc_ref=1
deleting plan x                  |
                                 | access plan x --> cause core!
*/

void ObCacheObjAtomicOp::operator()(ObjKV &entry)
{
  if (NULL != entry.second) {
    int64_t ref_cnt = entry.second->inc_ref_count(ref_handle_);
    if (ref_cnt > 1) {
      cache_obj_ = entry.second;
    } else {
      cache_obj_ = nullptr;
    }
    SQL_PC_LOG(DEBUG, "succ to get plan");
  } else {
    // do nothing
  }
}
