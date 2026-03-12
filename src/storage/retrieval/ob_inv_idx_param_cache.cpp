/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/hash/ob_hashutils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/retrieval/ob_inv_idx_param_cache.h"


namespace oceanbase
{
namespace storage
{

ObInvIdxParamCache::ObInvIdxParamCache()
  : total_doc_cnt_(0),
    avg_doc_token_cnt_map_(),
    avg_doc_token_cnt_map_inited_(false),
    total_doc_cnt_inited_(false)
{
}

ObInvIdxParamCache::~ObInvIdxParamCache()
{
  reset();
}

void ObInvIdxParamCache::reset()
{
  if (avg_doc_token_cnt_map_.created()) {
    avg_doc_token_cnt_map_.destroy();
  }
  total_doc_cnt_ = 0;
  avg_doc_token_cnt_map_inited_ = false;
  total_doc_cnt_inited_ = false;
}

int ObInvIdxParamCache::get_avg_doc_token_cnt(
    const common::ObTabletID &tablet_id,
    double &avg_doc_token_cnt) const
{
  int ret = OB_SUCCESS;
  avg_doc_token_cnt = 0.0;
  if (OB_UNLIKELY(!avg_doc_token_cnt_map_inited_)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(avg_doc_token_cnt_map_.get_refactored(tablet_id, avg_doc_token_cnt))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("failed to get avg doc token cnt", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObInvIdxParamCache::set_avg_doc_token_cnt(
    const common::ObTabletID &tablet_id,
    const double avg_doc_token_cnt)
{
  int ret = OB_SUCCESS;
  if (avg_doc_token_cnt_map_inited_) {
    // skip
  } else if (OB_FAIL(init_avg_doc_token_cnt_map())) {
    LOG_WARN("failed to init avg doc token cnt map", K(ret));
  }

  if (FAILEDx(avg_doc_token_cnt_map_.set_refactored(tablet_id, avg_doc_token_cnt))) {
    LOG_WARN("failed to set avg doc token cnt", K(ret), K(tablet_id), K(avg_doc_token_cnt));
  }
  return ret;
}

int ObInvIdxParamCache::get_total_doc_cnt(uint64_t &total_doc_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!total_doc_cnt_inited_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("total doc cnt not inited", K(ret));
  } else {
    total_doc_cnt = total_doc_cnt_;
  }
  return ret;
}

int ObInvIdxParamCache::set_total_doc_cnt(const uint64_t total_doc_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_doc_cnt_inited_)) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("total doc cnt already set", K(ret), K(total_doc_cnt), K_(total_doc_cnt));
  } else {
    total_doc_cnt_ = total_doc_cnt;
    total_doc_cnt_inited_ = true;
  }
  return ret;
}

int ObInvIdxParamCache::init_avg_doc_token_cnt_map()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(avg_doc_token_cnt_map_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("avg doc token cnt map already inited", K(ret));
  } else if (OB_FAIL(avg_doc_token_cnt_map_.create(
      DEFAULT_BUCKET_CNT,
      "AvgDocLenBkt",
      "AvgDocLenNode",
      MTL_ID()))) {
    LOG_WARN("failed to create tablet param map", K(ret));
  } else {
    avg_doc_token_cnt_map_inited_ = true;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
