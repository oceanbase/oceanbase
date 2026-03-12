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

#ifndef OB_INV_IDX_PARAM_CACHE_H_
#define OB_INV_IDX_PARAM_CACHE_H_

#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace storage
{

class ObInvIdxParamCache
{
  typedef common::hash::ObHashMap<common::ObTabletID, double, hash::NoPthreadDefendMode>
      AvgDocTokenCntMap;
public:
  ObInvIdxParamCache();
  ~ObInvIdxParamCache();
  void reset();

  int get_avg_doc_token_cnt(
      const common::ObTabletID &tablet_id,
      double &avg_doc_token_cnt) const;
  int set_avg_doc_token_cnt(
      const common::ObTabletID &tablet_id,
      const double avg_doc_token_cnt);
  int get_total_doc_cnt(uint64_t &total_doc_cnt) const;
  int set_total_doc_cnt(const uint64_t total_doc_cnt);
private:
  int init_avg_doc_token_cnt_map();
private:
  static constexpr int64_t DEFAULT_BUCKET_CNT = 16;
  uint64_t total_doc_cnt_;
  AvgDocTokenCntMap avg_doc_token_cnt_map_;
  bool avg_doc_token_cnt_map_inited_;
  bool total_doc_cnt_inited_;
};

} // namespace storage
} // namespace oceanbase

#endif