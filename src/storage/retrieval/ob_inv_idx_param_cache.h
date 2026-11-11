/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  typedef common::hash::ObHashMap<common::ObTabletID, uint64_t, hash::NoPthreadDefendMode>
      TotalDocCntMap;
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
  int get_total_doc_cnt(
      const common::ObTabletID &tablet_id,
      uint64_t &total_doc_cnt) const;
  int set_total_doc_cnt(
      const common::ObTabletID &tablet_id,
      const uint64_t total_doc_cnt);
private:
  int init_avg_doc_token_cnt_map();
  int init_total_doc_cnt_map();
private:
  static constexpr int64_t DEFAULT_BUCKET_CNT = 16;
  AvgDocTokenCntMap avg_doc_token_cnt_map_;
  TotalDocCntMap total_doc_cnt_map_;
  bool avg_doc_token_cnt_map_inited_;
  bool total_doc_cnt_map_inited_;
};

} // namespace storage
} // namespace oceanbase

#endif