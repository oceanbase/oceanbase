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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_WRITE_CACHE_SHRINK_CTX_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_WRITE_CACHE_SHRINK_CTX_H_

#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "deps/oblib/src/lib/list/ob_list.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"

namespace oceanbase
{
namespace tmp_file
{

struct WriteCacheShrinkContext
{
  friend class ObTmpFileWriteCache;
public:
  static const int64_t MAX_SHRINKING_DURATION = 5 * 60 * 1000 * 1000; // 5min
  static const int64_t SHRINKING_PERIOD = 5 * 60 * 1000 * 1000; // 5min
  static const int32_t AUTO_SHRINKING_WATERMARK_L1 = 10;
  static const int32_t AUTO_SHRINKING_WATERMARK_L2 = 5;
  static const int32_t AUTO_SHRINKING_WATERMARK_L3 = 0;
  static const int32_t AUTO_SHRINKING_TARGET_SIZE_L1 = 50; // 50%
  static const int32_t AUTO_SHRINKING_TARGET_SIZE_L2 = 20; // 20%
  static const int32_t AUTO_SHRINKING_TARGET_SIZE_L3 = 10; // 10%
  enum SHRINK_STATE
  {
    INVALID = 0,
    SHRINKING_SWAP,
    SHRINKING_RELEASE_BLOCKS,
    SHRINKING_FINISH
  };
  struct JudgeInShrinkRangeFunctor
  {
    JudgeInShrinkRangeFunctor(WriteCacheShrinkContext &shrink_ctx) : shrink_ctx_(shrink_ctx) {}
    bool operator()(PageNode *node) { return shrink_ctx_.in_shrinking_range(node); }
  private:
    WriteCacheShrinkContext &shrink_ctx_;
  };
public:
  WriteCacheShrinkContext();
  ~WriteCacheShrinkContext() { reset(); }
  int init(const uint32_t lower_page_id,
           const uint32_t upper_page_id,
           const bool is_auto);
  void reset();
  bool is_valid();
  int64_t get_not_alloc_page_num();
  bool is_execution_too_long() const;
  bool is_auto() const { return is_auto_; }
  bool in_not_alloc_range(uint32_t page_id);
  bool in_shrinking_range(PageNode *node);
  static int64_t get_auto_shrinking_percent(const int64_t current_watermark);
  TO_STRING_KV(K(is_inited_), K(is_auto_),
               K(lower_page_id_), K(max_allow_alloc_page_id_),
               K(upper_page_id_), K(shrink_state_),
               K(shrink_begin_ts_));
private:
  bool is_inited_;
  // auto shrinking may be aborted if watermark increases.
  bool is_auto_;
  SHRINK_STATE shrink_state_;
  // the pages in the range [max_allow_alloc_page_id_ + 1, upper_page_id_]
  // are not allowed to be allocated during shrinking.
  uint32_t max_allow_alloc_page_id_;
  uint32_t lower_page_id_;
  uint32_t upper_page_id_;
  int64_t current_max_used_watermark_;
  int64_t last_shrink_complete_ts_;
  int64_t shrink_begin_ts_;
  ObSpinLock shrink_lock_;
  // pages in this list are excluded from allocation by alloc_page()
  ObDList<PageNode> shrink_list_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
