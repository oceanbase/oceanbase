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

#ifndef OCEANBASE_ALLOCATOR_OB_TX_DATA_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_TX_DATA_ALLOCATOR_H_

#include "lib/allocator/ob_slice_alloc.h"
#include "share/ob_delegate.h"
#include "share/throttle/ob_share_throttle_define.h"
#include "lib/allocator/ob_vslice_alloc.h"

namespace oceanbase {
namespace share {

OB_INLINE int64_t &tx_data_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, tx_data_throttled_alloc);
  return tx_data_throttled_alloc;
}

class ObTenantTxDataAllocator {
public:
  using SliceAllocator = ObSliceAlloc;

  // define some default value
  static const int64_t TX_DATA_LIMIT_PERCENTAGE = 20;
  static const int64_t TX_DATA_THROTTLE_TRIGGER_PERCENTAGE = 60;
  static const int64_t TX_DATA_THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours
  static const int64_t ALLOC_TX_DATA_MAX_CONCURRENCY = 32;
  static const uint32_t THROTTLE_TX_DATA_INTERVAL = 20 * 1000; // 20ms

  // The tx data memtable will trigger a freeze if its memory use is more than 2%
  static constexpr double TX_DATA_FREEZE_TRIGGER_PERCENTAGE = 2;

public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(TxData);

public:
  ObTenantTxDataAllocator()
      : is_inited_(false), throttle_tool_(nullptr), block_alloc_(), slice_allocator_() {}
  ~ObTenantTxDataAllocator() { reset(); }
  int init(const char* label);
  void *alloc(const bool enable_throttle = true, const int64_t abs_expire_time = 0);
  void reset();
  int64_t hold() const { return block_alloc_.hold(); }

  DELEGATE_WITH_RET(slice_allocator_, free, void);

  TO_STRING_KV(K(is_inited_), KP(throttle_tool_), KP(&block_alloc_), KP(&slice_allocator_));

private:
  bool is_inited_;
  TxShareThrottleTool *throttle_tool_;
  common::ObBlockAllocMgr block_alloc_;
  SliceAllocator slice_allocator_;
};

class ObTxDataThrottleGuard
{
public:
  ObTxDataThrottleGuard(const bool for_replay, const int64_t abs_expire_time);
  ~ObTxDataThrottleGuard();

private:
  bool for_replay_;
  int64_t abs_expire_time_;
  share::TxShareThrottleTool *throttle_tool_;
};

class ObTenantTxDataOpAllocator : public ObIAllocator {
private:
  static const int64_t MDS_ALLOC_CONCURRENCY = 32;
public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(Mds);

public:
  ObTenantTxDataOpAllocator() : is_inited_(false), throttle_tool_(nullptr), block_alloc_(), allocator_() {}

  int init();
  void destroy() { is_inited_ = false; }
  void *alloc(const int64_t size, const int64_t expire_ts);
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void set_attr(const ObMemAttr &attr) override;
  int64_t hold() { return allocator_.hold(); }
  int64_t get_local_alloc_size() { return local_alloc_size_; }
  void reset_local_alloc_size() { local_alloc_size_ = 0; }
  TO_STRING_KV(K(is_inited_), KP(this), KP(throttle_tool_), KP(&block_alloc_), KP(&allocator_));

private:
  bool is_inited_;
  share::TxShareThrottleTool *throttle_tool_;
  common::ObBlockAllocMgr block_alloc_;
  common::ObVSliceAlloc allocator_;
  static thread_local int64_t local_alloc_size_;
};
}  // namespace share
}  // namespace oceanbase

#endif
