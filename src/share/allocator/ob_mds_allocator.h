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

#ifndef OCEANBASE_ALLOCATOR_OB_MDS_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_MDS_ALLOCATOR_H_

#include "lib/allocator/ob_vslice_alloc.h"
#include "share/throttle/ob_share_throttle_define.h"

namespace oceanbase {

namespace share {

OB_INLINE int64_t &mds_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, mds_throttled_alloc);
  return mds_throttled_alloc;
}

class ObTenantMdsAllocator : public ObIAllocator {
private:
  static const int64_t MDS_ALLOC_CONCURRENCY = 32;
public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(Mds);

public:
  ObTenantMdsAllocator() : is_inited_(false), throttle_tool_(nullptr), block_alloc_(), allocator_() {}

  int init();
  void destroy() { is_inited_ = false; }
  void *alloc(const int64_t size, const int64_t expire_ts);
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void set_attr(const ObMemAttr &attr) override;
  int64_t hold() { return allocator_.hold(); }
  TO_STRING_KV(K(is_inited_), KP(this), KP(throttle_tool_), KP(&block_alloc_), KP(&allocator_));

private:
  bool is_inited_;
  share::TxShareThrottleTool *throttle_tool_;
  common::ObBlockAllocMgr block_alloc_;
  common::ObVSliceAlloc allocator_;

};

struct ObTenantBufferCtxAllocator : public ObIAllocator// for now, it is just a wrapper of mtl_malloc
{
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void set_attr(const ObMemAttr &) override {}
};

class ObMdsThrottleGuard
{
public:
  ObMdsThrottleGuard(const bool for_replay, const int64_t abs_expire_time);
  ~ObMdsThrottleGuard();

private:
  bool for_replay_;
  int64_t abs_expire_time_;
  share::TxShareThrottleTool *throttle_tool_;
};

}  // namespace share
}  // namespace oceanbase

#endif