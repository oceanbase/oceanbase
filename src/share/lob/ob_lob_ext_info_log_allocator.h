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

#ifndef OCEANBASE_ALLOCATOR_OB_LOB_EXT_INFO_LOG_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_LOB_EXT_INFO_LOG_ALLOCATOR_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "share/throttle/ob_share_throttle_define.h"

namespace oceanbase {
namespace share {

class ObLobExtInfoLogAllocator; 
DEFINE_SHARE_THROTTLE(LobExtInfoLog, ObLobExtInfoLogAllocator);

OB_INLINE int64_t &lob_ext_info_log_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, lob_ext_info_log_throttled_alloc);
  return lob_ext_info_log_throttled_alloc;
}

class ObLobExtInfoLogAllocator : public ObIAllocator {

public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(LobExtInfoLog);

public:
  ObLobExtInfoLogAllocator() : 
    is_inited_(false), throttle_tool_(nullptr), allocator_() {}

  int init(LobExtInfoLogThrottleTool* throttle_tool);
  void destroy() { is_inited_ = false; }
  void *alloc(const int64_t size, const int64_t expire_ts);
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  int64_t hold() const { return allocator_.total(); }
  virtual int64_t total() const { return allocator_.total(); }
  virtual int64_t used() const { return allocator_.used(); }
  TO_STRING_KV(K(is_inited_), KP(this), KP(throttle_tool_), KP(&allocator_));

private:
  bool is_inited_;
  LobExtInfoLogThrottleTool *throttle_tool_;
  common::ObFIFOAllocator allocator_;
};

class ObLobExtInfoLogThrottleGuard
{
public:
  ObLobExtInfoLogThrottleGuard(const int64_t abs_expire_time, LobExtInfoLogThrottleTool* throttle_tool);
  ~ObLobExtInfoLogThrottleGuard();

private:
  int64_t abs_expire_time_;
  LobExtInfoLogThrottleTool *throttle_tool_;
};

}  // namespace share
}  // namespace oceanbase

#endif