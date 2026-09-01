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

#ifndef OCEANBASE_PL_OB_JAVA_SANDBOX_CHANNEL_H_
#define OCEANBASE_PL_OB_JAVA_SANDBOX_CHANNEL_H_

#include "pl/external_routine/ob_java_sandbox_frame.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_mutex.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/thread/thread_pool.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace pl
{

class ObJavaChannel {
public:
  ObJavaChannel() : fd_(-1), next_req_id_(1) {}
  ~ObJavaChannel();

  void set_fd(int fd) { fd_ = fd; }
  void close();
  int get_fd() const { return fd_; }

  // Send a frame with auto-assigned req_id. Returns the req_id used.
  int send_frame(uint8_t msg_type, const char *payload, int64_t payload_len, uint32_t &req_id);

  // Send a frame with a specific req_id (for responding to callbacks).
  int send_frame_with_req_id(uint32_t req_id, uint8_t msg_type,
                              const char *payload, int64_t payload_len);

  // Read a frame with timeout. Allocates payload from alloc.
  int recv_frame(int64_t timeout_us, ObJavaSandboxFrameHeader &header,
                  common::ObIAllocator &alloc, char *&payload);

  // Read a frame matching expected_req_id, discarding stale frames from
  // previous timed-out requests. Uses deadline-based timeout across retries.
  int recv_frame_matched(uint32_t expected_req_id, int64_t timeout_us,
                          ObJavaSandboxFrameHeader &header,
                          common::ObIAllocator &alloc, char *&payload);

  TO_STRING_KV(K_(fd), K_(next_req_id));

private:
  int fd_;
  uint32_t next_req_id_;

  DISALLOW_COPY_AND_ASSIGN(ObJavaChannel);
};

class ObJavaChannelPool : public lib::ThreadPool {
public:
  static constexpr int64_t DEFAULT_POOL_SIZE = 120;  // minijail MAX_PRESERVED_FDS=128, minus stdio=3 and margin

  ObJavaChannelPool();
  ~ObJavaChannelPool();

  int init(int64_t pool_size, uint64_t tenant_id);
  void destroy();

  // Wait until all acquired channels are released back to the pool.
  // Spins with usleep; intended for use before destroy() under launch_mutex_.
  void wait_all_released(int64_t timeout_us = 5000000);

  // Acquire a free channel. Blocks if none available.
  int acquire(ObJavaChannel *&channel);

  // Release a channel back to the pool.
  void release(ObJavaChannel *channel);

  // Isolate a channel whose request timed out. The reaper waits for the
  // expected terminal response and returns the channel to the free list only
  // after the old response has been fully consumed.
  void quarantine(ObJavaChannel *channel, uint32_t expected_req_id,
                  uint8_t expected_resp_type, int64_t ctx_id, char scope);

  // Permanently remove a channel whose request did not reach a terminal
  // response. The peer may still write a stale response, so it must never be
  // returned to the free list.
  void discard(ObJavaChannel *channel);

  // Collect child-side fds for SCM_RIGHTS passing.
  int get_child_fds(common::ObIArray<int> &fds) const;

  // Close child-side fds (after sandbox has inherited them).
  void close_child_fds();

  bool is_inited() const { return is_inited_; }
  bool needs_rollover() const { return 0 != ATOMIC_LOAD(&rollover_required_); }

private:
  enum ChannelState {
    CHANNEL_FREE = 0,
    CHANNEL_ACQUIRED,
    CHANNEL_QUARANTINED,
    CHANNEL_REAPING,
    CHANNEL_DISCARDED
  };

  struct ChannelSlot {
    int observer_fd_;
    int child_fd_;
    ObJavaChannel channel_;
    ChannelState state_;
    uint32_t expected_req_id_;
    uint8_t expected_resp_type_;
    int64_t ctx_id_;
    char scope_;
    int64_t quarantine_deadline_ts_;
    ChannelSlot()
      : observer_fd_(-1),
        child_fd_(-1),
        state_(CHANNEL_FREE),
        expected_req_id_(0),
        expected_resp_type_(0),
        ctx_id_(0),
        scope_(0),
        quarantine_deadline_ts_(0)
    {}
    TO_STRING_KV(K_(observer_fd), K_(child_fd), K_(state),
                 K_(expected_req_id), K_(expected_resp_type), K_(ctx_id), K_(scope),
                 K_(quarantine_deadline_ts));
  };

  void run1() override;
  ChannelSlot *find_slot_locked(ObJavaChannel *channel);
  void reset_quarantine_locked(ChannelSlot &slot);
  void discard_locked(ChannelSlot &slot);

  common::ObArray<ChannelSlot *> slots_;
  lib::ObMutex mutex_;
  common::ObArray<ObJavaChannel *> free_list_;
  int64_t discarded_count_;
  int64_t discarded_threshold_;
  int64_t rollover_required_;
  uint64_t tenant_id_;
  bool is_inited_;
  common::ObArenaAllocator alloc_;

  DISALLOW_COPY_AND_ASSIGN(ObJavaChannelPool);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_PL_OB_JAVA_SANDBOX_CHANNEL_H_
