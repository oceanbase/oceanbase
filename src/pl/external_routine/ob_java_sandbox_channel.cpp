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

#include "pl/external_routine/ob_java_sandbox_channel.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/rc/ob_tenant_base.h"

#define USING_LOG_PREFIX PL
#include <errno.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace oceanbase
{
namespace pl
{

static const int64_t JAVA_SANDBOX_QUARANTINE_TIMEOUT_US = 5 * 1000 * 1000L;
static const int64_t JAVA_SANDBOX_REAPER_RECV_TIMEOUT_US = 100 * 1000L;
static const int64_t JAVA_SANDBOX_REAPER_IDLE_US = 10 * 1000L;
static const int64_t JAVA_SANDBOX_DISCARDED_THRESHOLD_PERCENT = 10;

static void encode_int64_be(char *buf, int64_t value)
{
  uint64_t unsigned_value = static_cast<uint64_t>(value);
  for (int64_t i = 0; i < 8; ++i) {
    buf[7 - i] = static_cast<char>(unsigned_value & 0xff);
    unsigned_value >>= 8;
  }
}

// ---- ObJavaChannel ----

ObJavaChannel::~ObJavaChannel()
{
  close();
}

void ObJavaChannel::close()
{
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

int ObJavaChannel::send_frame(uint8_t msg_type, const char *payload,
                               int64_t payload_len, uint32_t &req_id)
{
  req_id = next_req_id_++;
  return ob_java_sandbox_send_frame(fd_, req_id, msg_type, payload, payload_len);
}

int ObJavaChannel::send_frame_with_req_id(uint32_t req_id, uint8_t msg_type,
                                           const char *payload, int64_t payload_len)
{
  return ob_java_sandbox_send_frame(fd_, req_id, msg_type, payload, payload_len);
}

int ObJavaChannel::recv_frame(int64_t timeout_us, ObJavaSandboxFrameHeader &header,
                               common::ObIAllocator &alloc, char *&payload)
{
  return ob_java_sandbox_recv_frame(fd_, timeout_us, header, alloc, payload);
}

int ObJavaChannel::recv_frame_matched(uint32_t expected_req_id, int64_t timeout_us,
                                       ObJavaSandboxFrameHeader &header,
                                       common::ObIAllocator &alloc, char *&payload)
{
  int ret = common::OB_SUCCESS;
  const int64_t deadline = common::ObTimeUtility::current_time() + timeout_us;
  while (OB_SUCC(ret)) {
    int64_t remaining = deadline - common::ObTimeUtility::current_time();
    if (remaining <= 0) {
      ret = common::OB_TIMEOUT;
      break;
    }
    if (OB_FAIL(recv_frame(remaining, header, alloc, payload))) {
      break;
    }
    if (header.req_id_ == expected_req_id) {
      break;
    }
    LOG_TRACE("discarding stale frame", K(header.req_id_), K(expected_req_id),
              K(header.msg_type_), K_(fd));
    if (OB_NOT_NULL(payload)) {
      alloc.free(payload);
    }
    payload = nullptr;
  }
  return ret;
}

// ---- ObJavaChannelPool ----

ObJavaChannelPool::ObJavaChannelPool()
  : lib::ThreadPool(1),
    mutex_(common::ObLatchIds::SANDBOX_LOCK),
    discarded_count_(0),
    discarded_threshold_(1),
    rollover_required_(0),
    tenant_id_(common::OB_SERVER_TENANT_ID),
    is_inited_(false),
    alloc_(common::ObMemAttr(common::OB_SERVER_TENANT_ID, "JavaSbChannel"))
    // alloc_ label is re-set in init() with the actual tenant_id
{
}

ObJavaChannelPool::~ObJavaChannelPool()
{
  destroy();
}

int ObJavaChannelPool::init(int64_t pool_size, uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
  } else if (OB_UNLIKELY(pool_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    discarded_count_ = 0;
    discarded_threshold_ =
        (pool_size * JAVA_SANDBOX_DISCARDED_THRESHOLD_PERCENT + 99) / 100;
    rollover_required_ = 0;
    tenant_id_ = tenant_id;
    alloc_.set_attr(common::ObMemAttr(tenant_id, "JavaSbChannel"));
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_size; ++i) {
      int fds[2] = {-1, -1};
      // The JVM endpoint is passed explicitly through SCM_RIGHTS later. Mark
      // both local descriptors CLOEXEC so a lazily exec'd ob_sandbox daemon
      // cannot retain accidental copies of either endpoint.
      if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) < 0) {
        ret = common::OB_ERR_SYS;
        LOG_WARN("socketpair failed", K(ret), K(errno), K(i));
      } else {
        void *buf = alloc_.alloc(sizeof(ChannelSlot));
        if (OB_ISNULL(buf)) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          ::close(fds[0]);
          ::close(fds[1]);
        } else {
          ChannelSlot *slot = new (buf) ChannelSlot();
          slot->observer_fd_ = fds[0];
          slot->child_fd_ = fds[1];
          slot->channel_.set_fd(fds[0]);
          if (OB_FAIL(slots_.push_back(slot))) {
            ::close(fds[0]);
            ::close(fds[1]);
            slot->~ChannelSlot();
          } else if (OB_FAIL(free_list_.push_back(&slot->channel_))) {
            // slot already in slots_, will be cleaned up in destroy()
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      lib::ThreadPool::set_run_wrapper(MTL_CTX());
      if (OB_FAIL(lib::ThreadPool::init())) {
        LOG_WARN("init java sandbox channel reaper failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(lib::ThreadPool::start())) {
        LOG_WARN("start java sandbox channel reaper failed", K(ret), K(tenant_id));
      } else {
        LOG_INFO("java sandbox channel pool inited",
                 K(pool_size), K_(discarded_threshold), K(tenant_id));
      }
    }
    if (OB_FAIL(ret)) {
      destroy();
    }
  }
  return ret;
}

void ObJavaChannelPool::destroy()
{
  lib::ThreadPool::stop();
  lib::ThreadPool::wait();
  lib::ThreadPool::destroy();

  lib::ObMutexGuard guard(mutex_);
  free_list_.reset();
  for (int64_t i = 0; i < slots_.count(); ++i) {
    ChannelSlot *slot = slots_.at(i);
    if (OB_NOT_NULL(slot)) {
      if (slot->child_fd_ >= 0) {
        ::close(slot->child_fd_);
        slot->child_fd_ = -1;
      }
      slot->~ChannelSlot();
    }
  }
  slots_.reset();
  discarded_count_ = 0;
  discarded_threshold_ = 1;
  rollover_required_ = 0;
  tenant_id_ = common::OB_SERVER_TENANT_ID;
  is_inited_ = false;
}

void ObJavaChannelPool::wait_all_released(int64_t timeout_us)
{
  int ret = common::OB_SUCCESS;
  UNUSED(ret);
  const int64_t start = common::ObTimeUtility::current_time();
  while (true) {
    bool all_released = true;
    {
      lib::ObMutexGuard guard(mutex_);
      for (int64_t i = 0; all_released && i < slots_.count(); ++i) {
        ChannelSlot *slot = slots_.at(i);
        all_released = OB_ISNULL(slot) || CHANNEL_ACQUIRED != slot->state_;
      }
      if (!is_inited_ || all_released) {
        break;
      }
    }
    if (common::ObTimeUtility::current_time() - start > timeout_us) {
      LOG_WARN("wait_all_released timeout, force proceed",
               "free", free_list_.count(), "total", slots_.count());
      break;
    }
    usleep(1000);
  }
}

int ObJavaChannelPool::acquire(ObJavaChannel *&channel)
{
  int ret = common::OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  channel = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (free_list_.count() <= 0) {
    ret = common::OB_SIZE_OVERFLOW;
    LOG_WARN("java sandbox channel pool exhausted", K(ret));
  } else {
    channel = free_list_.at(free_list_.count() - 1);
    free_list_.pop_back();
    ChannelSlot *slot = find_slot_locked(channel);
    if (OB_ISNULL(slot) || CHANNEL_FREE != slot->state_) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid java sandbox channel state on acquire",
                K(ret), KP(channel), KP(slot));
      channel = nullptr;
    } else {
      slot->state_ = CHANNEL_ACQUIRED;
    }
  }
  return ret;
}

void ObJavaChannelPool::release(ObJavaChannel *channel)
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(channel)) {
    lib::ObMutexGuard guard(mutex_);
    ChannelSlot *slot = find_slot_locked(channel);
    if (OB_ISNULL(slot)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_ERROR("java sandbox channel does not belong to pool", K(ret), KP(channel));
    } else if (CHANNEL_ACQUIRED != slot->state_ || channel->get_fd() < 0) {
      ret = common::OB_STATE_NOT_MATCH;
      LOG_ERROR("attempt to release unavailable java sandbox channel",
                K(ret), KP(channel), KPC(slot));
    } else if (OB_SUCC(ret)) {
      ret = free_list_.push_back(channel);
      if (OB_UNLIKELY(common::OB_SUCCESS != ret)) {
        LOG_ERROR("failed to return channel to pool", K(ret));
        discard_locked(*slot);
      } else {
        slot->state_ = CHANNEL_FREE;
      }
    }
  }
}

void ObJavaChannelPool::quarantine(ObJavaChannel *channel,
                                   uint32_t expected_req_id,
                                   uint8_t expected_resp_type,
                                   int64_t ctx_id,
                                   char scope)
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(channel)) {
    lib::ObMutexGuard guard(mutex_);
    ChannelSlot *slot = find_slot_locked(channel);
    if (OB_ISNULL(slot)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_ERROR("java sandbox channel does not belong to pool", K(ret), KP(channel));
    } else if (CHANNEL_ACQUIRED != slot->state_ || channel->get_fd() < 0) {
      ret = common::OB_STATE_NOT_MATCH;
      LOG_ERROR("attempt to quarantine unavailable java sandbox channel",
                K(ret), KP(channel), KPC(slot));
    } else {
      slot->state_ = CHANNEL_QUARANTINED;
      slot->expected_req_id_ = expected_req_id;
      slot->expected_resp_type_ = expected_resp_type;
      slot->ctx_id_ = ctx_id;
      slot->scope_ = scope;
      slot->quarantine_deadline_ts_ =
          common::ObTimeUtility::current_time() + JAVA_SANDBOX_QUARANTINE_TIMEOUT_US;
      LOG_WARN("quarantine java sandbox channel after request timeout",
               KP(channel), K(expected_req_id), K(expected_resp_type), K(ctx_id), K(scope),
               K(slot->quarantine_deadline_ts_));
    }
  }
}

void ObJavaChannelPool::discard(ObJavaChannel *channel)
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(channel)) {
    lib::ObMutexGuard guard(mutex_);
    ChannelSlot *slot = find_slot_locked(channel);
    if (OB_ISNULL(slot)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_ERROR("java sandbox channel does not belong to pool", K(ret), KP(channel));
    } else {
      discard_locked(*slot);
    }
  }
}

ObJavaChannelPool::ChannelSlot *ObJavaChannelPool::find_slot_locked(
    ObJavaChannel *channel)
{
  ChannelSlot *result = nullptr;
  for (int64_t i = 0; OB_ISNULL(result) && i < slots_.count(); ++i) {
    ChannelSlot *slot = slots_.at(i);
    if (OB_NOT_NULL(slot) && &slot->channel_ == channel) {
      result = slot;
    }
  }
  return result;
}

void ObJavaChannelPool::reset_quarantine_locked(ChannelSlot &slot)
{
  slot.expected_req_id_ = 0;
  slot.expected_resp_type_ = 0;
  slot.ctx_id_ = 0;
  slot.scope_ = 0;
  slot.quarantine_deadline_ts_ = 0;
}

void ObJavaChannelPool::discard_locked(ChannelSlot &slot)
{
  int ret = common::OB_SUCCESS;
  UNUSED(ret);
  if (CHANNEL_DISCARDED != slot.state_) {
    slot.channel_.close();
    slot.observer_fd_ = -1;
    slot.state_ = CHANNEL_DISCARDED;
    reset_quarantine_locked(slot);
    ++discarded_count_;
    if (discarded_count_ >= discarded_threshold_
        && 0 == ATOMIC_LOAD(&rollover_required_)) {
      ATOMIC_STORE(&rollover_required_, 1);
      LOG_WARN("java sandbox discarded channel threshold reached, request JVM rollover",
               K_(discarded_count), K_(discarded_threshold));
    }
    LOG_WARN("discard java sandbox channel without terminal response",
             K_(discarded_count), K_(discarded_threshold));
  }
}

void ObJavaChannelPool::run1()
{
  int ret = common::OB_SUCCESS;
  lib::set_thread_name("JavaSbReaper");
  while (!has_set_stop()) {
    ChannelSlot *target = nullptr;
    bool response_ready = false;
    bool quarantine_expired = false;
    uint32_t expected_req_id = 0;
    uint8_t expected_resp_type = 0;
    int64_t ctx_id = 0;
    char scope = 0;
    int64_t quarantine_deadline_ts = 0;
    {
      lib::ObMutexGuard guard(mutex_);
      const int64_t now = common::ObTimeUtility::current_time();
      for (int64_t i = 0; OB_ISNULL(target) && i < slots_.count(); ++i) {
        ChannelSlot *slot = slots_.at(i);
        if (OB_NOT_NULL(slot) && CHANNEL_QUARANTINED == slot->state_) {
          struct pollfd pfd;
          pfd.fd = slot->channel_.get_fd();
          pfd.events = POLLIN;
          pfd.revents = 0;
          int poll_ret = 0;
          do {
            poll_ret = ::poll(&pfd, 1, 0);
          } while (poll_ret < 0 && EINTR == errno);
          response_ready = poll_ret > 0
                           && 0 != (pfd.revents
                                   & (POLLIN | POLLERR | POLLHUP | POLLNVAL));
          quarantine_expired = now >= slot->quarantine_deadline_ts_;
          if (response_ready || quarantine_expired) {
            target = slot;
            target->state_ = CHANNEL_REAPING;
            expected_req_id = target->expected_req_id_;
            expected_resp_type = target->expected_resp_type_;
            ctx_id = target->ctx_id_;
            scope = target->scope_;
            quarantine_deadline_ts = target->quarantine_deadline_ts_;
          }
        }
      }
    }

    if (OB_ISNULL(target)) {
      usleep(JAVA_SANDBOX_REAPER_IDLE_US);
    } else if (quarantine_expired && !response_ready) {
      lib::ObMutexGuard guard(mutex_);
      if (CHANNEL_REAPING == target->state_) {
        discard_locked(*target);
      }
    } else {
      common::ObArenaAllocator tmp_alloc(
          common::ObMemAttr(tenant_id_, "JavaSbReap"));
      ObJavaSandboxFrameHeader header;
      char *payload = nullptr;
      ret = target->channel_.recv_frame_matched(
          expected_req_id,
          JAVA_SANDBOX_REAPER_RECV_TIMEOUT_US,
          header,
          tmp_alloc,
          payload);
      bool terminal_response =
          common::OB_SUCCESS == ret
          && expected_resp_type == header.msg_type_;

      if (terminal_response
          && JAVA_SANDBOX_LOAD_JAR_RESP == expected_resp_type
          && 0 == scope
          && 0 != ctx_id) {
        char evict_payload[8];
        encode_int64_be(evict_payload, ctx_id);
        uint32_t evict_req_id = 0;
        if (common::OB_SUCCESS !=
            (ret = target->channel_.send_frame(
                JAVA_SANDBOX_EVICT_STATEMENT,
                evict_payload,
                sizeof(evict_payload),
                evict_req_id))) {
          terminal_response = false;
          LOG_WARN("send EVICT_STATEMENT on recovered channel failed",
                   K(ret), K(ctx_id), K(evict_req_id));
        }
      }

      lib::ObMutexGuard guard(mutex_);
      if (CHANNEL_REAPING == target->state_) {
        if (terminal_response) {
          reset_quarantine_locked(*target);
          if (common::OB_SUCCESS !=
              (ret = free_list_.push_back(&target->channel_))) {
            LOG_WARN("return recovered java sandbox channel failed", K(ret));
            discard_locked(*target);
          } else {
            target->state_ = CHANNEL_FREE;
            LOG_INFO("recovered quarantined java sandbox channel",
                     K(expected_req_id), K(expected_resp_type), K(ctx_id), K(scope));
          }
        } else if (common::OB_TIMEOUT == ret
                   && common::ObTimeUtility::current_time()
                      < quarantine_deadline_ts) {
          target->state_ = CHANNEL_QUARANTINED;
        } else {
          discard_locked(*target);
        }
      }
    }
  }
}

int ObJavaChannelPool::get_child_fds(common::ObIArray<int> &fds) const
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < slots_.count(); ++i) {
    if (OB_FAIL(fds.push_back(slots_.at(i)->child_fd_))) {
      LOG_WARN("push child fd failed", K(ret), K(i));
    }
  }
  return ret;
}

void ObJavaChannelPool::close_child_fds()
{
  for (int64_t i = 0; i < slots_.count(); ++i) {
    ChannelSlot *slot = slots_.at(i);
    if (OB_NOT_NULL(slot) && slot->child_fd_ >= 0) {
      ::close(slot->child_fd_);
      slot->child_fd_ = -1;
    }
  }
}

} // namespace pl
} // namespace oceanbase
