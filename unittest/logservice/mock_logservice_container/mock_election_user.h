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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/election/message/election_message.h"
#include "share/ob_occam_timer.h"
#include "lib/utility/serialization.h"
#include <thread>
#include <mutex>
#include <assert.h>
#include <unordered_map>


namespace oceanbase
{
namespace unittest
{

extern int64_t MSG_DELAY;

// 定义用于hash map的判等方法
struct LogStreamKey
{
  LogStreamKey(const common::ObAddr &addr, const int64_t logstream_id) : addr_(addr), logstream_id_(logstream_id) {}
  bool operator==(const LogStreamKey &rhs) const { return addr_ == rhs.addr_ && logstream_id_ == rhs.logstream_id_; }
  const common::ObAddr addr_;
  const int64_t logstream_id_;
  TO_STRING_KV(KP(this), K_(addr), K_(logstream_id))
};

}
}

// 定义用于hash map的hash方法
namespace std
{

template <>
struct hash<std::pair<oceanbase::common::ObAddr, oceanbase::common::ObAddr>>
{
  std::size_t operator()(const std::pair<oceanbase::common::ObAddr, oceanbase::common::ObAddr>& addr) const
  {
    return addr.first.hash() + addr.second.hash();
  }
};

}

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace palf::election;
using namespace std;

// 线程局部的buffer，用于序列化消息
constexpr int BUFFER_SIZE = 1024;
struct MsgBuffer {
  char buffer_[BUFFER_SIZE];
  TO_STRING_KV(K_(buffer));
};
thread_local MsgBuffer TH_BUFFER;

// 全局的timer 和 thread pool，用于模拟网络延迟和接收端的工作线程
ObOccamTimer TIMER;
ObOccamThreadPool THREAD_POOL;

// 这是收发消息的方法
class MockNetService : public ElectionMsgSender
{
public:
  virtual int broadcast(const ElectionPrepareRequestMsg &msg, const common::ObIArray<common::ObAddr> &list) const override
  {
    std::lock_guard<std::mutex> lg(mutex_);
    broadcast_(msg, list);
    // ELECT_LOG(INFO, "send message", K(msg));
    return OB_SUCCESS;
  }
  virtual int broadcast(const ElectionAcceptRequestMsg &msg, const common::ObIArray<common::ObAddr> &list) const override
  {
    std::lock_guard<std::mutex> lg(mutex_);
    broadcast_(msg, list);
    // ELECT_LOG(INFO, "send message", K(msg));
    return OB_SUCCESS;
  }
  virtual int send(const ElectionPrepareResponseMsg &msg) const override
  {
    std::lock_guard<std::mutex> lg(mutex_);
    send_(msg);
    // ELECT_LOG(INFO, "send message", K(msg));
    return OB_SUCCESS;
  }
  virtual int send(const ElectionAcceptResponseMsg &msg) const override
  {
    std::lock_guard<std::mutex> lg(mutex_);
    send_(msg);
    // ELECT_LOG(INFO, "send message", K(msg));
    return OB_SUCCESS;
  }
  virtual int send(const ElectionChangeLeaderMsg &msg) const override
  {
    std::lock_guard<std::mutex> lg(mutex_);
    send_(msg);
    // ELECT_LOG(INFO, "send message", K(msg));
    return OB_SUCCESS;
  }
  void decode_and_process_buffer(const MsgBuffer &buffer, Election *election_) const
  {
    int64_t msg_type;
    int64_t pos = 0;
    serialization::decode(buffer.buffer_, BUFFER_SIZE, pos, msg_type);
    ElectionMsgType type = static_cast<ElectionMsgType>(msg_type);
    switch (type) {
    case ElectionMsgType::PREPARE_REQUEST:
    {
      ElectionPrepareRequestMsg msg;
      msg.deserialize(buffer.buffer_, BUFFER_SIZE, pos);
      // ELECT_LOG(INFO, "receive preapre request", K(msg));
      election_->handle_message(msg);
      break;
    }
    case ElectionMsgType::PREPARE_RESPONSE:
    {
      ElectionPrepareResponseMsg msg;
      msg.deserialize(buffer.buffer_, BUFFER_SIZE, pos);
      // ELECT_LOG(INFO, "receive preapre response", K(msg));
      election_->handle_message(msg);
      break;
    }
    case ElectionMsgType::ACCEPT_REQUEST:
    {
      ElectionAcceptRequestMsg msg;
      msg.deserialize(buffer.buffer_, BUFFER_SIZE, pos);
      election_->handle_message(msg);
      break;
    }
    case ElectionMsgType::ACCEPT_RESPONSE:
    {
      ElectionAcceptResponseMsg msg;
      msg.deserialize(buffer.buffer_, BUFFER_SIZE, pos);
      election_->handle_message(msg);
      break;
    }
    case ElectionMsgType::CHANGE_LEADER:
    {
      ElectionChangeLeaderMsg msg;
      msg.deserialize(buffer.buffer_, BUFFER_SIZE, pos);
      election_->handle_message(msg);
      break;
    }
    default:
      break;
    }
  }
  static void init() {
    share::ObTenantSwitchGuard guard;
    guard.switch_to(OB_SYS_TENANT_ID);
    THREAD_POOL.init_and_start(3);
    TIMER.init_and_start(THREAD_POOL, 1_ms, "election timer");
  }
  void connect(Election *left, Election *right) {
    std::lock_guard<std::mutex> lg(mutex_);
    map_.insert({{left->get_self_addr(), right->get_self_addr()}, right});
    ELECT_LOG(INFO, "connect", K(left->get_self_addr()), K(right->get_self_addr()));
  }
  void connect_two_side(Election *left, Election *right) {
    std::lock_guard<std::mutex> lg(mutex_);
    map_.insert({{left->get_self_addr(), right->get_self_addr()}, right});
    map_.insert({{right->get_self_addr(), left->get_self_addr()}, left});
    ELECT_LOG(INFO, "disconnect two side", K(left->get_self_addr()), K(right->get_self_addr()));
  }
  void disconnect(const Election *left, const Election *right) {
    std::lock_guard<std::mutex> lg(mutex_);
    map_.erase({left->get_self_addr(), right->get_self_addr()});
    ELECT_LOG(INFO, "disconnect", K(left->get_self_addr()), K(right->get_self_addr()));
  }
  void disconnect_two_side(const Election *left, const Election *right) {
    std::lock_guard<std::mutex> lg(mutex_);
    map_.erase({left->get_self_addr(), right->get_self_addr()});
    map_.erase({right->get_self_addr(), left->get_self_addr()});
    ELECT_LOG(INFO, "disconnect two side", K(left->get_self_addr()), K(right->get_self_addr()));
  }
  void clear() {
    std::lock_guard<std::mutex> lg(mutex_);
    map_.clear();
  }
private:
  template <typename MSG>
  void broadcast_(const MSG &msg, const common::ObIArray<common::ObAddr> &list) const {
    for (int64_t idx = 0; idx < list.count(); ++idx) {
      const_cast<MSG &>(msg).set_receiver(list.at(idx));
      send_(msg);
    }
  }
  template <typename MSG>
  void send_(const MSG &msg) const {
    // ELECT_LOG(INFO, "call send message", K(msg));
    int64_t pos = 0;
    serialization::encode(TH_BUFFER.buffer_, BUFFER_SIZE, pos, int64_t(msg.get_msg_type()));
    msg.serialize(TH_BUFFER.buffer_, BUFFER_SIZE, pos);
    auto iter = map_.find({msg.get_sender(), msg.get_receiver()});
    if (iter != map_.end()) {
      auto election = iter->second;
      auto buffer = TH_BUFFER;
      int ret = common::OB_SUCCESS;
      if (MSG_DELAY == 0) {
        ret = THREAD_POOL.commit_task_ignore_ret([buffer, election, ret, this]() mutable {
          int64_t begin = ObClockGenerator::getRealClock();
          this->decode_and_process_buffer(buffer, election);
          int64_t end = ObClockGenerator::getRealClock();
          if (end - begin > 1_ms) {
            ELECT_LOG(WARN, "execute task cost too much time", K(end - begin));
          }
        });
      } else {
        ret = TIMER.schedule_task_ignore_handle_after(MSG_DELAY, [buffer, election, ret, this]() mutable {
          int64_t begin = ObClockGenerator::getRealClock();
          this->decode_and_process_buffer(buffer, election);
          int64_t end = ObClockGenerator::getRealClock();
          if (end - begin > 1_ms) {
            ELECT_LOG(WARN, "execute task cost too much time", K(end - begin));
          }
          return false;
        });
      }
      assert(ret == OB_SUCCESS);
      // ELECT_LOG(INFO, "send message success", K(msg));
    } else {
      ELECT_LOG_RET(WARN, OB_ERR_UNEXPECTED, "send message failed", K(msg));
    }
  }
  unordered_map<std::pair<ObAddr,ObAddr>, Election*> map_;
  mutable std::mutex mutex_;
};

}
}
