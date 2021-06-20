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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_VIRTUAL_EVENT_HISTORY_
#define OCEANBASE_ELECTION_OB_ELECTION_VIRTUAL_EVENT_HISTORY_

#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace election {
class ObElectionEventHistory {
public:
  ObElectionEventHistory()
  {
    reset();
  }
  ~ObElectionEventHistory()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }

public:
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  uint64_t get_table_id() const
  {
    return partition_.get_table_id();
  }
  int64_t get_partition_id() const
  {
    return partition_.get_partition_id();
  }
  const common::ObAddr& get_current_leader() const
  {
    return current_leader_;
  }
  const char* get_event_name_cstr() const;
  const char* get_event_info_cstr() const;
  TO_STRING_KV(K_(partition), K_(addr), K_(current_leader), K_(event_type));

public:
  enum EventType {
    EVENT_MIN = 0,
    L_TAKEOVER_ASSIGN,
    L_TAKEOVER_DEVOTE,
    L_TAKEOVER_REAPPOINT,
    L_TAKEOVER_CHANGE,
    L_TAKEOVER_QUERY,
    L_REVOKE_LEASE_EXPIRE,
    L_REVOKE_NOT_CANDIDATE,
    L_REVOKE_CHANGE,
    EVENT_MAX
  };
  static const char* const EVENT_NAME_STR[EventType::EVENT_MAX];  // event name, limited within 32 bytes
  static const char* const EVENT_INFO_STR[EventType::EVENT_MAX];  // event info, limited within 32 bytes
  static bool is_valid_event_type(const int32_t etype)
  {
    return (etype >= EVENT_MIN && etype <= EVENT_MAX);
  }

public:
  int64_t timestamp_;
  common::ObPartitionKey partition_;
  common::ObAddr addr_;
  common::ObAddr current_leader_;
  int32_t event_type_;
};

class ObElectionEventHistoryArray {
  typedef ObElectionEventHistory::EventType EventType;

public:
  ObElectionEventHistoryArray() : array_(NULL), self_(), is_inited_(false), idx_(0)
  {}
  ~ObElectionEventHistoryArray()
  {
    destroy();
  }
  void destroy();
  int init(const common::ObAddr& self);

public:
  int32_t get_event_cursor() const
  {
    return ATOMIC_LOAD(&idx_);
  }
  int add_event_history_item(
      const common::ObPartitionKey& pkey, const common::ObAddr& current_leader, const EventType etype);
  int get_prev_event_history(const int64_t now, int32_t& idx, ObElectionEventHistory& event_history) const;

public:
  static const int64_t OB_ELECT_EVENT_ITEM_NUM = 50000 * 5;  // 5w partitions, 5 records each
private:
  ObElectionEventHistory* array_;
  common::ObAddr self_;
  bool is_inited_;
  int32_t idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObElectionEventHistoryArray);
};

}  // namespace election
}  // namespace oceanbase
#endif
