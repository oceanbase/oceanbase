/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DIAGNOSTIC_INFO_H_
#define OB_DIAGNOSTIC_INFO_H_

#include "lib/ash/ob_active_session_guard.h"
#include "lib/hash/ob_link_hashmap_deps.h"

namespace oceanbase
{

namespace observer
{
class ObSrvDeliver;
}

namespace common
{

#define DI_INFO_OBJ_PER_CORE 256
#define DI_INFO_OBJ_UPPER_LIMIT 8192
#define WAIT_EVENT_LIST_THRESHOLD 10

class ObDiagnosticInfoSwitchGuard;

template <class T>
class ObServerObjectPool;
class ObDiagnosticKey;
class ObDiagnosticInfoCollector;
class ObDiagnosticInfoSlot;

typedef common::ObServerObjectPool<ObWaitEventStatArray> ObWaitEventPool;

struct ObListWaitEventStat : public ObWaitEventStat
{
  // adding event_no for list ObWaitClassList element
  oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum event_no_;

  ObListWaitEventStat() : ObWaitEventStat(), event_no_(ObWaitEventIds::NULL_EVENT)
  {}
  ObListWaitEventStat(const ObListWaitEventStat &stat)
  {
    this->event_no_ = stat.event_no_;
    this->total_timeouts_ = stat.total_timeouts_;
    this->max_wait_ = stat.max_wait_;
    this->total_waits_ = stat.total_waits_;
    this->time_waited_ = stat.time_waited_;
  }
  void operator=(const ObListWaitEventStat &) = delete;
  int add(const ObListWaitEventStat &other)
  {
    int ret = OB_SUCCESS;
    if (this->event_no_ == other.event_no_) {
      ret = ObWaitEventStat::add(*static_cast<const ObWaitEventStat *>(&other));
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  void reset()
  {
    event_no_ = ObWaitEventIds::NULL_EVENT;
    ObWaitEventStat::reset();
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

enum class ObWaitEventRule
{
  LIST = 0,
  ARRAY,
};

class ObWaitEventContainer
{
public:
  ObWaitEventContainer();
  ~ObWaitEventContainer()
  {
    reset();
  }
  DISABLE_COPY_ASSIGN(ObWaitEventContainer);
  int get_and_set(ObWaitEventIds::ObWaitEventIdEnum event_no, ObWaitEventStat *&event);
  int get(ObWaitEventIds::ObWaitEventIdEnum event_no, ObWaitEventStat *&event);
  void for_each(
      const std::function<void(ObWaitEventIds::ObWaitEventIdEnum, const ObWaitEventStat &)> &fn);
  void accumulate_to(ObWaitEventStatArray &target);
  void reset();
  int init(ObWaitEventPool *pool);

private:
  void copy_list_stat_to_array();
  ObWaitEventRule rule_;
  ObStatArray<ObListWaitEventStat, WAIT_EVENT_LIST_THRESHOLD> list_;
  ObWaitEventStatArray *array_;
  ObWaitEventPool *pool_;
};

struct SessionID
{
  int64_t session_id_;

  SessionID() : session_id_(0)
  {}

  SessionID(const uint64_t session_id) : session_id_(session_id)
  {}

  int64_t hash() const
  {
    return static_cast<int64_t>(session_id_);
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  int compare(const SessionID &other) const
  {
    int cmp_ret = 0;

    if (session_id_ > other.session_id_) {
      cmp_ret = 1;
    } else if (session_id_ < other.session_id_) {
      cmp_ret = -1;
    } else {
      cmp_ret = 0;
    }

    return cmp_ret;
  }

  bool operator==(const SessionID &other) const
  {
    return 0 == compare(other);
  }
  bool operator!=(const SessionID &other) const
  {
    return !operator==(other);
  }
  bool operator<(const SessionID &other) const
  {
    return -1 == compare(other);
  }

  void reset()
  {
    session_id_ = 0;
  }

  TO_STRING_KV(K_(session_id));
};

typedef common::LinkHashNode<SessionID> DiagnosticInfoHashNode;
typedef common::LinkHashValue<SessionID> DiagnosticInfoHashValue;

class ObDiagnosticInfo : public DiagnosticInfoHashValue
{
public:
  friend class ObDiagnosticInfoSwitchGuard;
  friend class ObLocalDiagnosticInfo;
  ObDiagnosticInfo()
      : pool_(nullptr),
        summary_slot_(nullptr),
        events_(),
        stats_(),
        curr_wait_(),
        total_wait_(),
        tenant_id_(0),
        group_id_(0),
        session_id_(0),
        ref_cnt_(0),
#ifdef OB_UNITTEST
        need_aggregate_(false),
#else
        need_aggregate_(true),
#endif
        is_inited_(false),
        ash_stat_()
  {}
  ~ObDiagnosticInfo();
  ObDiagnosticInfo(const ObDiagnosticInfo &) = delete;
  void operator=(const ObDiagnosticInfo &) = delete;
  const ObWaitEventContainer &get_event_stats() const
  {
    return events_;
  }
  ObWaitEventContainer &get_event_stats()
  {
    return events_;
  }
  const ObStatEventAddStatArray &get_add_stat_stats() const
  {
    return stats_;
  }
  ObStatEventAddStatArray &get_add_stat_stats()
  {
    return stats_;
  }
  void add_stat(ObStatEventIds::ObStatEventIdEnum stat_no, int64_t value)
  {
    OB_ASSERT(stat_no < ObStatEventIds::STAT_EVENT_ADD_END);
    ObStatEventAddStat *stat = stats_.get(stat_no);
    if (OB_NOT_NULL(stat)) {
      stat->add(value);
    }
  }
  void update_stat(const int16_t stat_no, const int64_t delta)
  {
    add_stat(static_cast<ObStatEventIds::ObStatEventIdEnum>(stat_no), delta);
  }
  int reuse();
  int init(int64_t tenant_id, int64_t group_id, int64_t session_id, ObWaitEventPool &pool);
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_group_id() const
  {
    return group_id_;
  }
  int64_t get_session_id() const
  {
    return session_id_;
  }
  void set_summary_slot(ObDiagnosticInfoSlot *slot)
  {
    summary_slot_ = slot;
  }
  ObDiagnosticInfoSlot *get_summary_slot() const
  {
    return summary_slot_;
  }
  void begin_wait_event(const int64_t event_no, const uint64_t timeout_ms = 0,
      const uint64_t p1 = 0, const uint64_t p2 = 0, const uint64_t p3 = 0);
  void end_wait_event(const int64_t event_no, const bool is_idle = false);
  inline ObActiveSessionStat &get_ash_stat()
  {
    return ash_stat_;
  };
  void reset_total_wait()
  {
    total_wait_.reset();
  };
  const ObWaitEventStat &get_total_wait() const
  {
    return total_wait_;
  };
  const ObWaitEventDesc &get_curr_wait() const
  {
    return curr_wait_;
  };
  ObWaitEventDesc &get_curr_wait()
  {
    return curr_wait_;
  };
  bool is_active_session() const
  {
    return ash_stat_.is_active_session_;
  }
  bool need_aggregate() const
  {
    return need_aggregate_;
  };
  void set_aggregated()
  {
    need_aggregate_ = false;
  };
  bool operator==(ObDiagnosticInfo &other) const
  {
    return this->tenant_id_ == other.tenant_id_ && this->session_id_ == other.session_id_ &&
           this->group_id_ == other.group_id_;
  }
  TO_STRING_KV(K_(tenant_id), K_(group_id), K_(session_id), K_(curr_wait), K_(ref_cnt),
      K_(need_aggregate), K(get_uref()), K(get_href()), K_(ash_stat));

private:
  friend class oceanbase::observer::ObSrvDeliver;
  void inner_begin_wait_event(const int64_t event_no, const uint64_t timeout_ms = 0,
      const uint64_t p1 = 0, const uint64_t p2 = 0, const uint64_t p3 = 0);
  ObWaitEventPool *pool_;
  ObDiagnosticInfoSlot *summary_slot_;  // point to summary slot when acquire this object for
                                        // performance.
  ObWaitEventContainer events_;
  ObStatEventAddStatArray stats_;
  ObWaitEventDesc curr_wait_;
  ObWaitEventStat total_wait_;
  int64_t tenant_id_;
  int64_t group_id_;
  int64_t session_id_;
  int ref_cnt_;
  bool need_aggregate_;
  bool is_inited_;
  ObActiveSessionStat ash_stat_;
};

#define MAX_DI_PER_TENANT 8192

template <typename T, typename N>
class DiagnosticInfoValueAlloc
{
public:
  explicit DiagnosticInfoValueAlloc(ObFixedClassAllocator<T> *alloc, int64_t alloc_limit = MAX_DI_PER_TENANT)
      : alloc_count_(0), alloc_limit_(alloc_limit), allocator_(alloc)
  {}
  ~DiagnosticInfoValueAlloc()
  {}
  T *alloc_value()
  {
    int ret = OB_SUCCESS;
    T *di = NULL;
    if (ATOMIC_LOAD(&alloc_count_) > alloc_limit_) {
      COMMON_LOG(INFO, "diagnostic info exceed upper limit", K_(alloc_count), K_(alloc_limit));
    } else {
      di = op_instance_alloc_args(allocator_, T);
      if (di != nullptr) {
        ATOMIC_INC(&alloc_count_);
      }
    }
    return di;
  }

  void free_value(T *di)
  {
    op_free(di);
    ATOMIC_DEC(&alloc_count_);
  }

  void free_node(common::LinkHashNode<N> *node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }

  common::LinkHashNode<N> *alloc_node(T *value)
  {
    UNUSED(value);
    return op_alloc(common::LinkHashNode<N>);
  }
  TO_STRING_KV(K_(alloc_count));
  int64_t get_alloc_count() const { return alloc_count_; }

private:
  volatile int64_t alloc_count_;
  int64_t alloc_limit_;
  ObFixedClassAllocator<T> *allocator_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_DIAGNOSTIC_INFO_H_ */