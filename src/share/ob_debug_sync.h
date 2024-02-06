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

#ifndef OCEANBASE_COMMON_OB_DEBUG_SYNC_H_
#define OCEANBASE_COMMON_OB_DEBUG_SYNC_H_

#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "share/ob_debug_sync_point.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace common
{
class ObString;
typedef common::ObFixedLengthString<32> ObSyncEventName;

struct ObDebugSyncAction
{
public:
  OB_UNIS_VERSION(1);

public:
  ObDebugSyncAction() : sync_point_(INVALID_DEBUG_SYNC_POINT),
    timeout_(0), execute_(0), no_clear_()
  {}
  TO_STRING_KV(K_(sync_point), K_(timeout), K_(execute),
      K_(signal), K_(wait), K_(no_clear), K_(broadcast));

  void reset() { *this = ObDebugSyncAction(); }
  bool is_valid() const;

  ObDebugSyncPoint sync_point_;
  int64_t timeout_;
  int64_t execute_;
  ObSyncEventName signal_;
  ObSyncEventName broadcast_;
  ObSyncEventName wait_;
  bool  no_clear_;
};

class ObDSActionArray
{
public:
  static constexpr int MAX_DEBUG_SYNC_CACHED_POINT = 32;
  OB_UNIS_VERSION(1);

public:
  // const action array will always be empty
  explicit ObDSActionArray(const bool is_const = false);

  bool is_empty() const { return 0 >= active_cnt_; }

  void clear(const ObDebugSyncPoint sync_point);
  void clear_all();

  int add_action(const ObDebugSyncAction &action);
  // fetch action to execute,
  // return OB_SUCCESS for action exist, OB_ENTRY_NOT_EXIST for not exist
  int fetch_action(const ObDebugSyncPoint sync_point, ObDebugSyncAction &action);

  bool is_active(const ObDebugSyncPoint sync_point) const;
  int copy_action(const ObDebugSyncPoint sync_point, ObDebugSyncAction &action) const;

private:
  ObDebugSyncAction *action_ptrs_[MAX_DEBUG_SYNC_POINT];
  ObDebugSyncAction actions_[MAX_DEBUG_SYNC_CACHED_POINT];
  volatile int64_t active_cnt_;
  const bool is_const_;

  DISALLOW_COPY_AND_ASSIGN(ObDSActionArray);
};

class ObDSActionNode : public ObDLinkBase<ObDSActionNode>
{
public:
  ObDebugSyncAction action_;
};

class ObDSSessionActions
{
public:
  ObDSSessionActions() : inited_(false), block_head_(NULL), page_size_(0), allocator_(NULL)
  {
  }
  virtual ~ObDSSessionActions();

  int init(const int64_t page_size, ObIAllocator &allocator);
  bool is_inited() const { return inited_; }

  int add_action(const ObDebugSyncAction &action);

  void clear(const ObDebugSyncPoint sync_point);
  void reset() { clear_all(); }
  void clear_all();

  int to_thread_local(ObDSActionArray &local) const;
  int get_thread_local_result(const ObDSActionArray &local);

private:
  ObDSActionNode *alloc_node();
  void free_node(ObDSActionNode *node);

private:
  bool inited_;
  void *block_head_;
  int64_t page_size_;
  ObIAllocator *allocator_;

  ObDList<ObDSActionNode> actions_;
  ObDList<ObDSActionNode> free_list_;

  DISALLOW_COPY_AND_ASSIGN(ObDSSessionActions);
};

class ObDSEventControl
{
public:
  const static int64_t MIN_EVENT_CNT = 512;
  const static int64_t MAX_EVENT_CNT = MAX_DEBUG_SYNC_POINT <= MIN_EVENT_CNT / 10
      ? MIN_EVENT_CNT : MAX_DEBUG_SYNC_POINT * 10;

  struct Event : ObDLinkBase<Event>
  {
    Event() : signal_cnt_(0), waiter_cnt_(0), name_()
    {}

    void reset();

    int64_t signal_cnt_;
    int64_t waiter_cnt_;
    ObSyncEventName name_;
  };

  ObDSEventControl();
  virtual ~ObDSEventControl();

  int signal(const ObSyncEventName &name);
  int broadcast(const ObSyncEventName &name);
  int wait(const ObSyncEventName &name, const int64_t timeout_us, const bool clear);

  // clear events with no waiters.
  void clear_event();

  void stop();

private:
  // return NULL for alloc failed
  Event *alloc_event();
  void free_event(Event *e);
  // find exist event, return OB_ENTRY_NOT_EXIST for not found
  int find(const ObSyncEventName &name, Event *&e);
  // locate event by name (if not exist create one)
  int locate(const ObSyncEventName &name, Event *&e);

private:
  volatile bool stop_;

  Event events_[MAX_EVENT_CNT];
  ObDList<Event> free_;
  ObDList<Event> used_;

  ObThreadCond cond_;

  DISALLOW_COPY_AND_ASSIGN(ObDSEventControl);
};

class ObDebugSync
{
public:
  static ObDebugSync &instance();

  void set_rpc_proxy(obrpc::ObCommonRpcProxy *rpc_proxy);

  int add_debug_sync(const ObString &str, const bool is_global,
      ObDSSessionActions &session_actions);

  int set_global_action(const bool reset, const bool clear, const ObDebugSyncAction &action);

  int execute(const ObDebugSyncPoint sync_point);

  int set_thread_local_actions(const ObDSSessionActions &session_actions);
  int collect_result_actions(ObDSSessionActions &session_actions);

  ObDSActionArray *thread_local_actions() const;

  ObDSActionArray &rpc_spread_actions() const;

  void stop();

private:
  ObDebugSync() : stop_(false), lock_(ObLatchIds::DEFAULT_SPIN_LOCK), rpc_proxy_(NULL)
  {}

  int parse_action(const ObString &str, ObDebugSyncAction &action, bool &clear, bool &reset);
  static ObString get_token(ObString &str);

private:
  volatile bool stop_;
  ObSpinLock lock_; // protect global action access
  ObDSActionArray global_actions_;
  ObDSEventControl event_control_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObDebugSync);
};

#define GDS oceanbase::common::ObDebugSync::instance()
// TODO baihua: empty macro for release version?
#define DEBUG_SYNC(sync_point) GDS.execute((sync_point))

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_COMMON_OB_DEBUG_SYNC_H_
