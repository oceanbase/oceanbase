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

#define USING_LOG_PREFIX COMMON
#include "share/ob_debug_sync.h"

#include "share/ob_define.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "share/config/ob_server_config.h"
#include "share/ob_common_rpc_proxy.h"

namespace oceanbase
{
namespace common
{

bool ObDebugSyncAction::is_valid() const
{
  return (sync_point_ > INVALID_DEBUG_SYNC_POINT && sync_point_ < MAX_DEBUG_SYNC_POINT
      && (!signal_.is_empty() || !wait_.is_empty() || !broadcast_.is_empty()) && execute_ > 0);
}

OB_SERIALIZE_MEMBER(ObDebugSyncAction, sync_point_, timeout_,
    execute_, signal_, wait_, no_clear_, broadcast_);

ObDSActionArray::ObDSActionArray(const bool is_const /* = false */)
   : active_cnt_(0), is_const_(is_const)
{
  STATIC_ASSERT(0 == INVALID_DEBUG_SYNC_POINT, "INVALID_DEBUG_SYNC_POINT shouble be zero");
  memset(action_ptrs_, 0, sizeof(action_ptrs_));
}

void ObDSActionArray::clear(const ObDebugSyncPoint sync_point)
{
  if (sync_point > INVALID_DEBUG_SYNC_POINT && sync_point < MAX_DEBUG_SYNC_POINT) {
    if (NULL != action_ptrs_[sync_point]) {
      action_ptrs_[sync_point]->sync_point_ = INVALID_DEBUG_SYNC_POINT;
      action_ptrs_[sync_point] = NULL;
      active_cnt_--;
      LOG_INFO("clear sync point", K(sync_point));
    }
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid sync point", K(sync_point));
  }
}

void ObDSActionArray::clear_all()
{
  if (!is_empty()) {
    memset(action_ptrs_, 0, sizeof(action_ptrs_));
    memset(actions_, 0, sizeof(actions_));
    active_cnt_ = 0;
    LOG_INFO("clear all sync point");
  }
}

int ObDSActionArray::add_action(const ObDebugSyncAction &action)
{
  int ret = OB_SUCCESS;
  if (!action.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid action", K(ret), K(action));
  } else {
    if (!is_const_) {
      if (OB_ISNULL(action_ptrs_[action.sync_point_])) {
        if (active_cnt_ < MAX_DEBUG_SYNC_CACHED_POINT) {
          ObDebugSyncAction *dst = NULL;
          for (int i = 0; i < ARRAYSIZEOF(actions_); i++) {
            if (INVALID_DEBUG_SYNC_POINT == actions_[i].sync_point_) {
              dst = &actions_[i];
              break;
            }
          }
          if (dst != NULL) {
            active_cnt_++;
            action_ptrs_[action.sync_point_] = dst;
          } else {
            ret = OB_ERR_UNEXPECTED;
          }
        } else {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("sync action size overflow", K(active_cnt_));
        }
      }
      if (OB_SUCC(ret)) {
        *action_ptrs_[action.sync_point_] = action;
      }
    }
  }
  return ret;
}

int ObDSActionArray::fetch_action(const ObDebugSyncPoint sync_point,
    ObDebugSyncAction &action)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (sync_point <= INVALID_DEBUG_SYNC_POINT || sync_point >= MAX_DEBUG_SYNC_POINT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sync_point));
  } else {
    if (NULL != action_ptrs_[sync_point]) {
      action = *action_ptrs_[sync_point];
      action_ptrs_[sync_point]->execute_--;
      if (action_ptrs_[sync_point]->execute_ <= 0) {
        action_ptrs_[sync_point]->sync_point_ = INVALID_DEBUG_SYNC_POINT;
        action_ptrs_[sync_point] = NULL;
        active_cnt_--;
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool ObDSActionArray::is_active(const ObDebugSyncPoint sync_point) const
{
  return (sync_point > INVALID_DEBUG_SYNC_POINT && sync_point < MAX_DEBUG_SYNC_POINT)
      && NULL != action_ptrs_[sync_point];
}

int ObDSActionArray::copy_action(const ObDebugSyncPoint sync_point,
    ObDebugSyncAction &action) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_active(sync_point))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sync_point));
  } else {
    action = *action_ptrs_[sync_point];
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDSActionArray)
{
  int ret = OB_SUCCESS;
  if (active_cnt_ > MAX_DEBUG_SYNC_POINT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected action count", K(ret),
        K_(active_cnt), LITERAL_K(MAX_DEBUG_SYNC_POINT));
  } else {
    OB_UNIS_ENCODE(active_cnt_);
    if (active_cnt_ > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(action_ptrs_); ++i) {
        if (NULL != action_ptrs_[i]) {
          OB_UNIS_ENCODE(*action_ptrs_[i]);
        }
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDSActionArray)
{
  int ret = OB_SUCCESS;
  if (active_cnt_ > 0) {
    clear_all();
  }
  int64_t cnt = 0;
  OB_UNIS_DECODE(cnt);
  if (cnt > MAX_DEBUG_SYNC_POINT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected action count", K(ret), K(cnt), LITERAL_K(MAX_DEBUG_SYNC_POINT));
  }
  ObDebugSyncAction action;
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    action.reset();
    OB_UNIS_DECODE(action);
    if (OB_SUCC(ret) && !is_const_) {
      if (OB_FAIL(add_action(action))) {
        LOG_WARN("add action failed", K(ret), K(action));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDSActionArray)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (active_cnt_ > MAX_DEBUG_SYNC_POINT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected action count", K(ret),
        K_(active_cnt), LITERAL_K(MAX_DEBUG_SYNC_POINT));
  } else {
    OB_UNIS_ADD_LEN(active_cnt_);
    if (active_cnt_ > 0) {
      for (int64_t i = 0; i < ARRAYSIZEOF(action_ptrs_); ++i) {
        if (NULL != action_ptrs_[i]) {
          OB_UNIS_ADD_LEN(*action_ptrs_[i]);
        }
      }
    }
  }
  return len;
}

ObDSSessionActions::~ObDSSessionActions()
{
  if (inited_) {
    clear_all();
  }
}

int ObDSSessionActions::init(const int64_t page_size, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (page_size < static_cast<int64_t>(sizeof(void *) + sizeof(ObDSActionNode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: too small page size", K(ret), K(page_size));
  } else {
    allocator_ = &allocator;
    page_size_ = page_size;
    inited_ = true;
  }
  return ret;
}

ObDSActionNode *ObDSSessionActions::alloc_node()
{
  ObDSActionNode *node = NULL;
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (free_list_.is_empty()) {
      void **block = static_cast<void **>(allocator_->alloc(page_size_));
      if (OB_ISNULL(block)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K_(page_size));
      } else {
        // link to block head
        *block = block_head_;
        block_head_ = block;

        // add all action to free list
        ObDSActionNode *begin = reinterpret_cast<ObDSActionNode *>(
            reinterpret_cast<char *>(block)+ sizeof(void*));
        const int64_t cnt = (page_size_ - sizeof(void *)) / sizeof(ObDSActionNode);
        for (int64_t i = 0; i < cnt; ++i) {
          ObDSActionNode *n = new(&begin[i])ObDSActionNode();
          if (!free_list_.add_first(n)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("add empty node to list should always success", K(ret));

            // delete nodes added to %free_list_ and destruct it.
            n->~ObDSActionNode();
            for (int64_t j = 0; j < i; ++j) {
              ObDSActionNode *r = free_list_.remove_first();
              if (NULL != r) {
                r->~ObDSActionNode();
              }
            }
            break;
          }
        }
        if (OB_FAIL(ret)) {
          allocator_->free(block);
          block = NULL;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!free_list_.is_empty()) {
      node = free_list_.remove_first();
    }
  }
  return node;
}

void ObDSSessionActions::free_node(ObDSActionNode *node)
{
  if (NULL != node) {
    if (NULL != node->get_next()) { // in actions_
      actions_.remove(node);
    }
    node->action_.reset();
    if (!free_list_.add_first(node)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "add empty node to list should always success");
    }
  }
}

int ObDSSessionActions::add_action(const ObDebugSyncAction &action)
{
  int ret = OB_SUCCESS;
  ObDSActionNode *n = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!action.is_valid()) {
    LOG_WARN("invalid action", K(ret), K(action));
  } else {
    DLIST_FOREACH_NORET(it, actions_) {
      if (action.sync_point_ == it->action_.sync_point_) {
        n = &(*it);
        break;
      }
    }
    if (OB_LIKELY(NULL == n)) {
      if (NULL == (n = alloc_node())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("add action node failed", K(ret));
      } else {
        if (!actions_.add_first(n)) {
          ret = OB_ERR_UNEXPECTED;
          free_node(n);
          n = NULL;
          LOG_ERROR("add empty node to list should always success", K(ret));
        }
      }
    }
    if (NULL != n && OB_SUCC(ret)) {
      n->action_ = action;
    }
  }
  return ret;
}

void ObDSSessionActions::clear_all()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObDSActionNode *n = NULL;
    while (OB_SUCC(ret) && !actions_.is_empty()) {
      n = actions_.remove_first();
      if (OB_ISNULL(n)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL item", K(ret), KP(n));
      } else {
        n->~ObDSActionNode();
      }
    }
    while (OB_SUCC(ret) && !free_list_.is_empty()) {
      n = free_list_.remove_first();
      if (OB_ISNULL(n)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL item", K(ret), KP(n));
      } else {
        n->~ObDSActionNode();
      }
    }
    while (OB_SUCC(ret) && NULL != block_head_) {
      void **block = reinterpret_cast<void **>(block_head_);
      block_head_ = *block;
      allocator_->free(block);
      block = NULL;
    }
  }
}

void ObDSSessionActions::clear(const ObDebugSyncPoint sync_point)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(sync_point <= INVALID_DEBUG_SYNC_POINT)
      || OB_UNLIKELY(sync_point >= MAX_DEBUG_SYNC_POINT)) {
    ret = OB_INVALID_ARGUMENT;;
    LOG_WARN("invalid argument", K(ret), K(sync_point));
  } else {
    ObDSActionNode *n = NULL;
    DLIST_FOREACH_NORET(it, actions_) {
      if (it->action_.sync_point_ == sync_point) {
        n = &(*it);
        break;
      }
    }
    if (NULL != n) {
      LOG_INFO("clear action", K(sync_point), "action", n->action_);
      actions_.remove(n);
      free_node(n);
      n = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    if (actions_.is_empty()) {
      clear_all();
    }
  }
}

int ObDSSessionActions::to_thread_local(ObDSActionArray &local) const
{
  int ret = OB_SUCCESS;
  if (!local.is_empty()) {
    local.clear_all();
  }
  DLIST_FOREACH(it, actions_) {
    if (OB_FAIL(local.add_action(it->action_))) {
      LOG_WARN("add action failed", K(ret), "action", *it);
    }
  }
  return ret;
}

int ObDSSessionActions::get_thread_local_result(const ObDSActionArray &local)
{
  int ret = OB_SUCCESS;
  if (actions_.is_empty()) {
    // do nothing
  } else if (local.is_empty()) {
    clear_all();
  } else {
    DLIST_FOREACH_REMOVESAFE(it, actions_) {
      if (local.is_active(it->action_.sync_point_)) {
        if (OB_FAIL(local.copy_action(it->action_.sync_point_, it->action_))) {
          LOG_WARN("copy action failed", K(ret));
        }
      } else {
        actions_.remove(it);
        free_node(it);
        it = NULL;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (actions_.is_empty()) {
      clear_all();
    }
  }
  return ret;
}

void ObDSEventControl::Event::reset()
{
  ObDLinkBase<Event>::reset();
  signal_cnt_ = 0;
  waiter_cnt_ = 0;
  name_.reset();
}

ObDSEventControl::ObDSEventControl() : stop_(false)
{
  for (int64_t i = 0; i < ARRAYSIZEOF(events_); ++i) {
    if (!free_.add_first(&events_[i])) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "add empty node to list should always success");
    }
  }
  cond_.init(ObWaitEventIds::DEBUG_SYNC_COND_WAIT);
}

ObDSEventControl::~ObDSEventControl()
{
  stop();
}


ObDSEventControl::Event *ObDSEventControl::alloc_event()
{
  Event *e = NULL;
  if (free_.is_empty()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "exceed max event count", LITERAL_K(MAX_EVENT_CNT));
  } else {
    e = free_.remove_first();
  }
  return e;
}

void ObDSEventControl::free_event(Event *e)
{
  if (NULL != e) {
    e->reset();
    if (!free_.add_first(e)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "add empty node to list should always success");
    }
  }
}

int ObDSEventControl::find(const ObSyncEventName &name, Event *&e)
{
  int ret = OB_SUCCESS;
  if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(name));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    e = NULL;
    DLIST_FOREACH_NORET(it, used_) {
      if (it->name_ == name) {
        e = &(*it);
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObDSEventControl::locate(const ObSyncEventName &name, Event *&e)
{
  int ret = OB_SUCCESS;
  e = NULL;
  if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(name));
  } else if (OB_FAIL(find(name, e))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find event failed", K(ret), K(name));
    } else {
      ret = OB_SUCCESS;
      e = alloc_event();
      if (OB_ISNULL(e)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("exceed max event cnt, alloc failed", K(ret));
      } else {
        e->name_ = name;
        if (!used_.add_first(e)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("add empty node to list should always success", K(ret));
        }
      }
    }
  } else if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL event", K(ret));
  }
  return ret;
}

int ObDSEventControl::signal(const ObSyncEventName &name)
{
  int ret = OB_SUCCESS;
  Event *e = NULL;
  ObThreadCondGuard guard(cond_);
  if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid event name", K(ret), K(name));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("is stopping", K(ret), K(name));
  } else if (OB_FAIL(locate(name, e))) {
    LOG_WARN("locate event failed", K(ret), K(name));
  } else if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL event", K(ret));
  } else {
    e->signal_cnt_++;
    if (e->waiter_cnt_ > 0) {
      int tmp_ret = cond_.broadcast();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("condition broadcast failed", K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObDSEventControl::broadcast(const ObSyncEventName &name)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid event name", K(ret), K(name));
  } else {
    ObThreadCondGuard guard(cond_);

    DLIST_FOREACH(it, used_) {
      if (it->name_ == name) {
        if (it->waiter_cnt_ > 0 && it->signal_cnt_ < it->waiter_cnt_) {
          it->signal_cnt_ = it->waiter_cnt_;
        }
        LOG_INFO("broadcast", KP(it), K(name), "signal_cnt", it->signal_cnt_, "waiter_cnt", it->waiter_cnt_);
      }
    }

    if (OB_SUCCESS != (tmp_ret = cond_.broadcast())) {
      LOG_WARN("condition broadcast failed", K(tmp_ret), K(name));
    }
  }

  return ret;
}

int ObDSEventControl::wait(const ObSyncEventName &name,
    const int64_t timeout_us, const bool clear)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_ms = ObTimeUtility::current_time() / 1000;
  const int64_t abs_timeout_ms = start_time_ms + timeout_us / 1000;
  Event *e = NULL;
  ObThreadCondGuard guard(cond_);
  if (name.is_empty() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid event name", K(ret), K(name), K(timeout_us));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_INFO("is stopping", K(ret), K(name));
  } else if (OB_FAIL(locate(name, e))) {
    LOG_WARN("locate event failed", K(ret), K(name));
  } else if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL event", K(ret));
  } else {
    e->waiter_cnt_++;
    LOG_INFO("start wait", KP(e), K(name), "signal_cnt", e->signal_cnt_);
    while (!stop_ && e->signal_cnt_ <= 0) {
      const int64_t now_ms = ObTimeUtility::current_time() / 1000;
      if (now_ms < abs_timeout_ms) {
        cond_.wait(static_cast<int>(abs_timeout_ms - now_ms));
      } else {
        break;
      }
    }
    if (stop_) {
      ret = OB_CANCELED;
      LOG_INFO("is stopping", K(ret), K(name));
    } else {
      LOG_INFO("finish wait", KP(e), K(name));
      e->waiter_cnt_--;
      if (e->signal_cnt_ <= 0) {
        LOG_INFO("wait for event timeout", K(name), K(timeout_us));
      }
      if (e->signal_cnt_ > 0 && clear) {
        e->signal_cnt_--;
      }
      if (0 == e->waiter_cnt_ && 0 == e->signal_cnt_) {
        used_.remove(e);
        free_event(e);
        e = NULL;
      }
    }

  }
  return ret;
}

void ObDSEventControl::clear_event()
{
  ObThreadCondGuard guard(cond_);
  if (!stop_) {
    DLIST_FOREACH_REMOVESAFE_NORET(e, used_) {
      if (0 == e->waiter_cnt_) {
        used_.remove(e);
        free_event(e);
        e = NULL;
      }
    }
  }
}

void ObDSEventControl::stop()
{
  ObThreadCondGuard guard(cond_);
  stop_ = true;
  int tmp_ret = cond_.broadcast();
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "condition broadcast failed", K(tmp_ret));
  }
}

ObString ObDebugSync::get_token(ObString &str)
{
  ObString token;
  if (str.empty()) {
    token = str;
  } else {
    int begin = 0;
    while (begin < str.length() && isspace(str.ptr()[begin])) {
      begin++;
    }
    int end = begin;
    while (end < str.length() && !isspace(str.ptr()[end])) {
      end++;
    }
    if (end > begin) {
      token.assign_ptr(str.ptr() + begin, end - begin);
    }
    while (end < str.length() && isspace(str.ptr()[end])) {
      end++;
    }
    if (end < str.length()) {
      str.assign_ptr(str.ptr() + end, str.length() - end);
    } else {
      str.reset();
    }
  }

  return token;
}

int ObDebugSync::parse_action(const ObString &str_origin,
    ObDebugSyncAction &action, bool &clear, bool &reset)
{
  ObString str = str_origin;
  ObString token;
  int ret = OB_SUCCESS;
  bool finish = false; // finish parsing

  action.reset();
  action.timeout_ = GCONF.debug_sync_timeout;
  action.execute_ = 1;
  clear = false;
  reset = false;

  if (str_origin.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str_origin));
  } else {
    token = get_token(str);
    if (token.empty()) {
      ret = OB_PARSE_DEBUG_SYNC_ERROR;
      LOG_WARN("empty debug sync string", K(ret), K(str_origin));
    }
  }
  // parse RESET
  if (OB_SUCC(ret) && !finish) {
    if (token.case_compare("RESET") == 0) {
      reset = true;
      finish = true;
      token = get_token(str);
    }
  }

  // parse sync point
  if (OB_SUCC(ret) && !finish) {
    action.sync_point_ = get_debug_sync_point_value(token);
    if (action.sync_point_ <= INVALID_DEBUG_SYNC_POINT
        || action.sync_point_ >= MAX_DEBUG_SYNC_POINT) {
      ret = OB_UNKNOWN_DEBUG_SYNC_POINT;
      LOG_WARN("invalid sync point", K(ret), K(token), K(str_origin));
    } else {
      token = get_token(str);
      if (token.empty()) {
        ret = OB_PARSE_DEBUG_SYNC_ERROR;
        LOG_WARN("no action after sync point", K(ret), K(str_origin));
      }
    }
  }

  if (OB_SUCC(ret) && !finish) {
    // parse CLEAR action
    if (token.case_compare("CLEAR") == 0) {
      clear = true;
      finish = true;
      token = get_token(str);
    } else if (token.case_compare("SIGNAL") == 0
        || token.case_compare("BROADCAST") == 0
        || token.case_compare("WAIT_FOR") == 0) {
      // parse signal or wait for
      ObSyncEventName *name = &action.wait_;

      if (0 == token.case_compare("SIGNAL")) {
        name = &action.signal_;
      } else if (0 == token.case_compare("BROADCAST")) {
        name = &action.broadcast_;
        finish = true;
      }
      token = get_token(str);
      if (token.empty()) {
        ret = OB_PARSE_DEBUG_SYNC_ERROR;
        LOG_WARN("no event name", K(ret), K(str_origin));
      } else {
        // TODO baihua: to lower case?
        if (OB_FAIL(name->assign(token))) {
          LOG_WARN("assign event name failed", K(ret), K(token), K(str_origin));
        } else {
          token = get_token(str);
        }
      }
    } else {
      ret = OB_PARSE_DEBUG_SYNC_ERROR;
      LOG_WARN("not supported action", K(ret), K(token), K(str_origin));
    }
  }

  if (OB_SUCC(ret) && !finish && !action.signal_.is_empty()) {
    if (token.case_compare("WAIT_FOR") == 0) {
      token = get_token(str);
      if (token.empty()) {
        ret = OB_PARSE_DEBUG_SYNC_ERROR;
        LOG_WARN("no event name", K(ret), K(str_origin));
      } else if (OB_FAIL(action.wait_.assign(token))) {
        LOG_WARN("assign event name failed", K(ret), K(token), K(str_origin));
      } else {
        token = get_token(str);
      }
    }
  }

  while (OB_SUCC(ret) && !finish && !token.empty() && !action.wait_.is_empty()) {
    // parse TIMEOUT
    if (token.case_compare("TIMEOUT") == 0) {
      token = get_token(str);
      if (token.empty()) {
        ret = OB_PARSE_DEBUG_SYNC_ERROR;
        LOG_WARN("integer expected after TIMEOUT", K(ret), K(str_origin));
      } else {
        action.timeout_ = atoll(to_cstring(token));
        token = get_token(str);
      }
    } else if (token.case_compare("NO_CLEAR_EVENT") == 0) {
      // parse NO_CLEAR_EVENT
      action.no_clear_ = true;
      token = get_token(str);
    } else {
      break;
    }
  }

  // parse EXECUTE
  if (OB_SUCC(ret) && !finish && !token.empty()) {
    if (token.case_compare("EXECUTE") == 0) {
      token = get_token(str);
      if (token.empty()) {
        ret = OB_PARSE_DEBUG_SYNC_ERROR;
        LOG_WARN("integer expected after EXECUTE", K(ret), K(str_origin));
      } else {
        action.execute_ = atoll(to_cstring(token));
        if (action.execute_ <= 0) {
          ret = OB_PARSE_DEBUG_SYNC_ERROR;
          LOG_WARN("invalid execute count", K(ret), K(token), K(str_origin));
        } else {
          token = get_token(str);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!token.empty() || !str.empty()) {
      ret = OB_PARSE_DEBUG_SYNC_ERROR;
      LOG_WARN("unexpected parameters found", K(ret), K(token), K(str), K(str_origin));
    }
  }
  LOG_INFO("finish get debug sync action", K(ret), K(str_origin), K(action), K(clear), K(reset));
  return ret;
}

int ObDebugSync::add_debug_sync(const ObString &str, const bool is_global,
    ObDSSessionActions &session_actions)
{
  int ret = OB_SUCCESS;
  ObDebugSyncAction action;
  ObDSActionArray *local_actions = thread_local_actions();
  bool clear = false;
  bool reset = false;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("is stopping", K(ret));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc proxy not set", K(ret));
  } else if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str));
  } else if (OB_ISNULL(local_actions)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("get thread local actions failed", K(ret), K(str));
  } else if (OB_FAIL(parse_action(str, action, clear, reset))) {
    LOG_WARN("parse debug sync action failed", K(ret), K(str));
  } else {
    if (!is_global) {
      if (clear) {
        local_actions->clear(action.sync_point_);
        session_actions.clear(action.sync_point_);
      } else if (reset) {
        local_actions->clear_all();
        session_actions.clear_all();
        event_control_.clear_event();
      } else {
        if (!action.is_valid()) {
          ret = OB_PARSE_DEBUG_SYNC_ERROR;
          LOG_WARN("invalid action", K(ret), K(str), K(action));
        } else if (OB_FAIL(local_actions->add_action(action))) {
          LOG_WARN("add action failed", K(ret), K(action));
        } else if (OB_FAIL(session_actions.add_action(action))) {
          LOG_WARN("add action failed", K(ret), K(action));
        }
      }
      if (OB_SUCC(ret)) {
        DEBUG_SYNC(NOW);
      }
    } else {
      obrpc::ObDebugSyncActionArg arg;
      arg.reset_ = reset;
      arg.clear_ = clear;
      arg.action_ = action;
      if (OB_FAIL(rpc_proxy_->broadcast_ds_action(arg))) {
        LOG_WARN("broadcast debug sync action failed", K(ret), K(arg));
      }
    }
  }

  return ret;
}

int ObDebugSync::execute(const ObDebugSyncPoint sync_point)
{
  int ret = OB_SUCCESS;
  ObDSActionArray *local_actions = thread_local_actions();
  bool is_local_action = false;

  if (OB_UNLIKELY(sync_point <= INVALID_DEBUG_SYNC_POINT)
      || OB_UNLIKELY(sync_point >= MAX_DEBUG_SYNC_POINT)) {
    ret = OB_INVALID_ARGUMENT;;
    LOG_WARN("invalid argument", K(ret), K(sync_point));
  } else if (!GCONF.is_debug_sync_enabled()
      || ((OB_ISNULL(local_actions) || local_actions->is_empty())
          && global_actions_.is_empty())) {
    // quick path
  } else {
    ObDebugSyncAction action;
    bool got = false;
    if (NULL != local_actions) {
      if (OB_UNLIKELY(OB_SUCCESS == (ret = local_actions->fetch_action(sync_point, action)))) {
        got = true;
        is_local_action = true;
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fetch action failed", K(ret), K(sync_point));
      }
    }
    if (OB_SUCC(ret)) {
      if (!got && !global_actions_.is_empty()) {
        ObSpinLockGuard guard(lock_);
        if (OB_UNLIKELY(OB_SUCCESS == (ret = global_actions_.fetch_action(
            sync_point, action)))) {
          got = true;
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fetch action failed", K(ret), K(sync_point));
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(got)) {
      LOG_INFO("execute action",K(is_local_action), K(action));
      if (!action.signal_.is_empty()) {
        if (OB_FAIL(event_control_.signal(action.signal_))) {
          LOG_WARN("signal failed", K(ret), K(action));
        }
      } else if (!action.broadcast_.is_empty()) {
        if (OB_FAIL(event_control_.broadcast(action.broadcast_))) {
          LOG_WARN("failed to broadcast", K(ret), K(action));
        }
      }

      if (OB_SUCC(ret) && !action.wait_.is_empty()) {
        if (OB_FAIL(event_control_.wait(action.wait_, action.timeout_, !action.no_clear_))) {
          LOG_WARN("wait failed", K(ret), K(action));
        }
      }
    }
  }

  return ret;
}

int ObDebugSync::set_thread_local_actions(const ObDSSessionActions &session_actions)
{
  int ret = OB_SUCCESS;
  ObDSActionArray *local = thread_local_actions();
  if (NULL != local) {
    if (OB_FAIL(session_actions.to_thread_local(*local))) {
      LOG_WARN("to thread local actions failed", K(ret));
    }
  }
  return ret;
}

int ObDebugSync::collect_result_actions(ObDSSessionActions &session_actions)
{
  int ret = OB_SUCCESS;
  ObDSActionArray *local = thread_local_actions();
  if (NULL != local) {
    if (OB_FAIL(session_actions.get_thread_local_result(*local))) {
      LOG_WARN("get thread local actions result failed", K(ret));
    }
  }
  return ret;
}

ObDebugSync &ObDebugSync::instance()
{
  static ObDebugSync global_instance_;
  return global_instance_;
}

ObDSActionArray *ObDebugSync::thread_local_actions() const
{
  return GET_TSI(ObDSActionArray);
}

ObDSActionArray &ObDebugSync::rpc_spread_actions() const
{
  static const bool const_array = true;
  static ObDSActionArray empty_actions(const_array);
  ObDSActionArray *actions = thread_local_actions();
  if (OB_ISNULL(actions) || !GCONF.is_debug_sync_enabled()) {
    actions = &empty_actions;
  }
  return *actions;
}

void ObDebugSync::stop()
{
  stop_ = true;
  event_control_.stop();
}

int ObDebugSync::set_global_action(const bool reset, const bool clear,
    const ObDebugSyncAction &action)
{
  int ret = OB_SUCCESS;
  const bool is_debug_sync_enabled = GCONF.is_debug_sync_enabled();
  if (!reset && !clear && !action.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(reset), K(clear), K(action));
  } else {
    if (is_debug_sync_enabled) {
      ObSpinLockGuard guard(lock_);
      if (clear) {
        global_actions_.clear(action.sync_point_);
      } else if (reset) {
        global_actions_.clear_all();
        event_control_.clear_event();
      } else {
        if (OB_FAIL(global_actions_.add_action(action))) {
          LOG_WARN("add action failed", K(ret), K(action));
        }
      }
    } else {
      LOG_ERROR("cannot set global action when debug ysnc is disabled", K(action));
    }
  }
  if (OB_SUCC(ret)) {
    DEBUG_SYNC(NOW);
  }
  LOG_INFO("set debug sync global action", K(reset), K(clear), K(action), K(is_debug_sync_enabled));
  return ret;
}

void ObDebugSync::set_rpc_proxy(obrpc::ObCommonRpcProxy *rpc_proxy)
{
  rpc_proxy_ = rpc_proxy;
}

} // end namespace common
} // end namespace oceanbase
