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

#define USING_LOG_PREFIX SERVER

#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_running_mode.h"
#include "lib/ob_running_mode.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "share/interrupt/ob_interrupt_rpc_proxy.h"

namespace oceanbase {
namespace common {

OB_SERIALIZE_MEMBER(ObInterruptibleTaskID, first_, last_);

// Thread local provides a thread interrupter

int ObInterruptChecker::register_checker(const ObInterruptibleTaskID &tid)
{
  int ret = OB_SUCCESS;
  ret = ObGlobalInterruptManager::getInstance()->register_checker(this, tid);
  return ret;
}

void ObInterruptChecker::unregister_checker(const ObInterruptibleTaskID &tid)
{
    // If there is an exception, the relevant log has been printed in the Manager, and it is already in the destructor here, and no more solutions can be provided
  (void) ObGlobalInterruptManager::getInstance()->unregister_checker(this, tid);
}

bool ObInterruptChecker::is_interrupted()
{
  return interrupted_;
}

ObInterruptCode &ObInterruptChecker::get_interrupt_code()
{
  //Returns the first interrupt received
  return interrupt_code_array_[0];
}

void ObInterruptChecker::interrupt(ObInterruptCode &interrupt_code)
{
  //Each checker can only receive up to 16 interrupts
  // By adding pos + 1 each time, you can count how many interrupts there are in total.
  int64_t pos = ATOMIC_FAA(&array_pos_, 1);
  if (pos < T_ARRAY_SIZE) {
    interrupt_code_array_[pos] = interrupt_code;
  }
  interrupted_ = true;
}

void ObInterruptChecker::clear_status()
{
  interrupted_ = false;
  array_pos_ = 0;
  ref_count_ = 0;
  for (int idx = 0; idx < T_ARRAY_SIZE; ++idx)
  {
    interrupt_code_array_[idx].reset();
  }
}

void ObInterruptChecker::clear_interrupt_status()
{
  interrupted_ = false;
}

ObGlobalInterruptManager *ObGlobalInterruptManager::getInstance()
{
  return instance_;
}

int ObGlobalInterruptManager::init(const common::ObAddr &local, ObInterruptRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "rpc_proxy must not null");
  } else if (OB_FAIL(map_.create(!lib::is_mini_mode() ? DEFAULT_HASH_MAP_BUCKETS_COUNT :
                                 MINI_MODE_HASH_MAP_BUCKETS_COUNT,
                                 ObModIds::OB_HASH_BUCKET_INTERRUPT_CHECKER,
                                 ObModIds::OB_HASH_NODE_INTERRUPT_CHECKER))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "create hash table failed", K(ret));
  } else {
    local_ = local;
    rpc_proxy_ = rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObGlobalInterruptManager::register_checker(ObInterruptChecker *checker,
                                               const ObInterruptibleTaskID &tid)
{
  int ret = OB_SUCCESS;
  ObInterruptCheckerNode *checker_node = NULL;
  ObInterruptGetCheckerNodeCall get_node_call(checker);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "interrupt manager not inited", K(ret));
  } else if (OB_ISNULL(checker)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invaild checker pointer");
  } else if (FALSE_IT(map_.read_atomic(tid, get_node_call))) {
  } else if (get_node_call.is_checker_exist()) {
    ret = OB_HASH_EXIST;
    LIB_LOG(ERROR, "the check has already registered", K(ret));
  } else if (OB_FAIL(create_checker_node(checker, checker_node))) {
    LIB_LOG(ERROR, "fail to create checker node", K(ret));
  } else {
    // A slightly more complicated but safe inspection operation
    // Since map does not provide the operation of "create or modify", nor does it provide the ability to hold bucket locks
    // Therefore, add cyclic verification to prevent the occurrence of
    // thread a failed to create --> thread b erased --> thread a failed to modify
    // Such a situation
    do {
      if (OB_HASH_EXIST == (ret = map_.set_refactored(tid, checker_node))) {
        ObInterruptCheckerAddCall call(checker_node);
        ret = map_.atomic_refactored(tid, call);
        // If it is an empty queue, it means that another thread wants to delete the node but unexpectedly got the lock by this thread
        // So do not delete, try to put again according to HASH_NOT_EXIST
        if (call.is_empty()) {
          ret = OB_HASH_NOT_EXIST;
        }
      }
    } while (ret == OB_HASH_NOT_EXIST);
    if (OB_SUCC(ret)) {
      ATOMIC_INC(&(checker->ref_count_));
    }
  }
  return ret;
}

int ObGlobalInterruptManager::unregister_checker(ObInterruptChecker *checker,
                                                 const ObInterruptibleTaskID &tid)
{
  int ret = OB_SUCCESS;
  int ignore = OB_SUCCESS;
  UNUSED(ignore);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "interrupt manager not inited");
  } else if (OB_ISNULL(checker)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invaild checker pointer");
  } else {
    ObInterruptGetCheckerNodeCall get_node_call(checker);
    if (OB_HASH_NOT_EXIST == map_.read_atomic(tid, get_node_call)) {
      LIB_LOG(ERROR, "unregister checker failed", K(ret));
    } else if (!get_node_call.is_checker_exist()) {
      ret = OB_HASH_NOT_EXIST;
      LIB_LOG(ERROR, "the checker is not exist", K(ret));
    } else {
      ObInterruptCheckerNode *checker_node = get_node_call.get_checker_node();
      ObInterruptCheckerRemoveCall call(checker_node);
      ret = map_.atomic_refactored(tid, call);
      if (OB_LIKELY(OB_SUCCESS != ret)) {
        LIB_LOG(ERROR, "unregister checker failed", K(ret));
      } else if (call.is_empty()) {
        // Delete here must be successful
        ignore = map_.erase_refactored(tid, nullptr);
        assert(ignore == OB_SUCCESS);
      }
      if (OB_SUCC(ret)) {
        int64_t rc = ATOMIC_AAF(&(checker_node->checker_->ref_count_), -1);
        if (0 == rc) {
          checker_node->checker_->clear_status();
        } else {
          // for nested interrupt, only clear interrupt status
          checker_node->checker_->clear_interrupt_status();
        }
        ob_delete(checker_node);
      }
    }
  }
  return ret;
}

void ObGlobalInterruptManager::destroy()
{
  if (IS_INIT) {
    map_.destroy();
  }
}

int ObGlobalInterruptManager::interrupt(const ObInterruptibleTaskID &tid, ObInterruptCode &interrupt_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "interrupt manager not inited", K(tid));
  } else {
    ObInterruptCheckerUpdateCall updatecall(interrupt_code);
    ret = map_.atomic_refactored(tid, updatecall);
    ret = ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : ret;
  }
  return ret;
}

int ObGlobalInterruptManager::interrupt(const ObAddr &dst, const ObInterruptibleTaskID &tid,
                                        ObInterruptCode &interrupt_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "interrupt manager not inited", K(dst), K(tid), K(interrupt_code), K(ret));
  } else if (dst == local_) {
    ObInterruptCheckerUpdateCall updatecall(interrupt_code);
    //Consider that in the remote call, the execution time of the suspend command is later than the completion time of the remote execution. At this time, it should not be handled according to the sending failure.
    ret = map_.atomic_refactored(tid, updatecall);
    ret = ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : ret;
  } else {
    ObInterruptMessage message(tid.first_, tid.last_, interrupt_code);
    ret = rpc_proxy_->to(dst).remote_interrupt_call(message, NULL);
    if (OB_UNLIKELY(OB_SUCCESS != ret)) {
      LIB_LOG(WARN, "fail to send remote interrupt call", K(dst), K(tid), K(interrupt_code), K(ret));
    }
  }
  return ret;
}

int ObGlobalInterruptManager::create_checker_node(ObInterruptChecker *checker,
                                                  ObInterruptCheckerNode *&checker_node)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObMemAttr attr(GET_TENANT_ID(), ObModIds::OB_INTERRUPT_CHECKER_NODE, common::ObCtxIds::DEFAULT_CTX_ID);
  if (OB_ISNULL(checker)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invaild checker pointer", K(ret));
  } else if (OB_ISNULL(ptr = ob_malloc(sizeof(ObInterruptCheckerNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "fail to alloc memory in interrupt manager", K(ret));
  } else {
    checker_node = new(ptr) ObInterruptCheckerNode();
    checker_node->checker_ = checker;
    checker_node->next_ = NULL;
    checker_node->prev_ = NULL;
  }
    return ret;
}

void ObInterruptCheckerAddCall::operator()(hash::HashMapPair<ObInterruptibleTaskID,
    ObInterruptCheckerNode *> &entry)
{
  // The map only stores the head of the linked list, so add it directly from the head
  if (entry.second != nullptr) {
    checker_node_->next_ = entry.second;
    entry.second->prev_ = checker_node_;
    entry.second = checker_node_;
    is_empty_ = false;
  } else {
    is_empty_ = true;
  }
}

void ObInterruptCheckerRemoveCall::operator()(
    hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry)
{
  is_empty_ = true;
  if (checker_node_->next_ != nullptr) {
    checker_node_->next_->prev_ = checker_node_->prev_;
    is_empty_ = false;
  }
  if (checker_node_->prev_ != nullptr) {
    checker_node_->prev_->next_ = checker_node_->next_;
    is_empty_ = false;
  } else {
    // Prev is empty, which means that the current deleted element is the head of the linked list pointed to by map_value, and head is set to next
    entry.second = checker_node_->next_;
  }
  checker_node_->prev_ = nullptr;
  checker_node_->next_ = nullptr;
}

void ObInterruptCheckerUpdateCall::operator()(
    hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry)
{
  auto x = entry.second;
  while (OB_NOT_NULL(x)) {
    x->checker_->interrupt(code_);
    x = x->next_;
  }
}

void ObInterruptGetCheckerNodeCall::operator()
    (hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry)
{
  auto x = entry.second;
  while (OB_NOT_NULL(x)) {
    if (x->checker_ == checker_) {
      checker_node_ = x;
      checker_exist_ = true;
      break;
    }
    x = x->next_;
  }
}

ObGlobalInterruptManager *ObGlobalInterruptManager::instance_ = new ObGlobalInterruptManager();

} // namespace common
} // namespace oceanbase
