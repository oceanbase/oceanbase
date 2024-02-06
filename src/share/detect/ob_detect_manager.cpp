/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#include "share/detect/ob_detect_manager.h"
#include "lib/ob_running_mode.h"
#include "share/rc/ob_context.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase {
namespace common {

ObDetectableIdGen::ObDetectableIdGen()
{
  detect_sequence_id_ = ObTimeUtil::current_time();
  callback_node_sequence_id_ = ObTimeUtil::current_time();
}

ObDetectableIdGen &ObDetectableIdGen::instance()
{
  static ObDetectableIdGen d_id_gen;
  return d_id_gen;
}

int ObDetectableIdGen::generate_detectable_id(ObDetectableId &detectable_id, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // use server id to ensure that the detectable_id is unique in cluster
  uint64_t server_id = GCTX.server_id_;
  if (!is_valid_server_id(server_id)) {
    ret = OB_SERVER_IS_INIT;
    LIB_LOG(WARN, "[DM] server id is invalid");
  } else {
    detectable_id.first_ = get_detect_sequence_id();
    // use timestamp to ensure that the detectable_id is unique during the process
    uint64_t timestamp = ObTimeUtility::current_time();
    // [ server_id (16bits) ][ timestamp (32bits) ]
    // only if qps > 2^48 or same server reboots after 2^48 the detectable_id be repeated
    detectable_id.second_ = (server_id) << 48 | (timestamp & 0x0000FFFFFFFFFFFF);
    detectable_id.tenant_id_ = tenant_id;
  }
  return ret;
}

int ObDMMultiDlist::init(int64_t tenant_id, int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  bucket_num_ = bucket_num;
  allocator_.set_attr(ObMemAttr(tenant_id, "ObDMMDL"));
  locks_.set_allocator(&allocator_);
  buckets_.set_allocator(&allocator_);
  if (OB_FAIL(locks_.init(bucket_num_))) {
    LIB_LOG(WARN, "[DM] failed to init locks_");
  } else if (OB_FAIL(buckets_.init(bucket_num_))) {
    LIB_LOG(WARN, "[DM] failed to init buckets_");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < bucket_num_; ++i) {
    ObLockWrapper *lock = OB_NEWx(ObLockWrapper, &allocator_);
    ObDList<ObDetectableIdDNode> *dlist = OB_NEWx(ObDList<ObDetectableIdDNode>, &allocator_);
    if (OB_ISNULL(lock) || OB_ISNULL(dlist)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "[DM] failed to allocate");
    } else if (OB_FAIL(locks_.push_back(lock))) {
      LIB_LOG(WARN, "[DM] failed to push_back lock");
    } else if (OB_FAIL(buckets_.push_back(dlist))) {
      LIB_LOG(WARN, "[DM] failed to push_back dlist");
    }
  }
  return ret;
}

void ObDMMultiDlist::destroy() {
  for (int64_t i = 0; i < locks_.count(); ++i) {
    locks_.at(i)->~ObLockWrapper();
    allocator_.free(locks_.at(i));
  }
  for (int64_t i = 0; i < buckets_.count(); ++i) {
    buckets_.at(i)->~ObDList<ObDetectableIdDNode>();
    allocator_.free(buckets_.at(i));
  }
  locks_.destroy();
  buckets_.destroy();
  LIB_LOG(INFO, "[DM] destroy ObDMMultiDlist");
}

void ObDMMultiDlist::add_list_node_tail(const ObDetectableId &detectable_id, ObDetectableIdDNode *node)
{
  int64_t bucket_id = detectable_id.first_ % bucket_num_;
  {
    ObLockGuard<common::ObSpinLock> lock_guard(*locks_.at(bucket_id));
    buckets_.at(bucket_id)->add_last(node);
  }
}

void ObDMMultiDlist::remove_list_node(const ObDetectableId &detectable_id, ObDetectableIdDNode *node)
{
  int64_t bucket_id = detectable_id.first_ % bucket_num_;
  {
    ObLockGuard<common::ObSpinLock> lock_guard(*locks_.at(bucket_id));
    buckets_.at(bucket_id)->remove(node);
  }
}

void ObDMMultiDlist::pop_active_node(
    hash::ObHashSet<ObDetectableId, hash::NoPthreadDefendMode> &still_need_check_id)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtil::current_time();
  for (int64_t i = 0; OB_SUCC(ret) && i < bucket_num_; ++i) {
    bool end_bucket_traverse = false;
    // pop MAX_LOCK_LENGTH nodes per lock time
    while (!end_bucket_traverse && OB_SUCC(ret)) {
      int64_t poped_cnt = 0;
      ObLockGuard<common::ObSpinLock> lock_guard(*locks_.at(i));
      if (buckets_.at(i)->is_empty()) {
        break;
      }
      ObDetectableIdDNode *cur = buckets_.at(i)->get_first();
      ObDetectableIdDNode *next = nullptr;
      while (OB_SUCC(ret) && ++poped_cnt <= MAX_LOCK_LENGTH) {
        if (cur->activate_tm_ > cur_time) {
          // all remain nodes in this bucket are not active, stopping traverse this bucket.
          end_bucket_traverse = true;
          break;
        } else {
          if (OB_FAIL(still_need_check_id.set_refactored(cur->detectable_id_))) {
            if (OB_HASH_EXIST != ret) {
              end_bucket_traverse = true;
              LIB_LOG(WARN, "[DM] failed to set_refactored", K(ret), K(cur->detectable_id_));
              break;
            } else {
              ret = OB_SUCCESS;
            }
          }
          // pop cur node
          next = cur->get_next();
          buckets_.at(i)->remove(cur);
          cur = next;
          if (buckets_.at(i)->is_empty()) {
            end_bucket_traverse = true;
            break;
          }
        }
      }
    }
  }
}

int ObDetectManager::mtl_init(ObDetectManager *&dm)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t mem_limit = lib::get_tenant_memory_limit(tenant_id);
  double mem_factor = mem_limit >= MAX_TENANT_MEM_LIMIT ? 1.0 : (static_cast<double>(mem_limit) / MAX_TENANT_MEM_LIMIT);
  // less memory for meta tenant
  if (is_meta_tenant(tenant_id)) {
    mem_factor = mem_factor * 0.01;
  }
  dm = OB_NEW(ObDetectManager, ObMemAttr(tenant_id, "DetectManager"), tenant_id);
  if (OB_ISNULL(dm)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] failed to alloc detect manager", K(ret));
  } else if (OB_FAIL(dm->init(GCTX.self_addr(), mem_factor))) {
    LIB_LOG(WARN, "[DM] failed to init detect manager", K(ret));
  }
  return ret;
}

void ObDetectManager::mtl_destroy(ObDetectManager *&dm)
{
  if (nullptr != dm) {
    dm->destroy();
    ob_delete(dm);
    dm = nullptr;
  }
}

void ObDetectManager::destroy()
{
  int ret = OB_SUCCESS;
  // destroy dm_multi_list_
  dm_multi_list_.destroy();

  // destroy node and callback in all_check_items_
  FOREACH(iter, all_check_items_) {
    const ObDetectableId &detectable_id = iter->first;
    ObDetectCallbackNode *node = iter->second;
    while (OB_NOT_NULL(node)) {
      ObDetectCallbackNode *next_node = node->next_;
      // DM is destroying means that all tenant threads have already exited,
      // the remain detect callbacks must be
      // ObSingleDfoDetectCB, ObTempTableDetectCB, or ObP2PDataHubDetectCB
      int temp_ret = node->cb_->do_callback();
      LIB_LOG(WARN, "[DM] do callback during mtl destroy",
            K(temp_ret), K(node->cb_->get_trace_id()),
            K(detectable_id), K(node->cb_->get_detect_callback_type()),
            K(node->sequence_id_));
      delete_cb_node(node);
      node = next_node;
    }
  }
  all_check_items_.destroy();
  still_need_check_id_.destroy();
  detectable_ids_.destroy();
  DESTROY_CONTEXT(mem_context_);
  LIB_LOG(INFO, "[DM] destory dm", K_(tenant_id));
}

int ObDetectManager::init(const ObAddr &self, double mem_factor)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(tenant_id_, "DetectManager")
       .set_properties(lib::ALLOC_THREAD_SAFE)
       .set_parallel(4);
  int64_t check_map_bucket_count = static_cast<int64_t>(DEFAULT_CHECK_MAP_BUCKETS_COUNT * mem_factor);
  int64_t detectable_ids_bucket_count = static_cast<int64_t>(DEFAULT_CHECK_MAP_BUCKETS_COUNT * mem_factor);
  int64_t still_need_check_id_bucket_count = static_cast<int64_t>(MIDDLE_SET_BUCKETS_COUNT * mem_factor);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "[DM] invalid arguments", K(ret), K(self));
  } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LIB_LOG(WARN, "[DM] create memory context failed", K(ret));
  } else if (OB_FAIL(all_check_items_.create(check_map_bucket_count,
                                             "HashBuckDmChe",
                                             "HashNodeDmChe",
                                             tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] create hash table failed", K(ret));
  } else if (OB_FAIL(detectable_ids_.create(detectable_ids_bucket_count,
                                            "HashBuckDmId",
                                            "HashNodeDmId",
                                            tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] create hash set failed", K(ret));
  } else if (OB_FAIL(still_need_check_id_.create(still_need_check_id_bucket_count,
                                         "HashBuckDmId",
                                         "HashNodeDmId",
                                         tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] create hash set failed", K(ret));
  } else if (OB_FAIL(dm_multi_list_.init(tenant_id_,
        is_meta_tenant(tenant_id_) ? DM_MDLIST_BUCKETS_COUNT_FOR_META_TENANT : DM_MDLIST_BUCKETS_COUNT))) {
    LIB_LOG(WARN, "[DM] failed to init ObDMMultiDlist", K(ret));
  } else {
    self_ = self;
    is_inited_ = true;
    LIB_LOG(INFO, "[DM] ObDetectManager init success", K(self), K_(tenant_id), K(mem_factor));
  }
  return ret;
}

int ObDetectManager::register_detectable_id(const ObDetectableId &detectable_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "[DM] detect manager not inited", K(ret));
  } else if (detectable_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "[DM] invaild detectable_id, please generate from ObDetectManager", K(ret), K(common::lbt()));
  } else if (OB_FAIL(detectable_ids_.set_refactored(detectable_id, 0/* flag */))) {
    if (OB_HASH_EXIST != ret) {
      LIB_LOG(WARN, "[DM] detectable_ids_ set_refactored failed", K(ret));
    } else {
      LIB_LOG(WARN, "[DM] detectable_id already exists in detectable_ids_");
    }
  }
  return ret;
}

int ObDetectManager::unregister_detectable_id(const ObDetectableId &detectable_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "[DM] detect manager not inited", K(ret));
  } else if (detectable_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "[DM] invaild detectable_id", K(common::lbt()));
  } else if (OB_FAIL(detectable_ids_.erase_refactored((detectable_id)))) {
    LIB_LOG(WARN, "[DM] detectable_ids_ erase_refactored failed", K(ret), K(detectable_id));
  }
  return ret;
}

int ObDetectManager::do_register_check_item(const ObDetectableId &detectable_id, ObDetectCallbackNode *cb_node,
                                         uint64_t &node_sequence_id, bool need_ref)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cb_node)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "[DM] invaild cb node pointer");
  } else {
    // if need_ref is true, which means that work thread may use cb, so add ref_count in case of deleting cb during ObDetectCallbackNodeExecuteCall
    // typical scene: qc may change sqc's task state in callback upon receiving the report message from sqc, but dm may free callback already
    if (need_ref) {
      cb_node->cb_->inc_ref_count();
    }
    node_sequence_id = cb_node->sequence_id_;
    // A slightly more complicated but safe inspection operation
    // Since map does not provide the operation of "create or modify", nor does it provide the ability to hold bucket locks
    // Therefore, add cyclic verification to prevent the occurrence of
    // thread a failed to create --> thread b erased --> thread a failed to modify
    // Such a situation
    ObObDetectCallbackNodeSetCall set_node_call(this);
    do {
      if (OB_HASH_EXIST == (ret = all_check_items_.set_refactored(detectable_id, cb_node, 0, 0, 0, &set_node_call))) {
        ObDetectCallbackNodeAddCall add_node_call(cb_node, this);
        ret = all_check_items_.atomic_refactored(detectable_id, add_node_call);
        // If it is an empty queue, it means that another thread wants to delete the node but unexpectedly got the lock by this thread
        // So do not delete, try to put again according to HASH_NOT_EXIST
        if (add_node_call.is_empty()) {
          ret = OB_HASH_NOT_EXIST;
        }
      }
    } while (ret == OB_HASH_NOT_EXIST);
    // hashmap may set_refactored for alloc failed
    if OB_FAIL(ret) {
      delete_cb_node(cb_node);
    }
  }

  if (OB_SUCC(ret)) {
    LIB_LOG(DEBUG, "[DM] register_check_item", K(ret), K(detectable_id),
            K(cb_node->cb_->get_detect_callback_type()), K(node_sequence_id));
  }
  return ret;
}

int ObDetectManager::unregister_check_item(const ObDetectableId &detectable_id, const uint64_t &node_sequence_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "[DM] detect manager not inited");
  } else if (detectable_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "[DM] invaild detectable_id", K(node_sequence_id) ,K(common::lbt()));
  } else {
    // CHECK_MAP key: ObDetectableId, CHECK_MAP value: linked list with node type ObDetectCallbackNode*
    // remove node from CHECK_MAP with detectable_id(CHECK_MAP key) and node_sequence_id(identify specific node in the linked list)
    // remove kv pair from CHECK_MAP if this node is the last one in the linked list
    ObDetectCallbackNodeRemoveCall remove_node_call(this, node_sequence_id);
    bool is_erased = false;
    if (OB_FAIL(all_check_items_.erase_if(detectable_id, remove_node_call, is_erased))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // if not found, the possible reason is that node is removed by ObDetectCallbackNodeExecuteCall
        LIB_LOG(TRACE, "[DM] unregister cb failed, maybe removed by other thread",
            K(detectable_id), K(node_sequence_id));
      } else {
        LIB_LOG(WARN, "[DM] unregister cb failed", K(ret), K(detectable_id), K(node_sequence_id));
      }
    }
  }
  LIB_LOG(DEBUG, "[DM] unregister_check_item", K(ret), K(detectable_id), K(node_sequence_id));
  return ret;
}

int ObDetectManager::gather_requests(REQUEST_MAP &req_map, lib::MemoryContext &req_map_context)
{
  int ret = OB_SUCCESS;
  IGNORE_RETURN dm_multi_list_.pop_active_node(still_need_check_id_);

  ObSEArray<ObDetectableId, 32> remove_list;
  FOREACH(iter, still_need_check_id_) {
    const ObDetectableId &detectable_id = iter->first;
    ObDetectReqGetCall req_get_call(req_map, req_map_context);
    ret = all_check_items_.read_atomic(detectable_id, req_get_call);
    if (OB_HASH_NOT_EXIST == ret) {
      // already unregistered, remove it. if push_back failed, let next detect loop do this
      if (OB_FAIL(remove_list.push_back(detectable_id))) {
        LIB_LOG(WARN, "[DM] failed to push_back to remove_list", K(ret));
      }
      ret = OB_SUCCESS;
    }
  }

  ARRAY_FOREACH_NORET(remove_list, idx) {
    if (OB_FAIL(still_need_check_id_.erase_refactored(remove_list.at(idx)))) {
      LIB_LOG(WARN, "[DM] failed to erase_refactored from still_need_check_id_");
    }
  }

  LIB_LOG(DEBUG, "[DM] gather_requests ", K(req_map.size()), K(ret));
  return ret;
}

void ObDetectManager::do_detect_local(const ObDetectableId &detectable_id)
{
  int ret = OB_SUCCESS;
  bool task_alive = is_task_alive(detectable_id);
  if (!task_alive) {
    ObCheckStateFinishCall check_state_finish_call(self_);
    if (OB_SUCC(all_check_items_.read_atomic(detectable_id, check_state_finish_call))) {
      if (check_state_finish_call.is_finished()) {
        // nop
      } else {
        // peer exits unexpectly, do detect callbacks
        ObDetectCallbackNodeExecuteCall execute_call(this, detectable_id, self_);
        bool is_erased = false;
        IGNORE_RETURN all_check_items_.erase_if(detectable_id, execute_call, is_erased);
      }
    }
  }
}

void ObDetectManager::do_handle_one_result(const ObDetectableId &detectable_id, const obrpc::ObDetectRpcStatus &rpc_status)
{
  int ret = OB_SUCCESS;
  ObCheckStateFinishCall check_state_finish_call(rpc_status.dst_);
  if (OB_SUCC(all_check_items_.read_atomic(detectable_id, check_state_finish_call))) {
    if (check_state_finish_call.is_finished()) {
      // nop
    } else {
      ObDetectCallbackNodeExecuteCall execute_call(this, detectable_id, rpc_status.dst_);
      bool is_erased = false;
      IGNORE_RETURN all_check_items_.erase_if(detectable_id, execute_call, is_erased);
    }
  }
}

void ObDetectManager::delete_cb_node(ObDetectCallbackNode *&cb_node)
{
  int ret = OB_SUCCESS;
  // cb and cb_node allocated simultaneously, just need to free cb_node
  LIB_LOG(DEBUG, "[DM] dm free cb ", K(cb_node->cb_));
  cb_node->cb_->destroy();
  cb_node->cb_ = nullptr;
  LIB_LOG(DEBUG, "[DM] dm free cbnode ", K(cb_node));
  mem_context_->free(cb_node);
  cb_node = nullptr;
}

int ObDetectManager::ObObDetectCallbackNodeSetCall::operator()(const hash::HashMapPair<ObDetectableId,
    ObDetectCallbackNode *> &entry)
{
  int ret = OB_SUCCESS;
  (void) dm_->dm_multi_list_.add_list_node_tail(entry.first, &entry.second->cb_->d_node_);
  return ret;
}

void ObDetectManager::ObDetectCallbackNodeAddCall::operator()(hash::HashMapPair<ObDetectableId,
    ObDetectCallbackNode *> &entry)
{
  // The map only stores the head of the linked list, so add it directly from the head
  if (entry.second != nullptr) {
    cb_node_->next_ = entry.second;
    entry.second->prev_ = cb_node_;
    entry.second = cb_node_;
    (void) dm_->dm_multi_list_.add_list_node_tail(entry.first, &cb_node_->cb_->d_node_);
    is_empty_ = false;
  } else {
    is_empty_ = true;
  }
}

bool ObDetectManager::ObDetectCallbackNodeRemoveCall::operator()(hash::HashMapPair<ObDetectableId,
    ObDetectCallbackNode *> &entry)
{
  auto node = entry.second;
  int node_cnt = 0; // node_cnt in linked list, only if node_cnt == 0 the kv pair can be deleted.
  while (OB_NOT_NULL(node)) {
    node_cnt++;
    if (node_sequence_id_ == node->sequence_id_) {
      found_node_ = true;
      if (node->next_ != nullptr) {
        node->next_->prev_ = node->prev_;
        // loop will be break after found node, but next node is not null
        // add node_cnt to prevent to delete the kv pair
        node_cnt++;
      }
      if (node->prev_ != nullptr) {
        node->prev_->next_ = node->next_;
      } else {
        // Prev is empty, which means that the current deleted element is the head of the linked list pointed to by map_value, and head is set to next
        entry.second = node->next_;
      }
      (void) dm_->dm_multi_list_.remove_list_node(entry.first, &node->cb_->d_node_);
      dm_->delete_cb_node(node);
      node_cnt--;
      break;
    }
    node = node->next_;
  }
  // 0 == node_cnt means that the linked list is empty, return true so that erase_if can erase the kv pair
  return (0 == node_cnt);
}

bool ObDetectManager::ObDetectCallbackNodeExecuteCall::operator()(hash::HashMapPair<ObDetectableId,
    ObDetectCallbackNode *> &entry)
{
  int ret = OB_SUCCESS;
  const ObDetectableId &detectable_id = entry.first;
  ObDetectCallbackNode *node = entry.second;
  int node_cnt = 0; // node_cnt in linked list, remove kv pair if the last node is removed
  while (OB_NOT_NULL(node)) {
    ret = OB_SUCCESS;
    node_cnt++;
    ObDetectCallbackNode *next_node = node->next_;
    // if a callback can be reentrant, don't set is_executed so that it can be do for several times
    // typical scene: qc detects sqc, if at least 2 sqc failed, both of them should be set not_need_report,
    // the callback should be reentrant and node not set to executed
    if (!node->is_executed()) {
      LIB_LOG(WARN, "[DM] DM found peer not exist, execute detect callback",
              K(node->cb_->get_trace_id()), K(from_svr_addr_),
              K(detectable_id), K(node->cb_->get_detect_callback_type()), K(node->sequence_id_));
      node->cb_->set_from_svr_addr(from_svr_addr_);
      if (OB_FAIL(node->cb_->do_callback())) {
        // if do_callback failed, reset state to running for next detect loop
        node->cb_->atomic_set_running(from_svr_addr_);
        LIB_LOG(WARN, "[DM] failed to do_callback",
                K(node->cb_->get_trace_id()), K(from_svr_addr_), K(detectable_id),
                K(node->cb_->get_detect_callback_type()), K(node->sequence_id_));
      } else {
        if (!node->cb_->reentrant()) {
          node->set_executed();
        }
        // ref_count > 0 means that cb is still referred by work thread, don‘t remove it from the linked list
        int64_t ref_count = node->cb_->get_ref_count();
        if (0 == ref_count) {
          if (node->next_ != nullptr) {
            node->next_->prev_ = node->prev_;
          }
          if (node->prev_ != nullptr) {
            node->prev_->next_ = node->next_;
          } else {
            // Prev is empty, which means that the current deleted element is the head of the linked list pointed to by map_value, and head is set to next
            entry.second = node->next_;
          }
          (void) dm_->dm_multi_list_.remove_list_node(entry.first, &node->cb_->d_node_);
          dm_->delete_cb_node(node);
          node_cnt--;
        }
      }
    }
    node = next_node;
  }
  return 0 == node_cnt;
}

void ObDetectManager::ObCheckStateFinishCall::operator()(hash::HashMapPair<ObDetectableId,
    ObDetectCallbackNode *> &entry)
{
  ObDetectCallbackNode *node = entry.second;
  // check task has already been marked as finished
  while (OB_NOT_NULL(node)) {
    ObTaskState state = ObTaskState::RUNNING;
    if (OB_SUCCESS == node->cb_->atomic_set_finished(addr_, &state)) {
      if (ObTaskState::FINISHED == state) {
        finished_ = true;
      }
      break;
    }
    node = node->next_;
  }
}

void ObDetectManager::ObDetectReqGetCall::operator()(hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry)
{
  int ret = OB_SUCCESS;
  const ObDetectableId &peer_id = entry.first;
  ObDetectCallbackNode *cb_node = entry.second;

  while (OB_NOT_NULL(cb_node)) {
    if (!cb_node->is_executed()) {
      ObIArray<ObPeerTaskState> &peer_states = cb_node->cb_->get_peer_states();
      ARRAY_FOREACH_NORET(peer_states, idx) {
        ObPeerTaskState &peer_state = peer_states.at(idx);
        // only detect running tasks
        if ((int32_t)ObTaskState::FINISHED == ATOMIC_LOAD((int32_t*)&peer_state)) {
          continue;
        }
        obrpc::ObTaskStateDetectReq **req_ptr_ptr = req_map_.get(peer_state.peer_addr_);
        if (OB_NOT_NULL(req_ptr_ptr)) {
          if (OB_FAIL((*req_ptr_ptr)->peer_ids_.set_refactored(peer_id, 0/* flag */))) {
            if (OB_HASH_EXIST != ret) {
              LIB_LOG(WARN, "[DM] peer_ids_ set_refactored failed", K(peer_id),
                      K(peer_state.peer_addr_), K(cb_node->cb_->get_trace_id()));
            } else {
              // this peer id has already been set, ignore
              ret = OB_SUCCESS;
            }
          }
        } else {
          obrpc::ObTaskStateDetectReq *req_ptr =
              OB_NEWx(obrpc::ObTaskStateDetectReq, &req_map_context_->get_arena_allocator());
          if (OB_ISNULL(req_ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LIB_LOG(WARN, "[DM] failed to new ObTaskStateDetectReq", K(peer_id),
                    K(peer_state.peer_addr_), K(cb_node->cb_->get_trace_id()));
          } else if (OB_FAIL(req_ptr->peer_ids_.set_refactored(peer_id, 0/* flag */))) {
            LIB_LOG(WARN, "[DM] peer_ids_ set_refactored failed", K(peer_id),
                    K(peer_state.peer_addr_), K(cb_node->cb_->get_trace_id()));
          } else if (OB_FAIL(req_map_.set_refactored(peer_state.peer_addr_, req_ptr))) {
            LIB_LOG(WARN, "[DM] req_map_ set req failed", K(peer_state.peer_addr_),
                    K(ret), K(peer_state.peer_addr_), K(cb_node->cb_->get_trace_id()));
          }
        }
      }
    }
    cb_node = cb_node->next_;
  }
}

ObDetectManagerThread &ObDetectManagerThread::instance()
{
  static ObDetectManagerThread dm_thread;
  return dm_thread;
}

int ObDetectManagerThread::init(const ObAddr &self, rpc::frame::ObReqTransport *transport)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid() || nullptr == transport) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "[DM] invalid arguments", K(ret), K(self), KP(transport));
  } else if (OB_FAIL(rpc_proxy_.init(transport, self))) {
    LIB_LOG(WARN, "[DM] rpc_proxy_.init failed", K(ret));
  } else if (OB_FAIL(req_map_.create(DEFAULT_REQUEST_MAP_BUCKETS_COUNT,
                                     "HashBuckDmReq",
                                     "HashNodeDmReq"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] create hash set failed", K(ret));
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LIB_LOG(WARN, "[DM] failed to init cond_", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::DetectManager, *this))) {
    LIB_LOG(WARN, "[DM] schedule detect without timer failed", K(ret));
  } else {
    self_ = self;
    is_inited_ = true;
    LIB_LOG(INFO, "[DM] ObDetectManagerThread init success", K(self));
  }
  return ret;
}

void ObDetectManagerThread::run1()
{
  lib::set_thread_name("ObDetectManagerThread");
  IGNORE_RETURN detect();
}

int ObDetectManagerThread::detect() {
  static int loop_time = 0;
  int ret = OB_SUCCESS;
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    const int64_t start_time = ObTimeUtility::current_time();
    int64_t send_cnt = 0; // mark rpc cnt

    // 1. Gather all request from CHECK_MAP into REQUEST_MAP, clustered by addr.
    // 2. Send all requests ip by ip. If is local, directly access detectable_ids_.
    // 3. Process rpc result
    // All requests are allocated from TEMP_CONTEXT to simplify memory management.
    CREATE_WITH_TEMP_CONTEXT(lib::ContextParam().set_label(ObModIds::OB_TEMP_VARIABLES)) {
      lib::MemoryContext &temp_mem_context = CURRENT_CONTEXT;
      common::ObArray<uint64_t> tenant_ids;
      if (OB_ISNULL(GCTX.omt_)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "[DM] unexpected null of GCTX.omt_", K(ret));
      } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
        LIB_LOG(WARN, "[DM] fail to get_mtl_tenant_ids", K(ret));
      } else {
        for (int64_t i = 0; i < tenant_ids.size(); i++) {
          MTL_SWITCH(tenant_ids.at(i)) {
            ObDetectManager* dm = MTL(ObDetectManager*);
            if (OB_ISNULL(dm)) {
              LIB_LOG(WARN, "[DM] dm is null", K(tenant_ids.at(i)));
            } else if (OB_FAIL(dm->gather_requests(req_map_, temp_mem_context))) {
              LIB_LOG(WARN, "[DM] failed to gather_requests", K(tenant_ids.at(i)));
            }
          }
          // ignore errors at switching tenant
          ret = OB_SUCCESS;
        }
        if (FALSE_IT(send_requests(req_map_, send_cnt, temp_mem_context))) {
        } else if (FALSE_IT(handle_rpc_results(send_cnt, temp_mem_context))) {
        }
        FOREACH(it, req_map_) {
          it->second->destroy();
        }
      }
    }
    req_map_.reuse();
    const int64_t cost_time = ObTimeUtility::current_time() - start_time;
    if (cost_time > DETECT_COST_TIME_THRESHOLD) {
      LIB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "[DM] detect_loop_ cost too much time", K(cost_time));
    }

    int32_t sleep_time = DETECT_INTERVAL - static_cast<const int32_t>(cost_time);
    if (sleep_time < 0) {
      sleep_time = 0;
    } else {
      ob_usleep(sleep_time);
    }
    ++loop_time;
    LIB_LOG(DEBUG, "[DM] detect has execute ", K(loop_time));
  }
  return ret;
}

void ObDetectManagerThread::send_requests(REQUEST_MAP &req_map, int64_t &send_cnt, lib::MemoryContext &mem_context)
{
  FOREACH(iter, req_map) {
    const common::ObAddr &dst = iter->first;
    obrpc::ObTaskStateDetectReq *req = iter->second;
    if (dst == self_) {
      detect_local(req);
    } else {
      detect_remote(dst, req, send_cnt, mem_context);
    }
  }
}

void ObDetectManagerThread::detect_local(const obrpc::ObTaskStateDetectReq *req)
{
  int ret = OB_SUCCESS;
  FOREACH(iter, req->peer_ids_) {
    const ObDetectableId &detectable_id = iter->first;
    MTL_SWITCH(detectable_id.tenant_id_) {
      ObDetectManager* dm = MTL(ObDetectManager*);
      if (OB_ISNULL(dm)) {
        LIB_LOG(WARN, "[DM] dm is null", K(detectable_id.tenant_id_));
      } else {
        dm->do_detect_local(detectable_id);
      }
    }
  }
}

void ObDetectManagerThread::detect_remote(const common::ObAddr &dst,
    const obrpc::ObTaskStateDetectReq *req, int64_t &send_cnt, lib::MemoryContext &mem_context)
{
  int ret = OB_SUCCESS;
  obrpc::ObDetectRpcStatus *rpc_status = OB_NEWx(obrpc::ObDetectRpcStatus, &mem_context->get_malloc_allocator(), dst);
  LIB_LOG(DEBUG, "[DM] dm new rpc_status");
  if (OB_ISNULL(rpc_status)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] failed to allocate rpc_status");
  } else if (OB_FAIL(rpc_statuses_.push_back(rpc_status))) {
    rpc_status->destroy();
    mem_context->free(rpc_status);
    LIB_LOG(WARN, "[DM] failed to push_back rpc_status into rpc_statuses_");
  } else {
    obrpc::ObTaskStateDetectAsyncCB cb(cond_, rpc_status, dst);
    if (OB_FAIL(rpc_proxy_.to(dst).by(OB_SYS_TENANT_ID).timeout(DETECT_MSG_TIMEOUT).detect_task_state(*req, &cb))) {
      rpc_statuses_.pop_back();
      rpc_status->destroy();
      mem_context->free(rpc_status);
      LIB_LOG(WARN, "[DM] failed do rpc detect_task_state");
    } else {
      ++send_cnt;
    }
  }
}

void ObDetectManagerThread::handle_rpc_results(int64_t &send_cnt, lib::MemoryContext &mem_context)
{
  int ret = OB_SUCCESS;
  int64_t return_cb_cnt = 0;
  while (return_cb_cnt < send_cnt) {
    ObThreadCondGuard guard(cond_);
    // wait for timeout or until notified.
    cond_.wait_us(COND_WAIT_TIME_USEC);
    ARRAY_FOREACH_NORET(rpc_statuses_, idx) {
      obrpc::ObDetectRpcStatus &rpc_status = *rpc_statuses_.at(idx);
      if (!rpc_status.is_visited() && rpc_status.is_timeout()) {
        return_cb_cnt++;
        rpc_status.set_visited(true);
      } else if (!rpc_status.is_visited() && rpc_status.is_processed()) {
        return_cb_cnt++;
        rpc_status.set_visited(true);
        IGNORE_RETURN handle_one_result(rpc_status);
      }
    }
  }
  ARRAY_FOREACH_NORET(rpc_statuses_, idx) {
    obrpc::ObDetectRpcStatus *rpc_status = rpc_statuses_.at(idx);
    LIB_LOG(DEBUG, "[DM] dm free rpc_status");
    rpc_status->destroy();
    mem_context->get_malloc_allocator().free(rpc_status);
  }
  rpc_statuses_.reset();
}

void ObDetectManagerThread::handle_one_result(const obrpc::ObDetectRpcStatus &rpc_status)
{
  int ret = OB_SUCCESS;
  const obrpc::ObTaskStateDetectResp &cb_result = rpc_status.response_;
  ARRAY_FOREACH_NORET(cb_result.task_infos_, idx) {
    const obrpc::TaskInfo &task = cb_result.task_infos_.at(idx);
    if (common::ObTaskState::FINISHED == task.task_state_) {
      const ObDetectableId &detectable_id = task.task_id_;
      MTL_SWITCH(detectable_id.tenant_id_) {
        ObDetectManager* dm = MTL(ObDetectManager*);
        if (OB_ISNULL(dm)) {
          LIB_LOG(WARN, "[DM] dm is null", K(detectable_id.tenant_id_));
        } else {
          dm->do_handle_one_result(detectable_id, rpc_status);
        }
      }
    }
  }
}

} // end namespace common
} // end namespace oceanbase
