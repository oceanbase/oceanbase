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

#include "lib/allocator/ob_malloc.h"
#include "ob_election_async_log.h"
#include "ob_election_base.h"
#include "ob_election_group.h"
#include "ob_election_group_mgr.h"
#include "ob_election_group_cache.h"

using namespace oceanbase::election;
using namespace oceanbase::common;

/*******************************[define for ObElectionGroupKey]***************************/

ObElectionGroupKey::ObElectionGroupKey(
    const uint64_t tenant_id, const ObAddr& part_leader, const int64_t replica_num, const ObMemberList& member_list)
    : tenant_id_(tenant_id), part_leader_(part_leader), replica_num_(replica_num), member_list_(member_list)
{
  uint64_t hash_val = 0;
  int64_t part_leader_hash = part_leader.hash();
  uint64_t member_hash = member_list.hash();
  hash_val = murmurhash(&tenant_id, sizeof(tenant_id), hash_val);
  hash_val = murmurhash(&part_leader_hash, sizeof(part_leader_hash), hash_val);
  hash_val = murmurhash(&replica_num, sizeof(replica_num), hash_val);
  hash_val = murmurhash(&member_hash, sizeof(member_hash), hash_val);
  hash_value_ = hash_val;
}

bool ObElectionGroupKey::is_valid() const
{
  return tenant_id_ != 0 && part_leader_.is_valid() && replica_num_ != 0 && member_list_.is_valid();
}

bool ObElectionGroupKey::operator==(const ObElectionGroupKey& other) const
{
  return tenant_id_ == other.get_tenant_id() && part_leader_ == other.get_part_leader() &&
         replica_num_ == other.get_replica_num() && member_list_.member_addr_equal(other.get_member_list());
}

/*******************************[define for ObElectionGroupCache::ElementFactory]***************************/

// Define for ElectionGroupCache::ElementFactory
int ObElectionGroupCache::ElementFactory::create_queue(ObSpLinkQueue*& queue)
{
  int ret = OB_SUCCESS;
  if (nullptr == (queue = (ObSpLinkQueue*)ob_malloc(sizeof(ObSpLinkQueue), ObModIds::OB_ELECTION))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ELECT_ASYNC_LOG(WARN, "alloc new queue failed", K(ret));
  } else {
    queue = new (queue) ObSpLinkQueue();
    ATOMIC_INC(&queue_count_);
  }
  return ret;
}

int ObElectionGroupCache::ElementFactory::create_egid_node(const ObElectionGroupId& id, LinkedElectionGroupId*& node)
{
  int ret = OB_SUCCESS;
  if (nullptr == (node = (LinkedElectionGroupId*)ob_malloc(sizeof(LinkedElectionGroupId), ObModIds::OB_ELECTION))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ELECT_ASYNC_LOG(WARN, "alloc new eg_id node failed", K(id), K(ret));
  } else {
    node = new (node) LinkedElectionGroupId(id);
    ATOMIC_INC(&egid_count_);
  }
  return ret;
}

void ObElectionGroupCache::ElementFactory::release_queue(ObSpLinkQueue* queue)
{
  ELECT_ASYNC_LOG(INFO, "release queue", KP(queue));
  queue->~ObSpLinkQueue();
  ob_free(queue);
  ATOMIC_DEC(&queue_count_);
}

void ObElectionGroupCache::ElementFactory::release_egid_node(LinkedElectionGroupId* node)
{
  ELECT_ASYNC_LOG(INFO, "release node", KP(node));
  node->~LinkedElectionGroupId();
  ob_free(node);
  ATOMIC_DEC(&egid_count_);
}

int64_t ObElectionGroupCache::ElementFactory::get_queue_count() const
{
  return ATOMIC_LOAD(&queue_count_);
}

int64_t ObElectionGroupCache::ElementFactory::get_egid_count() const
{
  return ATOMIC_LOAD(&egid_count_);
}

/*******************************[define for ObElectionGroupCache::Functors]***************************/

// not thread-safe, only called by get_or_create_eg_id()
// fetch out a valid eg_id from queue, create a new one if there not exist a valid one.
// @param [in] key map's key
// @param [in] queue map's value
// @return true-success, false-failed
bool ObElectionGroupCache::GetOrCreateOperation::operator()(const ObElectionGroupKey& key, ObSpLinkQueue*& queue)
{
  int ret = OB_SUCCESS;
  int temp_ret = OB_SUCCESS;
  ObSpLinkQueue::Link* temp_node = nullptr;

  if (nullptr == queue || nullptr == election_group_mgr_) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR,
        "invalid argument, pointer is NULL, shouldn't meet, something wrong",
        K(key),
        KP(queue),
        K(election_group_mgr_),
        K(ret));
  } else {
    do {
      if (queue->is_empty()) {  // create a new node
        LinkedElectionGroupId* linked_group_id = nullptr;
        if (OB_FAIL(election_group_mgr_->create_new_eg_leader(key.get_tenant_id(),
                key.get_member_list(),
                key.get_replica_num(),
                eg_id_))) {  // create a new election group
          ELECT_ASYNC_LOG(ERROR, "create new election group operation failed", K(key), KP(queue), K(ret));
        } else if (OB_FAIL(factory_.create_egid_node(eg_id_, linked_group_id))) {
          ELECT_ASYNC_LOG(ERROR, "create new eg_id node failed", K(key), K(eg_id_), K(ret));
          // break loop anyway, avoid dead loop
          if (OB_SUCCESS != (temp_ret = election_group_mgr_->delete_election_group(eg_id_))) {
            ELECT_ASYNC_LOG(ERROR, "failed to delete election group", K(key), K(eg_id_), K(temp_ret));
          } else {
            // do nothing, ret is not OB_SUCCESS, will break
          }
        } else if (OB_FAIL(queue->push(linked_group_id))) {  // put new node into queue
          factory_.release_egid_node(linked_group_id);
          ELECT_ASYNC_LOG(
              ERROR, "push new created node to queue failed", K(key), K(linked_group_id->get_eg_id()), K(ret));
          // break loop anyway, avoid dead loop
          if (OB_SUCCESS != (temp_ret = election_group_mgr_->delete_election_group(eg_id_))) {
            ELECT_ASYNC_LOG(ERROR, "failed to delete election group", K(key), K(eg_id_), K(temp_ret));
          } else {
            // do nothing, ret is not OB_SUCCESS, will break
          }
        } else {
          // ret is OB_SUCCESS, try again
        }
      } else if (OB_FAIL(queue->top(temp_node)) || nullptr == temp_node) {  // get the top node
        ELECT_ASYNC_LOG(ERROR,
            "queue top operation failed or top node is nullptr",
            K(key),
            K(eg_id_),
            KP(queue),
            K(temp_node),
            K(ret));

        if (OB_SUCCESS != (temp_ret = queue->pop(temp_node))) {  // pop and get next
          ELECT_ASYNC_LOG(
              ERROR, "queue pop node failed, not expected", K(key), K(eg_id_), KP(queue), KP(temp_node), K(temp_ret));
        } else if (nullptr == temp_node) {
          ELECT_ASYNC_LOG(ERROR, "node poped from queue shouldn't be nullptr", K(key), K(ret));
        } else {
          factory_.release_egid_node(static_cast<LinkedElectionGroupId*>(temp_node));
          ret = temp_ret;
          // ret is OB_SUCCESS, try again
        }
      } else if (OB_FAIL(check_eg_id_valid_(static_cast<LinkedElectionGroupId*>(temp_node)->get_eg_id(),
                     key,
                     election_group_map_))) {                    // check if the eg_id is valid
        if (OB_SUCCESS != (temp_ret = queue->pop(temp_node))) {  // pop and get next
          ELECT_ASYNC_LOG(ERROR, "queue pop node failed, not expected", K(key), KP(queue), KP(temp_node), K(temp_ret));
        } else {
          factory_.release_egid_node(static_cast<LinkedElectionGroupId*>(temp_node));
          if (OB_STATE_NOT_MATCH == ret ||  // need retry(eg is full or predestroy)
              OB_ENTRY_NOT_EXIST == ret) {  // eg not exist anymore, need retry
            ret = temp_ret;
          } else {
            // ret is not OB_SUCCESS, will break
          }
        }
      } else {
        eg_id_ = static_cast<LinkedElectionGroupId*>(temp_node)->get_eg_id();  // get a valid eg_id
        break;
      }
    } while (OB_SUCCESS == ret);  // need retry
  }

  operation_ret_ = ret;
  return OB_SUCCESS == ret;
}

// called by put_eg_id()
// @param [in] key map's key
// @param [in] queue map's 'value
// @return true-success, false-failed
bool ObElectionGroupCache::PutOperation::operator()(const ObElectionGroupKey& key, ObSpLinkQueue*& queue)
{
  int ret = OB_SUCCESS;
  LinkedElectionGroupId* linked_eg_id_node = nullptr;

  if (nullptr == queue) {
    ret = OB_ERR_UNEXPECTED;
    ELECT_ASYNC_LOG(ERROR, "queue is nullptr", K(key), KP(queue), K(eg_id_), K(ret));
  } else if (OB_FAIL(factory_.create_egid_node(eg_id_, linked_eg_id_node))) {
    ELECT_ASYNC_LOG(ERROR, "create eg_id node failed", K(key), KP(queue), K(eg_id_), K(ret));
  } else if (OB_FAIL(queue->push(linked_eg_id_node))) {
    factory_.release_egid_node(linked_eg_id_node);
    ELECT_ASYNC_LOG(ERROR, "queue push operation failed", K(key), K(eg_id_), K(ret));
  } else {
    // do nothing
  }

  operation_ret_ = ret;
  return OB_SUCCESS == ret;
}

// called by reset() and destory()
// collect and clean invalid info
// @param [in] key maps' key
// @param [in] queue map's value
// @return always true
bool ObElectionGroupCache::ClearOperation::operator()(const ObElectionGroupKey& key, common::ObSpLinkQueue*& queue)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link* temp_node = nullptr;

  if (nullptr == queue) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(
        ERROR, "invalid argument, pointer is NULL, shouldn't meet, something wrong", K(key), KP(queue), K(ret));
  } else {
    while (!queue->is_empty()) {
      if (OB_FAIL(queue->pop(temp_node))) {  // memory leak, error can't handle
        ELECT_ASYNC_LOG(ERROR, "pop node from queue failed, the error can't handle", K(key), K(ret));
        break;  // break anyway, avoid dead loop
      } else if (nullptr == temp_node) {
        ELECT_ASYNC_LOG(ERROR, "node poped from queue shouldn't be nullptr", K(key), K(ret));
      } else {
        factory_.release_egid_node(static_cast<LinkedElectionGroupId*>(temp_node));
      }
    }
    factory_.release_queue(queue);
  }

  return true;
}

// called by gc_invalid_queue()
// try gc a queue in map
// @param [in] key map's key
// @param [in] queue map's value
// @return true-destroy the queue
bool ObElectionGroupCache::TryGCNodeOperation::operator()(const ObElectionGroupKey& key, common::ObSpLinkQueue*& queue)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link* temp_node = nullptr;
  bool is_queue_released = false;

  if (nullptr == queue) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(
        ERROR, "invalid argument, pointer is NULL, shouldn't meet, something wrong", K(key), KP(queue), K(ret));
  } else {
    do {
      bool need_pop_falg = false;

      if (queue->is_empty()) {
        factory_.release_queue(queue);
        is_queue_released = true;
        break;
      } else if (OB_FAIL(queue->top(temp_node))) {
        ELECT_ASYNC_LOG(ERROR, "queue top operation failed", K(key), KP(queue), K(temp_node), K(ret));
        need_pop_falg = true;
      } else if (nullptr == temp_node) {
        ELECT_ASYNC_LOG(ERROR, "node value is nullptr", K(key), KP(queue), K(temp_node), K(ret));
        need_pop_falg = true;
      } else if (OB_FAIL(check_eg_id_valid_(
                     static_cast<LinkedElectionGroupId*>(temp_node)->get_eg_id(), key, election_group_map_))) {
        need_pop_falg = true;
      } else {
        break;
      }

      if (true == need_pop_falg) {
        if (OB_FAIL(queue->pop(temp_node))) {
          ELECT_ASYNC_LOG(ERROR,
              "pop node from queue failed, the error can't handle",
              K(key),
              KP(queue),
              K(temp_node),
              K(ret));  // will break
        } else if (nullptr == temp_node) {
          ELECT_ASYNC_LOG(ERROR, "node poped from queue shouldn't be nullptr", K(key), K(ret));
        } else {
          factory_.release_egid_node(static_cast<LinkedElectionGroupId*>(temp_node));
        }
      } else {
      }
    } while (OB_SUCCESS == ret);
  }

  return is_queue_released;  // true-gc <key,value>, false-skip
}

/*******************************[define for ObElectionGroupCache itself]***************************/

int ObElectionGroupCache::init(ElectionGroupMap* election_group_map, ObElectionGroupMgr* election_group_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    ELECT_ASYNC_LOG(ERROR, "init already", K(ret));
  } else if (nullptr == election_group_map || nullptr == election_group_mgr) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "pointer in args is nullptr", KP(election_group_map), KP(election_group_mgr), K(ret));
  } else if (OB_FAIL(election_group_id_cache_map_.init())) {
    ELECT_ASYNC_LOG(ERROR, "election_group_id_cache_map_ init failed", K(ret));
  } else {
    election_group_map_ = election_group_map;
    election_group_mgr_ = election_group_mgr;
    is_inited_ = true;
  }

  return ret;
}

// for now, just for unit test
int64_t ObElectionGroupCache::get_group_num()
{
  int64_t size = -1;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(ERROR, "not init yet");
  } else if (nullptr == election_group_map_) {
    ELECT_ASYNC_LOG(ERROR, "election_group_map_ shouldn't be nullptr");
  } else {
    size = election_group_map_->size();
  }

  return size;
}

// for now, just for unit test
int64_t ObElectionGroupCache::get_queue_num()
{
  return factory_.get_queue_count();
}

// for now, just for unit test
int64_t ObElectionGroupCache::get_node_num()
{
  return factory_.get_egid_count();
}

// @return void
void ObElectionGroupCache::reset()
{
  ClearOperation clear_operation(factory_);
  election_group_id_cache_map_.for_each(clear_operation);
  election_group_id_cache_map_.clear();
}

void ObElectionGroupCache::destroy()
{
  reset();
  election_group_id_cache_map_.destroy();
}

// put a eg_id into queue, when a election instance move out from that group
// @param [in] key key of queue
// @param [in] eg_id eg_id cached
// @return err code
int ObElectionGroupCache::put_eg_id(const ObElectionGroupKey& key, const ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  PutOperation put_operation(eg_id, factory_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K(key), K(eg_id), K(ret));
  } else if (!key.is_valid() || !eg_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "key or eg_id is not valid", K(key), K(eg_id), K(ret));
  } else if (OB_FAIL(keep_trying_operation_(key,
                 put_operation))) {  // could retry if err can handle
    ELECT_ASYNC_LOG(ERROR, "operation executed failed", K(key), K(ret));
  } else {
    // do nothing
  }

  if (OB_EAGAIN == ret) {
    ret = put_operation.get_operation_ret();
  }
  return ret;
}

// get a eg_id, expected success always
// @param [in] key to locate a group
// @param [out] eg_id id of a election group
// @return error code
int ObElectionGroupCache::get_or_create_eg_id(const ObElectionGroupKey& key, ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  GetOrCreateOperation get_or_create_operation(eg_id, election_group_map_, election_group_mgr_, factory_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K(key), K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "key is not valid", K(key), K(eg_id), K(ret));
  } else if (OB_FAIL(keep_trying_operation_(key, get_or_create_operation))) {
    ELECT_ASYNC_LOG(ERROR, "operation executed failed, and could not try again", K(key), K(ret));
  } else {
    // do nothing
  }

  if (OB_EAGAIN == ret) {
    ret = get_or_create_operation.get_operation_ret();
  } else {
    // do nothing
  }
  return ret;
}

void ObElectionGroupCache::gc_invalid_queue()
{
  if (!is_inited_) {
    ELECT_ASYNC_LOG(ERROR, "not init yet");
  } else {
    TryGCNodeOperation try_gc_node_operation(election_group_map_, factory_);
    election_group_id_cache_map_.remove_if(try_gc_node_operation);
  }
}

// called by GetOrCreateOperation::operator() and TryGCNodeOperation::operator()
// check if a eg_id is valid
// @param [in] key map's key
// @param [in] queue map's value
// @return err code
int ObElectionGroupCache::check_eg_id_valid_(
    const ObElectionGroupId& eg_id, const ObElectionGroupKey& key, ElectionGroupMap* election_group_map)
{
  int ret = OB_SUCCESS;
  ObIElectionGroup* temp_election_group = NULL;
  int64_t partition_count = MAX_EG_PARTITION_NUM;

  if (!eg_id.is_valid() || !key.is_valid() || nullptr == election_group_map) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(
        ERROR, "invalid argument, shouldn't meet, something wrong", K(eg_id), K(key), KP(election_group_map), K(ret));
  } else if (OB_FAIL(election_group_map->get(eg_id, temp_election_group))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ELECT_ASYNC_LOG(INFO,
          "the specified elction group is not in election_group_map anymore",
          K(eg_id),
          KP(election_group_map),
          K(ret));
    } else {
      ELECT_ASYNC_LOG(ERROR,
          "something wrong with election_group_map's get() "
          "operation, not expected",
          K(eg_id),
          KP(election_group_map),
          K(ret));
    }
  } else if (ObElectionRole::ROLE_LEADER != temp_election_group->get_role() ||
             key.get_tenant_id() != temp_election_group->get_tenant_id() ||
             key.get_part_leader() != temp_election_group->get_curr_leader() ||
             key.get_replica_num() != temp_election_group->get_replica_num() ||
             !key.get_member_list().member_addr_equal(temp_election_group->get_member_list())) {
    ret = OB_ERR_UNEXPECTED;
    const ObElectionRole eg_role = temp_election_group->get_role();
    const uint64_t key_tenant_id = key.get_tenant_id();
    const uint64_t eg_tenant_id = temp_election_group->get_tenant_id();
    const ObAddr& key_leader = key.get_part_leader();
    const ObAddr& eg_leader = temp_election_group->get_curr_leader();
    const uint64_t key_replica_num = key.get_replica_num();
    const uint64_t eg_replica_num = temp_election_group->get_replica_num();
    const ObMemberList& key_member_list = key.get_member_list();
    const ObMemberList& eg_member_list = temp_election_group->get_member_list();
    election_group_map->revert(temp_election_group);
    ELECT_ASYNC_LOG(ERROR,
        "the election group key properties changed, which is not expected",
        K(eg_id),
        KP(election_group_map),
        K(ret),
        K(eg_role),
        K(key_tenant_id),
        K(eg_tenant_id),
        K(key_leader),
        K(eg_leader),
        K(key_replica_num),
        K(eg_replica_num),
        K(key_member_list),
        K(eg_member_list));
  } else if ((partition_count = temp_election_group->get_partition_count()) >=
                 MAX_EG_PARTITION_NUM ||                     // not call is_full(), avoid deadlock
             temp_election_group->is_pre_destroy_state()) {  // not permitted when group is full or predestory
    ret = OB_STATE_NOT_MATCH;
    bool eg_full_flag = partition_count >= MAX_EG_PARTITION_NUM;
    bool eg_pre_destory_flag = temp_election_group->is_pre_destroy_state();
    election_group_map->revert(temp_election_group);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG(INFO,
          "this election group is not satisfied",
          K(eg_id),
          KP(election_group_map),
          K(ret),
          K(eg_full_flag),
          K(eg_pre_destory_flag));
    }
  } else {
    election_group_map->revert(temp_election_group);
  }

  return ret;
}

// trying some operation until there occurs a error can not handle
// @param [in] key election_group_id_cache_map_'s key
// @param [in] operation operation on that key,value
// @return error code
template <typename Function>
int ObElectionGroupCache::keep_trying_operation_(const ObElectionGroupKey& key, Function& operation)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue* temp_queue;

  do {
    if (OB_SUCC(election_group_id_cache_map_.operate(key, operation))) {
      break;
    } else if (OB_ENTRY_NOT_EXIST != ret) {  // the only error could handle
      ELECT_ASYNC_LOG(ERROR, "key exist but linear_hashmap operate() failed", K(key), K(ret));
    } else if (OB_FAIL(factory_.create_queue(temp_queue))) {  // key not exist, create a new queue
      ELECT_ASYNC_LOG(ERROR, "create new queue failed", K(key), K(ret));
    } else if (OB_FAIL(election_group_id_cache_map_.insert(key, temp_queue))) {  // put this new queue into map
      if (OB_ENTRY_EXIST != ret) {                                               // can only handle key exist error
        factory_.release_queue(temp_queue);
        ELECT_ASYNC_LOG(ERROR, "insert queue into map failed, the error can't handle", K(key), K(ret));
      } else {  // someone insert a queue in concurrency, release queue and retry
        factory_.release_queue(temp_queue);
        ELECT_ASYNC_LOG(INFO,
            "the key has already exist, someone is faster than me, "
            "fine, I'm going to try again",
            K(key),
            K(ret));
      }
    } else {
      // retry
    }
  } while (OB_SUCCESS == ret ||     // OB_SUCCESS-I create a new queue
           OB_ENTRY_EXIST == ret);  // OB_ENTRY_EXIST-some create a new queue

  return ret;
}