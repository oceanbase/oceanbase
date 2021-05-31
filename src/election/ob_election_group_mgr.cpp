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

#include "ob_election_mgr.h"
#include "ob_election_group_mgr.h"
#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"

namespace oceanbase {
namespace election {
int64_t ObElectionGroupFactory::alloc_count_ = 0;
int64_t ObElectionGroupFactory::release_count_ = 0;

ObIElectionGroup* ObElectionGroupFactory::alloc_election_group(const uint64_t tenant_id)
{
  ObIElectionGroup* eg = NULL;
  int ret = OB_SUCCESS;
  ObTenantMutilAllocator* allocator = NULL;
  if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_mutil_allocator(tenant_id, allocator))) {
    ELECT_ASYNC_LOG(WARN, "get_tenant_log_allocator failed", K(ret), K(tenant_id));
  } else if (NULL == (eg = allocator->alloc_election_group())) {
    ELECT_ASYNC_LOG(WARN, "alloc election_group error");
  } else {
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }
  return eg;
}

void ObElectionGroupFactory::release_election_group(ObIElectionGroup* eg)
{
  if (NULL == eg) {
    ELECT_ASYNC_LOG(WARN, "ObIElectionGroup is NULL", "eg", OB_P(eg));
  } else {
    ObElectionGroup* tmp_eg = NULL;
    if (NULL == (tmp_eg = static_cast<ObElectionGroup*>(eg))) {
      ELECT_ASYNC_LOG(WARN, "static_cast return NULL", "eg", OB_P(eg));
    } else {
      common::ob_slice_free_election_group(tmp_eg);
    }
    (void)ATOMIC_FAA(&release_count_, 1);
  }
}

static bool stop_all_election_group(const ObElectionGroupId& eg_id, ObIElectionGroup* eg)
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;

  if (!eg_id.is_valid() || OB_ISNULL(eg)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(eg_id), KP(eg));
  } else if (OB_SUCCESS != (tmp_ret = eg->stop())) {
    ELECT_ASYNC_LOG(WARN, "election_group stop error", "ret", tmp_ret);
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

int ObElectionGroupMgr::init(const ObAddr& self, ObIElectionRpc* rpc, common::ObTimeWheel* tw,
    ObIElectionMgr* election_mgr, ObIElectionGroupPriorityGetter* eg_cb)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr inited twice", K(ret));
  } else if (!self.is_valid() || OB_ISNULL(rpc) || OB_ISNULL(tw) || OB_ISNULL(election_mgr) || OB_ISNULL(eg_cb)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(self), KP(rpc), KP(tw), KP(election_mgr), KP(eg_cb));
  } else if (OB_FAIL(election_group_map_.init(ObModIds::OB_ELECTION_GROUP_HASH_BUCKET))) {
    ELECT_ASYNC_LOG(WARN, "init hash map failed", K(ret));
  } else if (OB_FAIL(election_group_cache_.init(&election_group_map_, this))) {
    ELECT_ASYNC_LOG(WARN, "init group cache failed", K(ret));
  } else {
    self_ = self;
    rpc_ = rpc;
    tw_ = tw;
    election_mgr_ = election_mgr;
    last_create_time_ = ObTimeUtility::current_time();
    eg_cb_ = eg_cb;
    is_inited_ = true;
    ELECT_ASYNC_LOG(INFO, "ObElectionGroupMgr inited success", K(self));
  }

  return ret;
}

void ObElectionGroupMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  self_.reset();
  election_group_map_.reset();
  election_group_cache_.reset();
  rpc_ = NULL;
  tw_ = NULL;
  election_mgr_ = NULL;
  eg_cb_ = NULL;
  last_create_time_ = 0;
}

void ObElectionGroupMgr::destroy()
{
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    // do nothing
  } else {
    is_inited_ = false;
    if (OB_SUCCESS != (tmp_ret = stop_()) && OB_NOT_RUNNING != tmp_ret) {
      ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr stop error", "ret", tmp_ret);
    }
    election_group_map_.destroy();
    election_group_cache_.destroy();
    ELECT_ASYNC_LOG(INFO, "ObElectionGroupMgr destroy");
  }
}

int ObElectionGroupMgr::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not init");
  } else if (is_running_) {
    ret = OB_ELECTION_GROUP_MGR_IS_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr already running");
  } else {
    is_running_ = true;
  }

  return ret;
}

int ObElectionGroupMgr::stop()
{
  return stop_();
}

int ObElectionGroupMgr::stop_()
{
  int ret = OB_SUCCESS;

  if (!is_running_) {
    // not running, return success
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running");
  } else if (OB_FAIL(election_group_map_.for_each(stop_all_election_group))) {
    ELECT_ASYNC_LOG(WARN, "election group manager stop error", K(ret));
  } else {
    is_running_ = false;
    ELECT_ASYNC_LOG(INFO, "election group manager stop success");
  }

  return ret;
}

bool EgGCFunctor::operator()(const ObElectionGroupId& eg_id, ObIElectionGroup* eg)
{
  bool bool_ret = true;
  int ret = OB_SUCCESS;

  if (!eg->is_pre_destroy_state()) {
    // not in pre_destroy_state, skip
  } else if (OB_FAIL(eg->prepare_destroy())) {
    ELECT_ASYNC_LOG(WARN, "prepare_destroy failed", K(ret), K(eg_id));
  } else if (OB_FAIL(eg->stop())) {
    ELECT_ASYNC_LOG(WARN, "stop election_group failed", K(ret), K(eg_id));
  } else if (OB_FAIL(gc_candidates_.push_back(eg_id))) {
    ELECT_ASYNC_LOG(WARN, "gc_candidates_.push_back failed", K(ret), K(eg_id));
  } else {
    ELECT_ASYNC_LOG(INFO, "collect eg gc candidate success", K(eg_id));
  }

  return bool_ret;
}

int ObElectionGroupMgr::get_create_time_(int64_t& create_time)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    while (OB_SUCCESS == ret) {
      const int64_t old_val = ATOMIC_LOAD(&last_create_time_);
      const int64_t now = ObTimeUtility::current_time();
      const int64_t new_val = (old_val >= now) ? old_val + 1 : now;
      if (ATOMIC_BCAS(&last_create_time_, old_val, new_val)) {
        create_time = new_val;
        break;
      } else {
        PAUSE();
      }
    }
  }

  return ret;
}

int ObElectionGroupMgr::get_election_group_cache(ObElectionGroupCache*& cache)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else {
    cache = &election_group_cache_;
  }

  return ret;
}

int ObElectionGroupMgr::create_new_eg_leader(const uint64_t tenant_id, const common::ObMemberList& member_list,
    const int64_t replica_num, ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  int64_t create_time = -1;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID || !member_list.is_valid() || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument, attention", K(tenant_id), K(member_list), K(replica_num), K(ret));
  } else if (OB_FAIL(get_create_time_(create_time))) {
    ELECT_ASYNC_LOG(WARN, "get_create_time_ failed", K(create_time), K(ret));
  } else if (OB_FAIL(eg_id.init(self_, create_time))) {
    ELECT_ASYNC_LOG(WARN, "eg_id init failed", K(create_time), K(self_), K(ret));
  } else if (OB_FAIL(create_new_eg_leader_(eg_id, tenant_id, member_list, replica_num))) {
    ELECT_ASYNC_LOG(
        WARN, "create_new_eg_leader_ failed", K(tenant_id), K(member_list), K(replica_num), K(eg_id), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObElectionGroupMgr::assign_election_group(const common::ObPartitionKey& pkey, const common::ObAddr& part_leader,
    const int64_t replica_num, const common::ObMemberList& member_list, ObElectionGroupId& ret_eg_id)
{
  // get a related election group, create a new one if there not exists.
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey.get_tenant_id();
  ret_eg_id.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else if (!pkey.is_valid() || !part_leader.is_valid() || replica_num <= 0 || !member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(pkey), K(part_leader), K(replica_num), K(member_list));
  } else if (part_leader != self_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG(WARN,
        "self is not partition leader",
        K(ret),
        K(self_),
        K(pkey),
        K(part_leader),
        K(replica_num),
        K(member_list));
  } else {
    ObElectionGroupKey election_group_key(tenant_id, part_leader, replica_num, member_list);
    ObElectionGroupId eg_id;
    if (OB_FAIL(election_group_cache_.get_or_create_eg_id(election_group_key, eg_id))) {
      ELECT_ASYNC_LOG(ERROR, "can not get eg_id from election_group_cache_, which shouldn't happen", K(ret));
    } else {
      ret_eg_id = eg_id;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        ELECT_ASYNC_LOG(INFO, "finish assign_election_group", K(ret), K(ret_eg_id));
      }
    }
  }

  return ret;
}

int ObElectionGroupMgr::create_new_eg_leader_(const ObElectionGroupId& eg_id, const uint64_t tenant_id,
    const ObMemberList& member_list, const int64_t replica_num)
{
  // create a leader election group
  int ret = OB_SUCCESS;
  ObIElectionGroup* tmp_election_group = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else if (!eg_id.is_valid() || 0 == tenant_id || !member_list.is_valid() || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(eg_id), K(tenant_id), K(member_list), K(replica_num));
  } else if (NULL == (tmp_election_group = ObElectionGroupFactory::alloc_election_group(tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ELECT_ASYNC_LOG(WARN, "alloc_election_group failed", K(ret));
  } else if (OB_FAIL(tmp_election_group->init(eg_id,
                 self_,
                 tenant_id,
                 member_list,
                 replica_num,
                 rpc_,
                 tw_,
                 election_mgr_,
                 &election_group_cache_,
                 eg_cb_,
                 this))) {
    ELECT_ASYNC_LOG(WARN, "election_group init failed", K(ret));
    ObElectionGroupFactory::release_election_group(tmp_election_group);
    tmp_election_group = NULL;
  } else if (OB_FAIL(tmp_election_group->start())) {
    ELECT_ASYNC_LOG(WARN, "election_group start failed", K(ret));
  } else if (OB_FAIL(election_group_map_.insert_and_get(eg_id, tmp_election_group))) {
    ELECT_ASYNC_LOG(WARN, "insert election_group failed", K(ret), K(eg_id));
    ObElectionGroupFactory::release_election_group(tmp_election_group);
    tmp_election_group = NULL;
  } else {
    // need revert after link_hashmap insert success
    const_cast<ElectionGroupMap&>(election_group_map_).revert(tmp_election_group);
  }

  ELECT_ASYNC_LOG(INFO, "finish create_new_eg_leader_", K(ret), K(eg_id), "now eg cnt", election_group_map_.count());
  return ret;
}

int ObElectionGroupMgr::create_new_eg_follower_(const ObElectionGroupId& eg_id, const uint64_t tenant_id)
{
  // create a follower election group
  int ret = OB_SUCCESS;
  ObIElectionGroup* tmp_election_group = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else if (!eg_id.is_valid() || 0 == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(eg_id), K(tenant_id));
  } else if (NULL == (tmp_election_group = ObElectionGroupFactory::alloc_election_group(tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ELECT_ASYNC_LOG(WARN, "alloc_election_group failed", K(ret));
  } else if (OB_FAIL(
                 tmp_election_group->init(eg_id, self_, tenant_id, rpc_, tw_, election_mgr_, nullptr, eg_cb_, this))) {
    ELECT_ASYNC_LOG(WARN, "election_group init failed", K(ret));
    ObElectionGroupFactory::release_election_group(tmp_election_group);
    tmp_election_group = NULL;
  } else if (OB_FAIL(tmp_election_group->start())) {
    ELECT_ASYNC_LOG(WARN, "election_group start failed", K(ret));
  } else if (OB_FAIL(election_group_map_.insert_and_get(eg_id, tmp_election_group))) {
    ELECT_ASYNC_LOG(WARN, "insert election_group failed", K(ret), K(eg_id));
    ObElectionGroupFactory::release_election_group(tmp_election_group);
    tmp_election_group = NULL;
  } else {
    // need revert after link_hashmap insert success
    const_cast<ElectionGroupMap&>(election_group_map_).revert(tmp_election_group);
  }

  ELECT_ASYNC_LOG(INFO, "finish create_new_eg_follower_", K(ret), K(eg_id), "now eg cnt", election_group_map_.count());
  return ret;
}

int ObElectionGroupMgr::construct_empty_election_group(
    const ObElectionGroupId& eg_id, const ObPartitionArray& partition_array)
{
  // create a empty election group according leader message
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else if (!eg_id.is_valid() || partition_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(eg_id), "array size", partition_array.count());
  } else {
    common::ObPartitionKey pkey = partition_array.at(0);
    const uint64_t tenant_id = pkey.get_tenant_id();
    if (self_ == eg_id.get_server()) {
      ret = OB_STATE_NOT_MATCH;
      ELECT_ASYNC_LOG(WARN, "self is leader, cannot exec construct", K(ret), K(pkey));
    } else if (OB_FAIL(create_new_eg_follower_(eg_id, tenant_id))) {
      ELECT_ASYNC_LOG(WARN, "create_new_eg_follower_ failed", K(ret));
    } else {
      ELECT_ASYNC_LOG(INFO, "construct_empty_election_group success", K(ret), K(eg_id));
    }
  }

  return ret;
}

int ObElectionGroupMgr::revert_election_group(ObIElectionGroup* election_group)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not init");
  } else if (NULL == election_group) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(INFO, "invalid argument", K(ret), KP(election_group));
  } else {
    const_cast<ElectionGroupMap&>(election_group_map_).revert(election_group);
  }

  return ret;
}

ObIElectionGroup* ObElectionGroupMgr::get_election_group(const ObElectionGroupId& eg_id) const
{
  // get the election group pointer specified by eg_id
  int ret = OB_SUCCESS;
  ObIElectionGroup* election_group = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not init");
  } else if (!eg_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(eg_id));
  } else if (OB_FAIL(const_cast<ElectionGroupMap&>(election_group_map_).get(eg_id, election_group)) &&
             OB_ENTRY_NOT_EXIST != ret) {
    ELECT_ASYNC_LOG(WARN, "election_group_map_ get eg failed", K(ret), K(eg_id), KP(election_group));
  } else {
    // do nothing
  }

  return election_group;
}

int ObElectionGroupMgr::delete_election_group(const ObElectionGroupId eg_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIElectionGroup* tmp_eg = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret), K(eg_id));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else if (!eg_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(eg_id));
  } else if (NULL == (tmp_eg = get_election_group(eg_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    ELECT_ASYNC_LOG(WARN, "get_election_group return null", K(ret), K(eg_id));
  } else if (OB_FAIL(tmp_eg->prepare_destroy())) {
    ELECT_ASYNC_LOG(WARN, "prepare_destroy failed", K(ret), K(eg_id));
  } else if (!tmp_eg->is_pre_destroy_state()) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG(WARN, "election_group is not in pre_destroy_state", K(ret), K(eg_id));
  } else if (OB_FAIL(tmp_eg->stop())) {
    ELECT_ASYNC_LOG(WARN, "stop election_group failed", K(ret), K(eg_id));
  } else {
    // do nothing
  }
  // revert election_group
  if (NULL != tmp_eg && OB_SUCCESS != (tmp_ret = revert_election_group(tmp_eg))) {
    ELECT_ASYNC_LOG(WARN, "revert_election_group failed", K(tmp_ret), K(eg_id));
  }
  // delete election_group
  if (OB_SUCC(ret)) {
    if (OB_FAIL(election_group_map_.del(eg_id))) {
      ELECT_ASYNC_LOG(WARN, "delete election_group failed", K(ret), K(eg_id));
    }
  }
  ELECT_ASYNC_LOG(INFO, "delete election_group finished", K(ret), K(eg_id), "now eg cnt", election_group_map_.count());

  return ret;
}

bool IterateElectionGroupFunctor::operator()(const ObElectionGroupId& eg_id, ObIElectionGroup* eg)
{
  int tmp_ret = OB_SUCCESS;
  bool bool_ret = true;
  ObElectionGroupInfo eg_info;

  if (!eg_id.is_valid() || OB_ISNULL(eg)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(tmp_ret), K(eg_id), KP(eg));
  } else if (OB_SUCCESS != (tmp_ret = eg->get_election_group_info(eg_info))) {
    ELECT_ASYNC_LOG(WARN, "get_election_group_info failed", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = eg_info_iter_.push(eg_info))) {
    ELECT_ASYNC_LOG(WARN, "eg_info_iter_.push failed", K(tmp_ret));
  } else {
    // do nothing
  }

  return bool_ret;
}

int ObElectionGroupMgr::iterate_election_group_info(ObElectionGroupInfoIterator& eg_info_iter)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "election manager is not running");
    ret = OB_NOT_RUNNING;
  } else {
    IterateElectionGroupFunctor fn(eg_info_iter);
    if (OB_FAIL(foreach_election_group_(fn))) {
      ELECT_ASYNC_LOG(WARN, "for each all election group failed", K(ret));
    }
  }

  return ret;
}

int ObElectionGroupMgr::exec_gc_loop()
{
  // try gc predestory eg
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionGroupMgr not running", K(ret));
  } else {
    EgGCFunctor::EgIdArray gc_candidates;
    EgGCFunctor fn(gc_candidates);
    if (OB_FAIL(foreach_election_group_(fn))) {
      ELECT_ASYNC_LOG(WARN, "for each all election error", K(ret));
    } else if (gc_candidates.count() > 0) {
      const int64_t count = gc_candidates.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const ObElectionGroupId& eg_id = gc_candidates.at(i);
        if (OB_FAIL(election_group_map_.del(eg_id))) {
          ELECT_ASYNC_LOG(WARN, "election_group_map_.del failed", K(ret), K(eg_id));
        } else {
          ELECT_ASYNC_LOG(INFO, "delete election_group success", K(eg_id));
        }
      }
    }

    // gc invalid queue in ObElectionGroupCache
    election_group_cache_.gc_invalid_queue();
    int64_t election_remove_count = ATOMIC_LOAD(&oceanbase::election::ObElectionMgr::TOTAL_REMOVE_COUNT);
    int64_t election_add_count = ATOMIC_LOAD(&oceanbase::election::ObElectionMgr::TOTAL_ADD_COUNT);
    int64_t election_release_count = ATOMIC_LOAD(&oceanbase::election::ElectionAllocHandle::TOTAL_RELEASE_COUNT);
    ELECT_ASYNC_LOG(INFO,
        "report election number summary",
        K(election_add_count),
        K(election_remove_count),
        K(election_release_count));
  }

  return ret;
}

}  // namespace election
}  // namespace oceanbase
