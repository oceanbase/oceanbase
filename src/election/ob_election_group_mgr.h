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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_MGR_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_MGR_

#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/ob_define.h"
#include "common/ob_simple_iterator.h"
#include "common/ob_member_list.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/transaction/ob_time_wheel.h"
#include "ob_election_base.h"
#include "ob_election_group_id.h"
#include "ob_election_group.h"
#include "ob_election_group_info.h"
#include "ob_election_group_cache.h"

namespace oceanbase {
namespace election {
class ObIElectionMgr;

typedef common::ObSimpleIterator<ObElectionGroupInfo, common::ObModIds::OB_ELECTION_VIRTAUL_TABLE_ELECTION_INFO, 16>
    ObElectionGroupInfoIterator;

class ObElectionGroupFactory {
public:
  static ObIElectionGroup* alloc_election_group(const uint64_t tenant_id);
  static void release_election_group(ObIElectionGroup* eg);
  static int64_t alloc_count_;
  static int64_t release_count_;
};

class ElectionGroupAllocHandle {
public:
  typedef common::LinkHashNode<ObElectionGroupId> Node;

  static ObIElectionGroup* alloc_value()
  {
    // do not allow alloc val in hashmap
    return NULL;
  }
  static void free_value(ObIElectionGroup* e)
  {
    ObElectionGroupFactory::release_election_group(e);
    e = NULL;
  }
  static Node* alloc_node(ObIElectionGroup* e)
  {
    UNUSED(e);
    return op_alloc(Node);
  }
  static void free_node(Node* node)
  {
    op_free(node);
    node = NULL;
  }
};

class ObIElectionGroupMgr {
public:
  ObIElectionGroupMgr()
  {}
  virtual ~ObIElectionGroupMgr()
  {}
  virtual int init(const common::ObAddr& self, ObIElectionRpc* rpc, common::ObTimeWheel* tw,
      ObIElectionMgr* election_mgr, ObIElectionGroupPriorityGetter* eg_cb) = 0;
  virtual void reset() = 0;
  virtual void destroy() = 0;
  virtual int start() = 0;
  virtual int stop() = 0;

public:
  virtual ObIElectionGroup* get_election_group(const ObElectionGroupId& eg_id) const = 0;
  virtual int revert_election_group(ObIElectionGroup* election_group) = 0;
  virtual int assign_election_group(const common::ObPartitionKey& pkey, const common::ObAddr& part_leader,
      const int64_t replica_num, const common::ObMemberList& member_list, ObElectionGroupId& eg_id) = 0;
  virtual int construct_empty_election_group(
      const ObElectionGroupId& eg_id, const common::ObPartitionArray& partition_array) = 0;
  virtual int delete_election_group(const ObElectionGroupId eg_id) = 0;
  virtual int iterate_election_group_info(ObElectionGroupInfoIterator& eg_info_iter) = 0;
};

class ObElectionGroupMgr : public ObIElectionGroupMgr {
public:
  ObElectionGroupMgr()
  {
    reset();
  }
  virtual ~ObElectionGroupMgr()
  {
    destroy();
  }
  int init(const common::ObAddr& self, ObIElectionRpc* rpc, common::ObTimeWheel* tw, ObIElectionMgr* election_mgr,
      ObIElectionGroupPriorityGetter* eg_cb);
  void reset();
  void destroy();
  int start();
  int stop();

public:
  ObIElectionGroup* get_election_group(const ObElectionGroupId& eg_id) const;
  int get_election_group_cache(ObElectionGroupCache*& cache);  // for now, just for unit test
  int revert_election_group(ObIElectionGroup* election_group);
  int create_new_eg_leader(const uint64_t tenant_id, const common::ObMemberList& member_list, const int64_t replica_num,
      ObElectionGroupId& eg_id);
  int assign_election_group(const common::ObPartitionKey& pkey, const common::ObAddr& part_leader,
      const int64_t replica_num, const common::ObMemberList& member_list, ObElectionGroupId& eg_id);
  int construct_empty_election_group(const ObElectionGroupId& eg_id, const common::ObPartitionArray& partition_array);
  int delete_election_group(const ObElectionGroupId eg_id);
  int iterate_election_group_info(ObElectionGroupInfoIterator& eg_info_iter);
  int exec_gc_loop();

private:
  int stop_();
  int create_new_eg_leader_(const ObElectionGroupId& eg_id, const uint64_t tenant_id,
      const common::ObMemberList& member_list, const int64_t replica_num);
  int create_new_eg_follower_(const ObElectionGroupId& eg_id, const uint64_t tenant_id);
  int get_create_time_(int64_t& create_time);

private:
  typedef common::DRWLock::WRLockGuard WRLockGuard;
  typedef common::DRWLock::RDLockGuard RDLockGuard;
  template <typename Fn>
  int foreach_election_group_(Fn& fn)
  {
    return election_group_map_.for_each(fn);
  }

private:
  bool is_inited_;
  bool is_running_;
  common::ObAddr self_;
  ElectionGroupMap election_group_map_;
  ObElectionGroupCache election_group_cache_;
  ObIElectionRpc* rpc_;
  common::ObTimeWheel* tw_;
  ObIElectionMgr* election_mgr_;
  ObIElectionGroupPriorityGetter* eg_cb_;
  int64_t last_create_time_;
};

class IterateElectionGroupFunctor {
public:
  explicit IterateElectionGroupFunctor(ObElectionGroupInfoIterator& eg_info_iter) : eg_info_iter_(eg_info_iter)
  {}
  bool operator()(const ObElectionGroupId& eg_id, ObIElectionGroup* eg);

private:
  ObElectionGroupInfoIterator& eg_info_iter_;
};

class EgGCFunctor {
public:
  typedef common::ObSEArray<election::ObElectionGroupId, 64> EgIdArray;
  explicit EgGCFunctor(EgIdArray& gc_candidates) : gc_candidates_(gc_candidates)
  {}
  bool operator()(const ObElectionGroupId& eg_id, ObIElectionGroup* eg);

private:
  EgIdArray& gc_candidates_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_GROUP_MGR_
