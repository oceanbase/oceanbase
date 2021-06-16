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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_CACHE_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_CACHE_

#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/queue/ob_link_queue.h"
#include "ob_election_group_id.h"

namespace oceanbase {
namespace election {

class ObLinkHashMap;
class ObIElectionGroup;
class ObElectionGroupMgr;
class ElectionGroupAllocHandle;

typedef common::RefHandle ElectionGroupRefHandle;
typedef common::ObLinkHashMap<ObElectionGroupId, ObIElectionGroup, ElectionGroupAllocHandle, ElectionGroupRefHandle>
    ElectionGroupMap;

// ElectionGroupKey specified by [tenant_id,part_leader,replica_num,member_list]
class ObElectionGroupKey {
public:
  ObElectionGroupKey() : tenant_id_(common::OB_INVALID_TENANT_ID), replica_num_(0), hash_value_(0)
  {}
  ObElectionGroupKey(const uint64_t tenant_id, const common::ObAddr& part_leader, const int64_t replica_num,
      const common::ObMemberList& member_list);
  explicit ObElectionGroupKey(const ObElectionGroupKey&) = default;
  ~ObElectionGroupKey() = default;
  ObElectionGroupKey& operator=(const ObElectionGroupKey&) = default;
  bool operator==(const ObElectionGroupKey& other) const;
  uint64_t hash() const
  {
    return hash_value_;
  }
  bool is_valid() const;
  TO_STRING_KV(K(tenant_id_), K(part_leader_), K(replica_num_), K(member_list_));
  const uint64_t& get_tenant_id() const
  {
    return tenant_id_;
  }
  const common::ObAddr& get_part_leader() const
  {
    return part_leader_;
  }
  const int64_t& get_replica_num() const
  {
    return replica_num_;
  }
  const common::ObMemberList& get_member_list() const
  {
    return member_list_;
  }

private:
  uint64_t tenant_id_;
  common::ObAddr part_leader_;
  int64_t replica_num_;
  common::ObMemberList member_list_;
  uint64_t hash_value_;
};

class ObElectionGroupCache {
public:
  ObElectionGroupCache() : is_inited_(false), election_group_map_(nullptr), election_group_mgr_(nullptr)
  {}
  ObElectionGroupCache(const ObElectionGroupCache&) = delete;
  ObElectionGroupCache& operator=(const ObElectionGroupCache&) = delete;
  ~ObElectionGroupCache() = default;

public:
  int init(ElectionGroupMap* election_group_map, ObElectionGroupMgr* election_group_mgr);
  void reset();
  void destroy();
  int put_eg_id(const ObElectionGroupKey& key, const ObElectionGroupId& eg_id);
  int get_or_create_eg_id(const ObElectionGroupKey& key, ObElectionGroupId& eg_id);
  void gc_invalid_queue();
  int64_t get_group_num();  // for now, just for unit test
  int64_t get_queue_num();  // for now, just for unit test
  int64_t get_node_num();   // for now, just for unit test

private:
  class LinkedElectionGroupId : public common::ObSpLinkQueue::Link {
  public:
    explicit LinkedElectionGroupId(const ObElectionGroupId& id) : id_(id)
    {}
    LinkedElectionGroupId& operator=(const LinkedElectionGroupId&) = default;
    ~LinkedElectionGroupId() = default;
    const ObElectionGroupId& get_eg_id() const
    {
      return id_;
    }

  private:
    ObElectionGroupId id_;
  };
  class ElementFactory {
  public:
    ElementFactory() : queue_count_(0), egid_count_(0){};
    ~ElementFactory() = default;
    ElementFactory& operator=(const ElementFactory& rhs) = delete;

  public:
    int create_queue(common::ObSpLinkQueue*& queue);
    int create_egid_node(const ObElectionGroupId& id, LinkedElectionGroupId*& node);
    void release_queue(common::ObSpLinkQueue* queue);
    void release_egid_node(LinkedElectionGroupId* node);
    int64_t get_queue_count() const;
    int64_t get_egid_count() const;

  private:
    int64_t queue_count_;
    int64_t egid_count_;
  };
  class PutOperation {
  public:
    PutOperation(const ObElectionGroupId& eg_id, ElementFactory& factory)
        : operation_ret_(common::OB_ERR_UNEXPECTED), eg_id_(eg_id), factory_(factory){};
    ~PutOperation() = default;
    PutOperation& operator=(const PutOperation&) = delete;
    bool operator()(const ObElectionGroupKey& key, common::ObSpLinkQueue*& value);
    int get_operation_ret()
    {
      return operation_ret_;
    }

  private:
    int operation_ret_;
    const ObElectionGroupId& eg_id_;
    ElementFactory& factory_;
  };
  class GetOrCreateOperation {
  public:
    GetOrCreateOperation(ObElectionGroupId& eg_id, ElectionGroupMap* election_group_map,
        ObElectionGroupMgr* election_group_mgr, ElementFactory& factory)
        : operation_ret_(common::OB_ERR_UNEXPECTED),
          eg_id_(eg_id),
          election_group_map_(election_group_map),
          election_group_mgr_(election_group_mgr),
          factory_(factory){};
    ~GetOrCreateOperation() = default;
    GetOrCreateOperation& operator=(const GetOrCreateOperation&) = delete;
    bool operator()(const ObElectionGroupKey& key, common::ObSpLinkQueue*& value);
    int get_operation_ret()
    {
      return operation_ret_;
    }

  private:
    int operation_ret_;
    ObElectionGroupId& eg_id_;
    ElectionGroupMap* election_group_map_;
    ObElectionGroupMgr* election_group_mgr_;
    ElementFactory& factory_;
  };
  class ClearOperation {
  public:
    explicit ClearOperation(ElementFactory& factory) : factory_(factory){};
    ClearOperation(const ClearOperation&) = delete;
    ClearOperation& operator=(const ClearOperation&) = delete;
    ~ClearOperation() = default;
    bool operator()(const ObElectionGroupKey& key, common::ObSpLinkQueue*& value);

  private:
    ElementFactory& factory_;
  };
  class TryGCNodeOperation {
  public:
    TryGCNodeOperation(ElectionGroupMap* election_group_map, ElementFactory& factory)
        : election_group_map_(election_group_map), factory_(factory){};
    TryGCNodeOperation(const TryGCNodeOperation&) = delete;
    TryGCNodeOperation& operator=(const TryGCNodeOperation&) = delete;
    ~TryGCNodeOperation() = default;
    bool operator()(const ObElectionGroupKey& key, common::ObSpLinkQueue*& value);

  private:
    ElectionGroupMap* election_group_map_;
    ElementFactory& factory_;
  };
  static int check_eg_id_valid_(
      const ObElectionGroupId& eg_id, const ObElectionGroupKey& key, ElectionGroupMap* election_group_map);
  template <typename Function>
  int keep_trying_operation_(const ObElectionGroupKey& key, Function& operation);
  common::ObLinearHashMap<ObElectionGroupKey, common::ObSpLinkQueue*> election_group_id_cache_map_;
  bool is_inited_;
  ElectionGroupMap* election_group_map_;
  ObElectionGroupMgr* election_group_mgr_;
  ElementFactory factory_;
};

}  // namespace election
}  // namespace oceanbase

#endif