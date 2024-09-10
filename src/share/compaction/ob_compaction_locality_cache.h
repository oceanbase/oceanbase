//Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_SHARE_OB_COMPACTION_LOCALITY_CACHE_H_
#define OCEANBASE_SHARE_OB_COMPACTION_LOCALITY_CACHE_H_

#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_table_operator.h"
#include "deps/oblib/src/lib/net/ob_addr.h"
#include "deps/oblib/src/common/ob_zone.h"
#include "rootserver/freeze/ob_major_merge_info_manager.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{

struct ObLSReplicaUniItem
{
  ObLSReplicaUniItem();
  ObLSReplicaUniItem(const ObLSID &ls_id, const common::ObAddr &server);
  ~ObLSReplicaUniItem();
  void reset();
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool is_valid() const;
  bool operator == (const ObLSReplicaUniItem &other) const;
  bool operator != (const ObLSReplicaUniItem &other) const;
  TO_STRING_KV(K_(ls_id), K_(server));

  share::ObLSID ls_id_;
  common::ObAddr server_;
};

class ObLSColumnReplicaCache
{
public:
  ObLSColumnReplicaCache();
  ~ObLSColumnReplicaCache();
  int init();
  void destroy();
  void reuse();
  int assign(const ObLSColumnReplicaCache &other);
  int deep_fetch(
      hash::ObHashSet<ObLSID> &target_ls_id_set,
      hash::ObHashSet<ObLSReplicaUniItem> &target_ls_replica_set,
      common::ObIArray<ObLSInfo> &target_ls_infos) const;
  int update(const ObLSID &ls_id);
  int update_with_ls_info(const ObLSInfo &ls_info);
  int check_is_cs_replica(const ObLSReplicaUniItem &ls_item, bool &is_cs_replica) const;
  int check_can_skip(const ObLSReplicaUniItem &ls_item, bool &can_skip) const;
  TO_STRING_KV(K_(is_inited), K_(ls_id_set), K_(ls_replica_set), K_(ls_infos));
private:
  int check_contains_ls(const ObLSID &ls_id, bool &contained) const;
  int mark_ls_finished(const ObLSID &ls_id);
  int add_cs_replica(const ObLSReplicaUniItem &ls_item);
private:
  const static int64_t BUCKET_NUM_OF_LS_ID_SET = 15;
  const static int64_t BUCKET_NUM_OF_LS_REPLICA_SET = 31;
private:
  bool is_inited_;
  hash::ObHashSet<ObLSID> ls_id_set_; // record looped ls id
  hash::ObHashSet<ObLSReplicaUniItem> ls_replica_set_; // cs-prelica ls
  common::ObSEArray<ObLSInfo, 4> ls_infos_; // used for check member list and learner list
};

class ObCompactionLocalityCache
{
public:
  ObCompactionLocalityCache();
  ~ObCompactionLocalityCache();
  int init(const uint64_t tenant_id, rootserver::ObMajorMergeInfoManager *merge_info_mgr = nullptr);
  void destroy();
  bool empty() const { return ls_infos_map_.empty(); }
  int refresh_ls_locality(const bool force_refresh);
  int get_ls_info(const share::ObLSID &ls_id, share::ObLSInfo &ls_info);
  const share::ObLSColumnReplicaCache& get_cs_replica_cache() const { return ls_cs_replica_cache_; }
  TO_STRING_KV(K_(is_inited), K_(tenant_id));
private:
  const int64_t CHECK_LS_LOCALITY_INTERVAL = 5 * 60 * 1000 * 1000L; // 5 mins
  int get_zone_list_from_inner_table(ObIArray<common::ObZone> &zone_list);
  int str2zone_list(
      const char *str,
      ObIArray<common::ObZone> &zone_list);
  int refresh_by_zone(
      const share::ObLSInfo &ls_info,
      const ObIArray<common::ObZone> &zone_list);
  bool replica_in_zone_list(
      const ObLSReplica &ls_replica,
      const ObIArray<common::ObZone> &zone_list);
  int inner_refresh_ls_locality();
  int build_member_list_set(
    const ObLSReplica &tmp_replica,
    ObIArray<common::ObAddr> &member_list_set);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  rootserver::ObMajorMergeInfoManager *merge_info_mgr_;
  common::hash::ObHashMap<share::ObLSID, share::ObLSInfo> ls_infos_map_;
  share::ObLSColumnReplicaCache ls_cs_replica_cache_;
};

struct ObMemberListInfo
{
  ObMemberListInfo(const uint64_t tenant_id)
    : member_list_array_()
  {
    member_list_array_.set_attr(ObMemAttr(tenant_id, "MemListInfo"));
  }
  ~ObMemberListInfo() {}
  int build(const ObLSReplica &tmp_replica);
  bool check_exist(const common::ObAddr &addr);
  bool empty() { return member_list_array_.empty(); }
  TO_STRING_KV("member_list_cnt", member_list_array_.count(), K(member_list_array_));
  ObSEArray<common::ObAddr, 6> member_list_array_; // including member_list & learner_list
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_COMPACTION_LOCALITY_CACHE_H_
