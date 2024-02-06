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

#define USING_LOG_PREFIX SQL_OPT

#include "ob_phy_table_location_info.h"
#include "observer/ob_server_struct.h"
#include "sql/das/ob_das_location_router.h"
#include "storage/tx/wrs/ob_black_list.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
namespace oceanbase
{
namespace sql
{

ObOptTabletLoc::ObOptTabletLoc()
    : partition_id_(OB_INVALID_INDEX),
      first_level_part_id_(OB_INVALID_INDEX),
      replica_locations_("SqlOptimLocaCac", OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObOptTabletLoc::~ObOptTabletLoc()
{
}

void ObOptTabletLoc::reset()
{
  partition_id_ = OB_INVALID_INDEX;
  first_level_part_id_ = OB_INVALID_INDEX;
  tablet_id_.reset();
  ls_id_.reset();
  replica_locations_.reset();
}

int ObOptTabletLoc::assign(const ObOptTabletLoc &other)
{
  int ret = OB_SUCCESS;
  tablet_id_ = other.tablet_id_;
  ls_id_ = other.ls_id_;
  partition_id_ = other.partition_id_;
  first_level_part_id_ = other.first_level_part_id_;
  if (OB_FAIL(replica_locations_.assign(other.replica_locations_))) {
    LOG_WARN("Failed to assign replica locations", K(ret));
  }
  return ret;
}

int ObOptTabletLoc::assign_with_only_readable_replica(const ObObjectID &partition_id,
                                                      const ObObjectID &first_level_part_id,
                                                      const common::ObTabletID &tablet_id,
                                                      const ObLSLocation &ls_location)
{
  int ret = OB_SUCCESS;
  reset();
  partition_id_ = partition_id;
  first_level_part_id_ = first_level_part_id;
  tablet_id_ = tablet_id;
  ls_id_ = ls_location.get_ls_id();
  int64_t leader_replica_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_location.get_replica_locations().count(); ++i) {
    const ObLSReplicaLocation &replica_loc = ls_location.get_replica_locations().at(i);
    if (replica_loc.is_strong_leader()) {
      leader_replica_idx = i;
    }
    if (ObReplicaTypeCheck::is_readable_replica(replica_loc.get_replica_type())) {
      transaction::ObBLKey bl_key;
      bool in_black_list = false;
      if (OB_FAIL(bl_key.init(replica_loc.get_server(), ls_location.get_tenant_id(), ls_location.get_ls_id()))) {
        LOG_WARN("init black list key failed", K(ret));
      } else if (OB_FAIL(ObBLService::get_instance().check_in_black_list(bl_key, in_black_list))) {
        LOG_WARN("check in black list failed", K(ret));
      } else if (!in_black_list) {
        if (OB_FAIL(replica_locations_.push_back(replica_loc))) {
          LOG_WARN("Failed to push back replica locations",
                   K(ret), K(i), K(replica_loc), K(replica_locations_));
        }
      } else {
        LOG_INFO("the replica location is invalid", K(bl_key), K(replica_loc));
      }
    }
  }

  // all replicas are in blacklist, add leader replica forcibly.
  if (OB_SUCC(ret) && 0 == replica_locations_.count()) {
    if (OB_INVALID_INDEX == leader_replica_idx) {
      LOG_INFO("there is no leader replica");
    } else if (OB_FAIL(replica_locations_.push_back(ls_location.get_replica_locations().at(leader_replica_idx)))) {
      LOG_WARN("failed to push back leader replica", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == replica_locations_.count())) {
      ret = OB_NO_READABLE_REPLICA;
      LOG_WARN("there has no readable replica", K(ret), K(ls_location.get_replica_locations()));
    }
  }
  return ret;
}

bool ObOptTabletLoc::is_valid() const
{
  //为了兼容性考虑，1.4.x和2.1升级到2.2之后，pg_key可能是无效的，因此此处不检查pg_key_
  return OB_INVALID_INDEX != partition_id_;
}

int ObOptTabletLoc::get_strong_leader(ObLSReplicaLocation &replica_location, int64_t &replica_idx) const
{
  int ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
      if (replica_locations_.at(i).is_strong_leader()) {
        replica_location = replica_locations_.at(i);
        replica_idx = i;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObOptTabletLoc::get_strong_leader(ObLSReplicaLocation &replica_location) const
{
  int64_t replica_idx = OB_INVALID_INDEX;
  return get_strong_leader(replica_location, replica_idx);
}

ObCandiTabletLoc::ObCandiTabletLoc()
  : opt_tablet_loc_(),
    selected_replica_idx_(OB_INVALID_INDEX),
    priority_replica_idxs_()
{
}

ObCandiTabletLoc::~ObCandiTabletLoc()
{
}

void ObCandiTabletLoc::reset()
{
  opt_tablet_loc_.reset();
  selected_replica_idx_ = OB_INVALID_INDEX;
  priority_replica_idxs_.reset();
}

int ObCandiTabletLoc::assign(const ObCandiTabletLoc &other)
{
  int ret = OB_SUCCESS;
  /*
  if (OB_UNLIKELY(other.selected_replica_idx_ < 0
                  || other.selected_replica_idx_ >=
                  other.opt_tablet_loc_.get_replica_locations().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("other select replica idx is invalid", K(ret), K(other.selected_replica_idx_),
              K(other.opt_tablet_loc_.get_replica_locations().count()),
              K(other.opt_tablet_loc_));
  } else
  */

  if (OB_FAIL(opt_tablet_loc_.assign(other.opt_tablet_loc_))) {
    LOG_WARN("fail to assign other opt_tablet_loc_", K(ret), K(other.opt_tablet_loc_));
  } else if (OB_FAIL(priority_replica_idxs_.assign(other.priority_replica_idxs_))) {
    LOG_WARN("fail to assign replica idxs", K(ret), K(other.priority_replica_idxs_));
  } else {
    selected_replica_idx_ = other.selected_replica_idx_;
  }
  return ret;
}

int ObCandiTabletLoc::set_selected_replica_idx(int64_t selected_replica_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(selected_replica_idx < 0
                  || selected_replica_idx >=
                  opt_tablet_loc_.get_replica_locations().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("select replica idx is invalid", K(ret), K(selected_replica_idx),
              K(opt_tablet_loc_.get_replica_locations().count()), K(opt_tablet_loc_));
  } else {
    selected_replica_idx_ = selected_replica_idx;
  }
  return ret;
}

int ObCandiTabletLoc::set_selected_replica_idx_with_priority()
{
  int ret = OB_SUCCESS;
  ObRoutePolicy::CandidateReplica selected_replica;
  ObRoutePolicy::CandidateReplica cur_replica;
  int64_t selected_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(priority_replica_idxs_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid priority replica count", K(priority_replica_idxs_), K(ret));
  } else if (priority_replica_idxs_.count() == 1) {
    selected_idx = priority_replica_idxs_.at(0);
    if (OB_FAIL(set_selected_replica_idx(selected_idx))) {
      LOG_WARN("fail to set selected replica idx", K(priority_replica_idxs_), K(ret));
    }
  } else if (priority_replica_idxs_.count() > 1) {
    //多个priority时，本地优先; 多个replica的priority相同则随机选
    ObSEArray<int64_t, 16> same_priority_ids;
    bool is_first = true;
    selected_replica.attr_.pos_type_ = ObRoutePolicy::POSITION_TYPE_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < priority_replica_idxs_.count(); ++i) {
      int64_t cur_priority_idx = priority_replica_idxs_.at(i);
      cur_replica.reset();
      if (OB_FAIL(get_priority_replica(cur_priority_idx, cur_replica))) {
        LOG_WARN("fail to get priority replca", K(cur_priority_idx), K(ret));
      } else if (is_first) {
        is_first = false;
        selected_replica = cur_replica;
        if (OB_FAIL(same_priority_ids.push_back(cur_priority_idx))) {
          LOG_WARN("fail to push back id", K(same_priority_ids), K(cur_priority_idx), K(ret));
        }
      } else if (cur_replica.attr_.pos_type_ == selected_replica.attr_.pos_type_) {
        if (OB_FAIL(same_priority_ids.push_back(cur_priority_idx))) {
          LOG_WARN("fail to push back id", K(same_priority_ids), K(cur_priority_idx), K(ret));
        }
      } else if (cur_replica.attr_.pos_type_ < selected_replica.attr_.pos_type_) {
        selected_replica = cur_replica;
        same_priority_ids.reset();
        if (OB_FAIL(same_priority_ids.push_back(cur_priority_idx))) {
          LOG_WARN("fail to push back id", K(same_priority_ids), K(cur_priority_idx), K(ret));
        }
      } else {
        /*cur_replica.attr_.pos_type_ < selected_replica.attr_.pos_type_ do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      int64_t rand_idx = rand() % same_priority_ids.count();
      selected_idx = same_priority_ids.at(rand_idx);
      if (OB_FAIL(set_selected_replica_idx(selected_idx))) {
        LOG_WARN("fail to set selected replica idx", K(priority_replica_idxs_), K(ret));
      }
    }
  } else {/*do nothing*/}

  return ret;
}

int ObCandiTabletLoc::add_priority_replica_idx(int64_t priority_replica_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(priority_replica_idxs_.push_back(priority_replica_idx))) {
    LOG_WARN("fail to push back priority replica idx", K(priority_replica_idx), K(ret));
  }
  return ret;
}

int ObCandiTabletLoc::get_priority_replica(int64_t selected_replica_idx,
                                           share::ObLSReplicaLocation &replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica_base(selected_replica_idx, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(ret));
  }
  return ret;
}

int ObCandiTabletLoc::get_priority_replica(int64_t selected_replica_idx,
                                           ObRoutePolicy::CandidateReplica &replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica_base(selected_replica_idx, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(ret));
  }
  return ret;
}

template<class T>
int ObCandiTabletLoc::get_priority_replica_base(int64_t selected_replica_idx, T &replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_INDEX == selected_replica_idx)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCandiTabletLoc is not inited", K(ret), K(opt_tablet_loc_));
  } else {
    const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list = opt_tablet_loc_.get_replica_locations();
    if (OB_UNLIKELY(selected_replica_idx < 0 || selected_replica_idx >= replica_loc_list.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("select replica idx is invalid", K(ret),
                K(selected_replica_idx), K(replica_loc_list.count()));
    } else {
      replica_loc = replica_loc_list.at(selected_replica_idx);
    }
  }
  return ret;
}

bool ObCandiTabletLoc::is_server_in_replica(const ObAddr &server,
                                                      int64_t &idx) const
{
  int ret = OB_SUCCESS;
  bool found_flag = false;
  ObRoutePolicy::CandidateReplica tmp_replica;
  idx = 0;
  for (int64_t replica_idx = 0;
       !found_flag && OB_SUCC(ret) && replica_idx < get_partition_location().get_replica_locations().count();
       ++replica_idx) {
    tmp_replica.reset();
    if (OB_FAIL(get_priority_replica(replica_idx, tmp_replica))) {
      LOG_WARN("fail to get priority replica", K(replica_idx), K(ret));
    } else if (tmp_replica.get_server() == server) {
      idx = replica_idx;
      found_flag = true;
    }
  }
  return found_flag;
}

int ObCandiTabletLoc::get_selected_replica(share::ObLSReplicaLocation &replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica(selected_replica_idx_, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(selected_replica_idx_), K(ret));
  }
  return ret;
}

int ObCandiTabletLoc::get_selected_replica(ObRoutePolicy::CandidateReplica &replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica(selected_replica_idx_, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(selected_replica_idx_), K(ret));
  }
  return ret;
}

int ObCandiTabletLoc::set_part_loc_with_only_readable_replica(const ObObjectID &partition_id,
                                                              const ObObjectID &first_level_part_id,
                                                              const common::ObTabletID &tablet_id,
                                                              const ObLSLocation &partition_location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(has_selected_replica())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition location has not been set yet, but replica idx has been selected",
              K(ret), K(*this), K(partition_location));
  } else if (OB_FAIL(opt_tablet_loc_.assign_with_only_readable_replica(partition_id,
                                                                       first_level_part_id,
                                                                       tablet_id,
                                                                       partition_location))) {
    LOG_WARN("fail to assign partition location with only readable replica",
             K(ret), K(partition_location));
  }
  return ret;
}

ObCandiTableLoc::ObCandiTableLoc()
  : table_location_key_(OB_INVALID_ID),
    ref_table_id_(OB_INVALID_ID),
    candi_tablet_locs_(),
    duplicate_type_(ObDuplicateType::NOT_DUPLICATE)
{
}

ObCandiTableLoc::~ObCandiTableLoc()
{
}

void ObCandiTableLoc::reset()
{
  table_location_key_ = OB_INVALID_ID;
  ref_table_id_ = OB_INVALID_ID;
  candi_tablet_locs_.reset();
  duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
}

int ObCandiTableLoc::assign(const ObCandiTableLoc &other)
{
  int ret = OB_SUCCESS;
  table_location_key_ = other.table_location_key_;
  ref_table_id_ = other.ref_table_id_;
  duplicate_type_ = other.duplicate_type_;
  if (OB_FAIL(candi_tablet_locs_.assign(other.candi_tablet_locs_))) {
    LOG_WARN("Failed to assign phy_part_loc_info_list", K(ret));
  }
  return ret;
}

//此前已经判断是复制表, 不是session重试 & 复制表不在DML修改对象之中, 优先选择本地的副本, 如果没有本地副本则选择leader副本
int ObCandiTableLoc::all_select_local_replica_or_leader(bool &is_on_same_server,
                                                              ObAddr &same_server,
                                                              const ObAddr &local_server)
{
  int ret = OB_SUCCESS;
  is_on_same_server = true;
  ObAddr first_server;
  ObLSReplicaLocation replica_location;
  int64_t replica_idx = OB_INVALID_INDEX;
  int64_t local_replica_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs_.count(); ++i) {
    replica_location.reset();
    replica_idx = OB_INVALID_INDEX;
    local_replica_idx = OB_INVALID_INDEX;
    ObCandiTabletLoc &phy_part_loc_info = candi_tablet_locs_.at(i);
    if (OB_FAIL(phy_part_loc_info.get_partition_location().get_strong_leader(replica_location, replica_idx))) {
      LOG_WARN("fail to get leader", K(ret), K(phy_part_loc_info.get_partition_location()));
    } else {
      if (phy_part_loc_info.is_server_in_replica(local_server, local_replica_idx)) {
        if (replica_idx != local_replica_idx) {
          LOG_TRACE("about to choose local replica rather than leader replica for duplicate table", K(replica_idx), K(local_replica_idx));
          replica_idx = local_replica_idx;
        }
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing ...
    } else if (phy_part_loc_info.has_selected_replica()
        && phy_part_loc_info.get_selected_replica_idx() != replica_idx) {
      // FIXME qianfu 子查询的weak属性和主查询的不一样的时候有可能会报这个错，后面会修掉
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("selected replica is not leader", K(ret), K(phy_part_loc_info));
    } else if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(replica_idx))) {
      LOG_WARN("fail to set selected replica idx", K(ret), K(replica_idx), K(phy_part_loc_info));
    } else {
      if (0 == i) {
        first_server = replica_location.get_server();
      } else if (first_server != replica_location.get_server()) {
        is_on_same_server = false;
      }
    }
  }
  if (OB_SUCC(ret) && is_on_same_server) {
    same_server = first_server;
  }
  return ret;
}

int ObCandiTableLoc::all_select_leader(bool &is_on_same_server,
                                              ObAddr &same_server)
{
  int ret = OB_SUCCESS;
  is_on_same_server = true;
  ObAddr first_server;
  ObLSReplicaLocation replica_location;
  int64_t replica_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs_.count(); ++i) {
    replica_location.reset();
    replica_idx = OB_INVALID_INDEX;
    ObCandiTabletLoc &phy_part_loc_info = candi_tablet_locs_.at(i);
    if (OB_FAIL(phy_part_loc_info.get_partition_location().get_strong_leader(replica_location, replica_idx))) {
      LOG_WARN("fail to get leader", K(ret), K(phy_part_loc_info.get_partition_location()));
    } else if (phy_part_loc_info.has_selected_replica()
               && phy_part_loc_info.get_selected_replica_idx() != replica_idx) {
      // FIXME qianfu 子查询的weak属性和主查询的不一样的时候有可能会报这个错，后面会修掉
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("selected replica is not leader", K(ret), K(phy_part_loc_info));
    } else if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(replica_idx))) {
      LOG_WARN("fail to set selected replica idx", K(ret), K(replica_idx), K(phy_part_loc_info));
    } else {
      if (0 == i) {
        first_server = replica_location.get_server();
      } else if (first_server != replica_location.get_server()) {
        is_on_same_server = false;
      }
    }
  }
  if (OB_SUCC(ret) && is_on_same_server) {
    same_server = first_server;
  }
  return ret;
}

// 目前只用于mv，若要用于其他地方，注意下面的OB_NO_READABLE_REPLICA这个返回码，
// 看看所用的地方合不合适，因为它会涉及到重试。
int ObCandiTableLoc::all_select_fixed_server(const ObAddr &fixed_server)
{
  int ret = OB_SUCCESS;
  ObLSReplicaLocation tmp_replica;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs_.count(); ++i) {
    ObCandiTabletLoc &phy_part_loc_info = candi_tablet_locs_.at(i);
    if (phy_part_loc_info.has_selected_replica()) {
      tmp_replica.reset();
      if (OB_FAIL(phy_part_loc_info.get_selected_replica(tmp_replica))) {
        LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
      } else if (fixed_server != tmp_replica.get_server()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("selected replica is not equal fixed server",
                  K(ret), K(phy_part_loc_info), K(fixed_server));
      }
    } else {
      const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list =
          phy_part_loc_info.get_partition_location().get_replica_locations();
      bool has_selected = false;
      for (int64_t j = 0; OB_SUCC(ret) && !has_selected && j < replica_loc_list.count(); ++j) {
        if (ObReplicaTypeCheck::is_readable_replica(replica_loc_list.at(j).get_replica_type())) {
          if (fixed_server == replica_loc_list.at(j).get_server()) {
            if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(j))) {
              LOG_WARN("fail to set selected replica idx", K(ret), K(j), K(phy_part_loc_info));
            } else {
              has_selected = true;
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(!has_selected)) {
        // 由于迁移unit的时候，mv对应的表没法同时完成，因此这里有可能找不到副本，
        // 返回OB_NO_READABLE_REPLICA让sql层刷新location cache重试。
        ret = OB_NO_READABLE_REPLICA;
        LOG_WARN("cannot found readable partition", K(ret), K(fixed_server));
      }
    }
  }
  return ret;
}

int ObCandiTableLoc::get_all_servers(common::ObIArray<common::ObAddr> &servers) const
{
  int ret = OB_SUCCESS;
  const ObCandiTabletLocIArray &phy_part_loc_info_list = get_phy_part_loc_info_list();
  FOREACH_CNT_X(it, phy_part_loc_info_list, OB_SUCC(ret)) {
    share::ObLSReplicaLocation replica_location;
    if (OB_FAIL((*it).get_selected_replica(replica_location))) {
      LOG_WARN("fail to get selected replica", K(*it));
    } else if (!replica_location.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("replica location is invalid", K(ret), K(replica_location));
    } else if (OB_FAIL(add_var_to_array_no_dup(servers, replica_location.get_server()))) {
      LOG_WARN("failed to push back server", K(ret));
    }
  }
  return ret;
}

//给定ObCandiTableLoc 和ObCandiTabletLoc(来自复制表)
//判断是否前者的每个分区leader都在同一个server, 且上面都存在复制表的副本, 如果是则返回TRUE
int ObCandiTableLoc::is_server_in_replica(const ObCandiTableLoc &l_phy_loc,
                                                 const ObCandiTabletLoc &part_loc_info,
                                                 bool &is_same,
                                                 ObAddr &the_server,
                                                 int64_t &new_idx)
{
  int ret = OB_SUCCESS;
  ObAddr first_server;
  ObLSReplicaLocation replica_location;
  const ObCandiTabletLocIArray &left_locations = l_phy_loc.get_phy_part_loc_info_list();
  is_same = true;
  for (int64_t i = 0; is_same && OB_SUCC(ret) && i < left_locations.count(); ++i) {
    replica_location.reset();
    const ObCandiTabletLoc &l_part_loc_info = left_locations.at(i);
    if (OB_FAIL(l_part_loc_info.get_selected_replica(replica_location))) {
      LOG_WARN("fail to get leader", K(ret), K(l_part_loc_info.get_partition_location()));
    } else if (0 == i) {
      first_server = replica_location.get_server();
      the_server = first_server;
    } else if (first_server != replica_location.get_server()) {
      is_same = false;
    }
  }
  if (OB_SUCC(ret) && is_same && !part_loc_info.is_server_in_replica(first_server, new_idx)) {
    is_same = false;
  }
  return ret;
}

bool ObCandiTableLoc::is_server_in_replica(const ObAddr &server,
                                                  ObIArray<int64_t> &new_replic_idxs) const
{
  int ret = OB_SUCCESS;
  bool found_flag = false;
  int64_t partition_location_count = get_partition_cnt();
  for (int64_t j = 0; OB_SUCC(ret) && j < partition_location_count; j++) {
    const ObCandiTabletLoc &phy_part_loc_info = get_phy_part_loc_info_list().at(j);
    if (!phy_part_loc_info.is_server_in_replica(server, new_replic_idxs.at(j))) {
      found_flag = false;
      break;
    } else {
      found_flag = true;
    }
  }
  return found_flag;
}

void ObCandiTableLoc::set_table_location_key(uint64_t table_location_key, uint64_t ref_table_id)
{
  table_location_key_ = table_location_key;
  ref_table_id_ = ref_table_id;
}

int ObCandiTableLoc::replace_local_index_loc(DASRelatedTabletMap &map, ObTableID ref_table_id)
{
  int ret = OB_SUCCESS;
  ref_table_id_ = ref_table_id;
  for (int64_t i = 0; i < candi_tablet_locs_.count(); ++i) {
    ObOptTabletLoc &tablet_loc = candi_tablet_locs_.at(i).get_partition_location();
    const DASRelatedTabletMap::Value *rv = nullptr;
    if (OB_ISNULL(rv = map.get_related_tablet_id(tablet_loc.get_tablet_id(), ref_table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("related tablet info is invalid", K(ret),
               K(tablet_loc.get_tablet_id()), K(ref_table_id), K(map));
    } else {
      tablet_loc.set_tablet_info(rv->tablet_id_, rv->part_id_, rv->first_level_part_id_);
    }
  }
  return ret;
}
}/* ns sql*/
}/* ns oceanbase */
