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

#define USING_LOG_PREFIX RS_LB

#include "ob_disaster_recovery_worker.h"

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_task_define.h"
#include "share/ob_define.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_disaster_recovery_task.h"
#include "rootserver/ob_disaster_recovery_info.h"
#include "rootserver/ob_disaster_recovery_task_mgr.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_balance_info.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace rootserver
{

ObLSReplicaTaskDisplayInfo::ObLSReplicaTaskDisplayInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    task_type_(ObDRTaskType::MAX_TYPE),
    task_priority_(ObDRTaskPriority::MAX_PRI),
    target_server_(),
    target_replica_type_(REPLICA_TYPE_FULL),
    target_replica_paxos_replica_number_(OB_INVALID_COUNT),
    source_server_(),
    source_replica_type_(REPLICA_TYPE_FULL),
    source_replica_paxos_replica_number_(OB_INVALID_COUNT),
    execute_server_(),
    comment_("LSReplicaTask")
{
}

ObLSReplicaTaskDisplayInfo::~ObLSReplicaTaskDisplayInfo()
{
}

void ObLSReplicaTaskDisplayInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  task_type_ = ObDRTaskType::MAX_TYPE;
  task_priority_ = ObDRTaskPriority::MAX_PRI;
  target_server_.reset();
  target_replica_type_ = REPLICA_TYPE_FULL;
  target_replica_paxos_replica_number_ = OB_INVALID_COUNT;
  source_server_.reset();
  source_replica_type_ = REPLICA_TYPE_FULL;
  source_replica_paxos_replica_number_ = OB_INVALID_COUNT;
  execute_server_.reset();
  comment_.reset();
}

int ObLSReplicaTaskDisplayInfo::init(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const ObDRTaskType &task_type,
    const ObDRTaskPriority &task_priority,
    const common::ObAddr &target_server,
    const common::ObReplicaType &target_replica_type,
    const int64_t &target_replica_paxos_replica_number,
    const common::ObAddr &source_server,
    const common::ObReplicaType &source_replica_type,
    const int64_t &source_replica_paxos_replica_number,
    const common::ObAddr &execute_server,
    const ObString &comment)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init a ObLSReplicaTaskDisplayInfo",
             KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("fail to assign comment", KR(ret), K(comment));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    task_type_ = task_type;
    task_priority_ = task_priority;
    target_server_ = target_server;
    target_replica_type_ = target_replica_type;
    target_replica_paxos_replica_number_ = target_replica_paxos_replica_number;
    source_server_ = source_server;
    source_replica_type_ = source_replica_type;
    source_replica_paxos_replica_number_ = source_replica_paxos_replica_number;
    execute_server_ = execute_server;
  }
  return ret;
}

bool ObLSReplicaTaskDisplayInfo::is_valid() const
{
  // do not check source_server and source_replica_type because it maybe useless
  return ls_id_.is_valid_with_tenant(tenant_id_)
         && task_type_ != ObDRTaskType::MAX_TYPE
         && task_priority_ != ObDRTaskPriority::MAX_PRI
         && target_server_.is_valid()
         && target_replica_type_ != REPLICA_TYPE_MAX
         && target_replica_paxos_replica_number_ != OB_INVALID_COUNT
         && source_replica_paxos_replica_number_ != OB_INVALID_COUNT
         && execute_server_.is_valid();
}

int ObLSReplicaTaskDisplayInfo::assign(const ObLSReplicaTaskDisplayInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(comment_.assign(other.comment_))) {
      LOG_WARN("fail to assign comment", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      ls_id_ = other.ls_id_;
      task_type_ = other.task_type_;
      task_priority_ = other.task_priority_;
      target_server_ = other.target_server_;
      target_replica_type_ = other.target_replica_type_;
      target_replica_paxos_replica_number_ = other.target_replica_paxos_replica_number_;
      source_server_ = other.source_server_;
      source_replica_type_ = other.source_replica_type_;
      source_replica_paxos_replica_number_ = other.source_replica_paxos_replica_number_;
      execute_server_ = other.execute_server_;
    }
  }
  return ret;
}

int64_t ObLSReplicaTaskDisplayInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(
      K_(tenant_id),
      K_(ls_id),
      K_(task_type),
      K_(task_priority),
      K_(target_server),
      K_(target_replica_type),
      K_(target_replica_paxos_replica_number),
      K_(source_server),
      K_(source_replica_type),
      K_(source_replica_paxos_replica_number),
      K_(execute_server),
      K_(comment));
  J_OBJ_END();
  return pos;
}

int ObDRWorker::LocalityAlignment::locate_zone_locality(
    const common::ObZone &zone,
    ReplicaDescArray *&replica_desc_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ReplicaDescArray *my_desc_array = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, my_desc_array);
    if (OB_SUCCESS == tmp_ret) {
      if (OB_ISNULL(my_desc_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my_desc_array ptr is null", KR(ret));
      } else {
        replica_desc_array = my_desc_array;
      }
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      void *raw_ptr = nullptr;
      my_desc_array = nullptr;
      if (OB_UNLIKELY(nullptr == (raw_ptr = allocator_.alloc(sizeof(ReplicaDescArray))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret));
      } else if (OB_UNLIKELY(nullptr == (my_desc_array = new (raw_ptr) ReplicaDescArray()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to construct replica desc array", KR(ret));
      } else if (OB_FAIL(locality_map_.set_refactored(zone, my_desc_array))) {
        LOG_WARN("fail to set refactored", KR(ret));
      } else {
        replica_desc_array = my_desc_array;
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(my_desc_array)) {
        my_desc_array->~ReplicaDescArray();
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get from map", KR(ret), K(zone));
    }
  }
  return ret;
}

ObDRWorker::LocalityAlignment::LocalityAlignment(ObUnitManager *unit_mgr,
                                                 ObZoneManager *zone_mgr,
                                                 DRLSInfo &dr_ls_info)
  : task_idx_(0),
    add_replica_task_(),
    unit_mgr_(unit_mgr),
    zone_mgr_(zone_mgr),
    dr_ls_info_(dr_ls_info),
    task_array_(),
    curr_paxos_replica_number_(0),
    locality_paxos_replica_number_(0),
    locality_map_(),
    replica_stat_map_(),
    unit_provider_(),
    allocator_()
{
}

ObDRWorker::LocalityAlignment::~LocalityAlignment()
{
  for (int64_t i = 0; i < task_array_.count(); ++i) {
    LATask *task = task_array_.at(i);
    if (nullptr != task) {
      task->~LATask();
    }
  }
  for (LocalityMap::iterator iter = locality_map_.begin(); iter != locality_map_.end(); ++iter) {
    ReplicaDescArray *val = iter->second;
    if (nullptr != val) {
      val->~ReplicaDescArray();
    }
  }
  allocator_.reset(); // free the memory above
}

int ObDRWorker::LocalityAlignment::generate_paxos_replica_number()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObZoneReplicaAttrSet> &locality_array = dr_ls_info_.get_locality();
  curr_paxos_replica_number_ = dr_ls_info_.get_paxos_replica_number();
  locality_paxos_replica_number_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < locality_array.count(); ++i) {
    const ObZoneReplicaAttrSet &locality = locality_array.at(i);
    locality_paxos_replica_number_ += locality.get_paxos_replica_num();
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::build_locality_stat_map()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id;
  if (OB_ISNULL(unit_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("LocalityAlignment not init", KR(ret), KP(unit_mgr_));
  } else if (OB_FAIL(dr_ls_info_.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret));
  } else {
    LOG_INFO("build ls locality stat map", K(tenant_id), K(ls_id));
    for (int64_t i = 0; OB_SUCC(ret) && i < dr_ls_info_.get_locality().count(); ++i) {
      ReplicaDescArray *zone_replica_desc = nullptr;
      const ObZoneReplicaAttrSet &zone_locality = dr_ls_info_.get_locality().at(i);
      const common::ObZone &zone = zone_locality.zone_;
      // full locality
      const ObIArray<ReplicaAttr> &full_locality =
        zone_locality.replica_attr_set_.get_full_replica_attr_array();
      // logonly locality
      const ObIArray<ReplicaAttr> &logonly_locality =
        zone_locality.replica_attr_set_.get_logonly_replica_attr_array();
      // encryption logonly locality
      const ObIArray<ReplicaAttr> &encryption_locality =
        zone_locality.replica_attr_set_.get_encryption_logonly_replica_attr_array();
      // readonly locality
      const ObIArray<ReplicaAttr> &readonly_locality =
        zone_locality.replica_attr_set_.get_readonly_replica_attr_array();

      if (OB_FAIL(locate_zone_locality(zone, zone_replica_desc))) {
        LOG_WARN("fail to locate zone locality", KR(ret), K(zone));
      } else if (OB_ISNULL(zone_replica_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to locate zone locality", KR(ret), K(zone));
      } else {
        // full replica
        for (int64_t j = 0; OB_SUCC(ret) && j < full_locality.count(); ++j) {
          const ReplicaAttr &replica_attr = full_locality.at(j);
          if (1 != replica_attr.num_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replica num unexpected", KR(ret), K(zone), K(full_locality));
          } else if (OB_FAIL(zone_replica_desc->push_back(ReplicaDesc(REPLICA_TYPE_FULL,
                                                                      replica_attr.memstore_percent_,
                                                                      replica_attr.num_)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        // logonly replica
        for (int64_t j = 0; OB_SUCC(ret) && j < logonly_locality.count(); ++j) {
          const ReplicaAttr &replica_attr = logonly_locality.at(j);
          if (1 != replica_attr.num_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replica num unexpected", KR(ret), K(zone), K(logonly_locality));
          } else if (OB_FAIL(zone_replica_desc->push_back(ReplicaDesc(REPLICA_TYPE_LOGONLY,
                                                                      replica_attr.memstore_percent_,
                                                                      replica_attr.num_)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        // encryption logonly replica
        for (int64_t j = 0; OB_SUCC(ret) && j < encryption_locality.count(); ++j) {
          const ReplicaAttr &replica_attr = encryption_locality.at(j);
          if (1 != replica_attr.num_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replica num unexpected", KR(ret), K(zone), K(encryption_locality));
          } else if (OB_FAIL(zone_replica_desc->push_back(ReplicaDesc(REPLICA_TYPE_ENCRYPTION_LOGONLY,
                                                                      replica_attr.memstore_percent_,
                                                                      replica_attr.num_)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        // readonly replica, all_server
        if (dr_ls_info_.is_duplicate_ls()) {
          // duplicate ls, should has R-replica all_server
          zone_replica_desc->is_readonly_all_server_ = true;
          zone_replica_desc->readonly_memstore_percent_ = 100;
        } else {
          // readonly replica, normal
          for (int64_t j = 0; OB_SUCC(ret) && j < readonly_locality.count(); ++j) {
            const ReplicaAttr &replica_attr = readonly_locality.at(j);
            if (0 >= replica_attr.num_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("replica num unexpected", KR(ret), K(zone), K(readonly_locality));
            } else if (OB_FAIL(zone_replica_desc->push_back(ReplicaDesc(REPLICA_TYPE_READONLY,
                                                                        replica_attr.memstore_percent_,
                                                                        replica_attr.num_)))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          }
        }
      }
      LOG_INFO("ls zone locality map info", K(zone), KPC(zone_replica_desc));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::build_replica_stat_map()
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  if (OB_FAIL(dr_ls_info_.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_cnt; ++i) {
      share::ObLSReplica *replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(dr_ls_info_.get_replica_stat(
              i,
              replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica related ptrs are null", KR(ret),
                 KP(replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
      } else if (OB_FAIL(replica_stat_map_.push_back(
              ReplicaStatDesc(replica,
                              server_stat_info,
                              unit_stat_info,
                              unit_in_group_stat_info)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    LOG_INFO("ls replica stat", "replica_stat_map", replica_stat_map_);
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_remove_match(
    ReplicaStatDesc &replica_stat_desc,
    const int64_t index)
{
  // replica_stat_desc contains informations of one replica in __all_ls_meta_table.
  // try_remove_match() aimed to figure out whether this replica's location and type is expected.
  // If is expected, we remove it from both replica_desc and locality_desc.
  // If is not expected, reserve this replica in replica_desc and locality_desc,
  // let do_generate_locality_task() generate certain tasks by referencing replica_desc and locality_desc later.
  //
  // We regard a replica is expected if these rules below all satisfied:
  //   (rule 1) this replica's unit is in pool
  //   (rule 2) this replica's unit is in active status
  //   (rule 3) the server of the replica is the same as the server of the unit it belongs
  //   (rule 4) the type of this replica is the same as locality described
  //   (rule 5) the memstore_percent is the same as locality described
  //   (rule 6) the remained replica number described in locality is not 0
  //
  // Rule 2 can ensure not removing replica on active unit when another unit is in deleting status
  // Rule 3 can ensure not removing migrate dest replica when source replica and dest replica both exists in member_list(learner_list)
  //
  // Under the rules described above, consider this case:
  //   This replica is migrate source replica or the unit it belongs is deleting AND dest replica not exist yet.
  //   This replica can remained in replica_desc and locality_desc, because one of those rules not satisfied.
  //   BUT we should treat this replica is expected and remove it from replica_desc and locality_desc anyway.
  //   Because we want migrate_unit() and shrink_resource_pool() to handle this situation, DO NOT let locality_alignment generate tasks
  int ret = OB_SUCCESS;
  share::ObLSReplica *replica = nullptr;
  DRServerStatInfo *server_stat_info = nullptr;
  DRUnitStatInfo *unit_stat_info = nullptr;
  if (OB_UNLIKELY(!replica_stat_desc.is_valid()
                  || index >= replica_stat_map_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc), K(index),
             "replica_stat_map_count", replica_stat_map_.count());
  } else if (OB_ISNULL(replica = replica_stat_desc.replica_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica ptr is null", KR(ret), K(replica_stat_desc));
  } else if (OB_ISNULL(server_stat_info = replica_stat_desc.server_stat_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat info ptr is null", KR(ret));
  } else if (OB_ISNULL(unit_stat_info = replica_stat_desc.unit_stat_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat info ptr is null", KR(ret));
  } else {
    const common::ObZone &zone = replica->get_zone();
    ReplicaDescArray *zone_replica_desc = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, zone_replica_desc);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // zone not exist in locality, not match
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_ISNULL(zone_replica_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone replica desc ptr is null", KR(ret), K(zone));
      } else {
        bool has_correct_dest_replica = false;
        if (replica->get_server() != unit_stat_info->get_unit_info().unit_.server_
            || !unit_stat_info->get_unit_info().unit_.is_active_status()) {
          // this replica is migrating or unit is deleting, check whether has a correct dest replica
          LOG_TRACE("try to check whether has dest replica", KPC(replica), KPC(unit_stat_info));
          const int64_t map_count = replica_stat_map_.count();
          for (int64_t i = map_count - 1; OB_SUCC(ret) && i >= 0; --i) {
            ReplicaStatDesc &replica_stat_desc_to_compare = replica_stat_map_.at(i);
            if (OB_UNLIKELY(!replica_stat_desc_to_compare.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("replica stat desc unexpected", KR(ret), K(replica_stat_desc_to_compare));
            } else if (OB_ISNULL(replica_stat_desc_to_compare.unit_stat_info_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc_to_compare));
            } else if (replica->get_zone() != replica_stat_desc_to_compare.replica_->get_zone()) {
              // not the same zone, just skip
            } else if (replica->is_in_service()
                       && replica_stat_desc_to_compare.replica_->get_replica_type() == replica->get_replica_type()
                       && replica_stat_desc_to_compare.replica_->get_server() != replica->get_server()
                       && (replica_stat_desc_to_compare.replica_->get_server() == unit_stat_info->get_unit_info().unit_.server_
                           || replica_stat_desc_to_compare.replica_->get_server() == replica_stat_desc_to_compare.unit_stat_info_->get_unit_info().unit_.server_)
                       && replica_stat_desc_to_compare.unit_stat_info_->get_unit_info().unit_.is_active_status()) {
              // A replica is a correct dest replica if these conditions above all satisfied
              //   (1) replica is in member_list(learner_lsit)
              //   (2) replica type is expected
              //   (3) replica is not on deleting unit
              //   (4) replica is on the server the same as its own unit (a unit migrate task triggered expected dest replica)
              //       OR replica is on the server the same as source replica's unit (a shrink resource task triggerd expected dest replica)
              has_correct_dest_replica = true;
              break;
            } else {
              LOG_TRACE("dest replica not match",
                        "replica_type_to_compare", replica_stat_desc_to_compare.replica_->get_replica_type(),
                        "replica_type", replica->get_replica_type(),
                        "server_to_compare", replica_stat_desc_to_compare.replica_->get_server(),
                        "server", replica->get_server(),
                        "server_with_unit", unit_stat_info->get_unit_info().unit_.server_,
                        "server_with_unit_to_compare", replica_stat_desc_to_compare.unit_stat_info_->get_unit_info().unit_.server_,
                        "unit_status_is_active", replica_stat_desc_to_compare.unit_stat_info_->get_unit_info().unit_.is_active_status());
            }
          }
        }

        for (int64_t i = zone_replica_desc->count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          bool found = false;
          ReplicaDesc &replica_desc = zone_replica_desc->at(i);
          if (unit_stat_info->is_in_pool()
              && replica->get_replica_type() == replica_desc.replica_type_
              && replica->get_memstore_percent() == replica_desc.memstore_percent_
              && replica_desc.replica_num_ > 0
              && (!has_correct_dest_replica
                  || (unit_stat_info->get_unit_info().unit_.is_active_status()
                      && server_stat_info->get_server() == unit_stat_info->get_unit_info().unit_.server_))) {
            found = true;
            if (OB_FAIL(replica_stat_map_.remove(index))) {
              LOG_WARN("fail to remove from stat map", KR(ret));
            } else if (FALSE_IT(--replica_desc.replica_num_)) {
              // shall never be here
            } else if (replica_desc.replica_num_ > 0) {
              // bypass
            } else if (OB_FAIL(zone_replica_desc->remove(i))) {
              LOG_WARN("fail to remove element", KR(ret));
            } else {
              break;
            }
          }
        }
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get from locality map", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::prepare_generate_locality_task()
{
  int ret = OB_SUCCESS;
  const int64_t map_count = replica_stat_map_.count();
  for (int64_t i = map_count - 1; OB_SUCC(ret) && i >= 0; --i) {
    ReplicaStatDesc &replica_stat_desc = replica_stat_map_.at(i);
    if (OB_UNLIKELY(!replica_stat_desc.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica stat desc unexpected", KR(ret));
    } else if (OB_FAIL(try_remove_match(replica_stat_desc, i))) {
      LOG_WARN("fail to try remove match", KR(ret));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::do_generate_locality_task_from_full_replica(
    ReplicaStatDesc &replica_stat_desc,
    share::ObLSReplica &replica,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  const common::ObZone &zone = replica.get_zone();
  if (REPLICA_TYPE_FULL != replica.get_replica_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica type unexpected", KR(ret), K(replica));
  } else {
    ReplicaDescArray *zone_replica_desc = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, zone_replica_desc);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove replica task", KR(ret));
      } else if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret));
      }
    } else if (OB_SUCCESS == tmp_ret && nullptr != zone_replica_desc) {
      bool found = false;
      std::sort(zone_replica_desc->begin(), zone_replica_desc->end());
      for (int64_t i = zone_replica_desc->count() - 1; !found && OB_SUCC(ret) && i >= 0; --i) {
        ReplicaDesc &replica_desc = zone_replica_desc->at(i);
        if (REPLICA_TYPE_FULL == replica_desc.replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica type unexpected", KR(ret), K(dr_ls_info_));
        } else if (REPLICA_TYPE_LOGONLY == replica_desc.replica_type_
            || REPLICA_TYPE_READONLY == replica_desc.replica_type_) {
          if (OB_FAIL(generate_type_transform_task(
                  replica_stat_desc,
                  replica_desc.replica_type_,
                  replica_desc.memstore_percent_))) {
            LOG_WARN("fail to generate type transform task", KR(ret), K(replica_stat_desc));
          } else if (OB_FAIL(zone_replica_desc->remove(i))) {
            LOG_WARN("fail to remove", KR(ret), K(i), K(replica), KPC(zone_replica_desc));
          } else if (OB_FAIL(replica_stat_map_.remove(index))) {
            LOG_WARN("fail to remove", KR(ret), K(index), K(replica), K(replica_stat_map_));
          } else {
            found = true;
          }
        }
      }
      // process not found
      if (OB_FAIL(ret)) {
        // failed
      } else if (found) {
        // found, bypass
      } else if (zone_replica_desc->is_readonly_all_server_) {
        if (OB_FAIL(generate_type_transform_task(
                replica_stat_desc,
                REPLICA_TYPE_READONLY,
                zone_replica_desc->readonly_memstore_percent_))) {
          LOG_WARN("fail to generate type transform task", KR(ret), K(replica_stat_desc));
        } else if (OB_FAIL(replica_stat_map_.remove(index))) {
          LOG_WARN("fail to remove", KR(ret), K(index), K(replica), K(replica_stat_map_));
        }
      } else {
        if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove replica task", KR(ret));
        } else if (OB_FAIL(replica_stat_map_.remove(index))) {
          LOG_WARN("fail to remove", KR(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get refactored", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::do_generate_locality_task_from_logonly_replica(
    ReplicaStatDesc &replica_stat_desc,
    share::ObLSReplica &replica,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  const common::ObZone &zone = replica.get_zone();
  if (REPLICA_TYPE_LOGONLY != replica.get_replica_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica type unexpected", KR(ret), K(replica));
  } else {
    ReplicaDescArray *zone_replica_desc = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, zone_replica_desc);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove replica task", KR(ret));
      } else if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret));
      }
    } else if (OB_SUCCESS == tmp_ret && nullptr != zone_replica_desc) {
      bool found = false;
      std::sort(zone_replica_desc->begin(), zone_replica_desc->end());
      // defensive check
      for (int64_t i = zone_replica_desc->count() - 1; !found && OB_SUCC(ret) && i >= 0; --i) {
        ReplicaDesc &replica_desc = zone_replica_desc->at(i);
        if (REPLICA_TYPE_LOGONLY == replica_desc.replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica type unexpected", KR(ret), K(dr_ls_info_));
        }
      }
      // normal routine
      if (OB_SUCC(ret)) {
        if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove replica task", KR(ret));
        } else if (OB_FAIL(replica_stat_map_.remove(index))) {
          LOG_WARN("fail to remove", KR(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get refactored", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::do_generate_locality_task_from_encryption_logonly_replica(
    ReplicaStatDesc &replica_stat_desc,
    share::ObLSReplica &replica,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  const common::ObZone &zone = replica.get_zone();
  if (REPLICA_TYPE_ENCRYPTION_LOGONLY != replica.get_replica_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica type unexpected", KR(ret), K(replica));
  } else {
    ReplicaDescArray *zone_replica_desc = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, zone_replica_desc);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove replica task", KR(ret));
      } else if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret));
      }
    } else if (OB_SUCCESS == tmp_ret && nullptr != zone_replica_desc) {
      bool found = false;
      std::sort(zone_replica_desc->begin(), zone_replica_desc->end());
      // defensive check
      for (int64_t i = zone_replica_desc->count() - 1; !found && OB_SUCC(ret) && i >= 0; --i) {
        ReplicaDesc &replica_desc = zone_replica_desc->at(i);
        if (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_desc.replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica type unexpected", KR(ret), K(dr_ls_info_));
        }
      }
      // normal routine
      if (OB_SUCC(ret)) {
        if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove replica task", KR(ret));
        } else if (OB_FAIL(replica_stat_map_.remove(index))) {
          LOG_WARN("fail to remove", KR(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get refactored", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_generate_type_transform_task_for_readonly_replica_(
    ReplicaDescArray &zone_replica_desc_in_locality,
    ReplicaStatDesc &replica_stat_desc,
    const int64_t index,
    bool &task_generated)
{
  int ret = OB_SUCCESS;
  task_generated = false;
  if (OB_UNLIKELY(0 > index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index));
  } else {
    for (int64_t i = zone_replica_desc_in_locality.count() - 1; !task_generated && OB_SUCC(ret) && i >= 0; --i) {
      ReplicaDesc &replica_desc = zone_replica_desc_in_locality.at(i);
      if (REPLICA_TYPE_READONLY == replica_desc.replica_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica type unexpected", KR(ret), K(dr_ls_info_));
      } else if (REPLICA_TYPE_FULL == replica_desc.replica_type_) {
        if (OB_ISNULL(replica_stat_desc.unit_stat_info_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc));
        } else {
          const share::ObUnitInfo &unit_info = replica_stat_desc.unit_stat_info_->get_unit_info();
          bool server_is_active = false;
          if (!unit_info.unit_.is_active_status()) {
            FLOG_INFO("unit status is not normal, can not generate type transform task", K(unit_info));
          } else if (OB_FAIL(SVR_TRACER.check_server_active(unit_info.unit_.server_, server_is_active))) {
            LOG_WARN("fail to check server is active", KR(ret), K(unit_info));
          } else if (!server_is_active) {
            FLOG_INFO("server status is not active, can not generate type transform task", K(unit_info));
          } else if (OB_FAIL(generate_type_transform_task(
                  replica_stat_desc,
                  replica_desc.replica_type_,
                  replica_desc.memstore_percent_))) {
            LOG_WARN("fail to generate type transform task", KR(ret), K(replica_stat_desc));
          } else if (OB_FAIL(zone_replica_desc_in_locality.remove(i))) {
            LOG_WARN("fail to remove", KR(ret), K(i), K(replica_stat_desc), K(zone_replica_desc_in_locality));
          } else if (OB_FAIL(replica_stat_map_.remove(index))) {
            LOG_WARN("fail to remove", KR(ret), K(index), K(replica_stat_desc), K(replica_stat_map_));
          } else {
            task_generated = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_generate_remove_readonly_task_for_duplicate_log_stream_(
    ReplicaStatDesc &replica_stat_desc,
    share::ObLSReplica &replica,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator unit_operator;
  common::ObArray<share::ObUnit> unit_info_array;
  if (OB_ISNULL(GCTX.sql_proxy_) || OB_UNLIKELY(0 > index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index));
  } else if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(gen_user_tenant_id(replica.get_tenant_id()), unit_info_array))) {
    LOG_WARN("fail to get unit info array", KR(ret), K(replica));
  } else {
    bool replica_need_delete = true;
    for (int64_t j = 0; OB_SUCC(ret) && j < unit_info_array.count(); ++j) {
      if (unit_info_array.at(j).server_ == replica.get_server()) {
        replica_need_delete = false;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (replica_need_delete) {
      // this R-replica need to be deleted
      if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove replica task", KR(ret), K(replica_stat_desc));
      } else if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret), K(index), K(replica), K(replica_stat_map_));
      }
    } else {
      // delete this R-replica from memory to avoid removing this replca
      if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret), K(replica_stat_map_), K(index));
      }
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::do_generate_locality_task_from_readonly_replica(
    ReplicaStatDesc &replica_stat_desc,
    share::ObLSReplica &replica,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  const common::ObZone &zone = replica.get_zone();
  if (REPLICA_TYPE_READONLY != replica.get_replica_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica type unexpected", KR(ret), K(replica));
  } else {
    ReplicaDescArray *zone_replica_desc = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, zone_replica_desc);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // zone has been shrinked, generate remove replica task
      if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove replica task", KR(ret));
      } else if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret));
      }
    } else if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(zone_replica_desc)) {
      bool task_generated = false;
      std::sort(zone_replica_desc->begin(), zone_replica_desc->end());
      // try to generate type_transform task if needed
      if (OB_FAIL(try_generate_type_transform_task_for_readonly_replica_(
                      *zone_replica_desc, replica_stat_desc, index, task_generated))) {
        LOG_WARN("fail to try generate type transform task", KR(ret),
                 KPC(zone_replica_desc), K(replica_stat_desc), K(index), K(task_generated));
      } else if (task_generated) {
        // a type transform task generated, bypass
      } else if (zone_replica_desc->is_readonly_all_server_) {
        // for duplicate log stream, try to remove redudant R-replicas
        if (OB_FAIL(try_generate_remove_readonly_task_for_duplicate_log_stream_(replica_stat_desc, replica, index))) {
          LOG_WARN("fail to generate remove replica task for duplicate log stream",
                   KR(ret), K(replica_stat_desc), K(replica), K(index));
        }
      } else {
        // for common log stream, just generate remove replica task
        if (OB_FAIL(generate_remove_replica_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove replica task", KR(ret), K(replica_stat_desc));
        } else if (OB_FAIL(replica_stat_map_.remove(index))) {
          LOG_WARN("fail to remove", KR(ret), K(replica_stat_map_), K(index));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get refactored", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_generate_locality_task_from_locality_map()
{
  int ret = OB_SUCCESS;
  LocalityMap::iterator iter = locality_map_.begin();
  for (; iter != locality_map_.end(); ++iter) {
    ReplicaDescArray *replica_desc_array = iter->second;
    if (OB_ISNULL(replica_desc_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone locality ptr is null", KR(ret), "zone", iter->first);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < replica_desc_array->count(); ++i) {
        if (OB_FAIL(generate_add_replica_task(iter->first, replica_desc_array->at(i)))) {
          LOG_WARN("fail to generate add replica task", KR(ret));
        } else {
          LOG_INFO("success generate add replica task from locality", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_generate_locality_task_from_paxos_replica_number()
{
  int ret = OB_SUCCESS;
  if (curr_paxos_replica_number_ == locality_paxos_replica_number_) {
    // bypass, paxos_replica_number match
  } else if (OB_FAIL(generate_modify_paxos_replica_number_task())) {
    LOG_WARN("fail to generate modify paxos replica number task", KR(ret));
  }
  return ret;
}

void ObDRWorker::LocalityAlignment::print_locality_information()
{
  for (LocalityMap::iterator iter = locality_map_.begin();
       iter != locality_map_.end();
       ++iter) {
    if (nullptr == iter->second) {
      LOG_INFO("zone locality ptr is null", "zone", iter->first);
    } else {
      LOG_INFO("zone locality", "zone", iter->first, "locality_info", *iter->second);
    }
  }
  LOG_INFO("replica stat map not empty", K(replica_stat_map_));
}

int ObDRWorker::LocalityAlignment::do_generate_locality_task()
{
  int ret = OB_SUCCESS;
  // step0: generate task from replica stat map
  for (int64_t i = replica_stat_map_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    ReplicaStatDesc &replica_stat_desc = replica_stat_map_.at(i);
    share::ObLSReplica *replica = replica_stat_desc.replica_;
    if (OB_UNLIKELY(!replica_stat_desc.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica stat desc unexpected", KR(ret));
    } else if (OB_ISNULL(replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica ptr is null", KR(ret));
    } else if (REPLICA_TYPE_FULL == replica->get_replica_type()) {
      if (OB_FAIL(do_generate_locality_task_from_full_replica(
              replica_stat_desc,
              *replica,
              i))) {
        LOG_WARN("fail to generate locality task from full replica", KR(ret));
      }
    } else if (REPLICA_TYPE_LOGONLY == replica->get_replica_type()) {
      if (OB_FAIL(do_generate_locality_task_from_logonly_replica(
              replica_stat_desc,
              *replica,
              i))) {
        LOG_WARN("fail to generate locality task from logonly replica", KR(ret));
      }
    } else if (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica->get_replica_type()) {
      if (OB_FAIL(do_generate_locality_task_from_encryption_logonly_replica(
              replica_stat_desc,
              *replica,
              i))) {
        LOG_WARN("fail to generate locality task from logonly replica", KR(ret));
      }
    } else if (REPLICA_TYPE_READONLY == replica->get_replica_type()) {
      if (OB_FAIL(do_generate_locality_task_from_readonly_replica(
              replica_stat_desc,
              *replica,
              i))) {
        LOG_WARN("fail to generate locality task from readonly replica", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type unexpected", KR(ret), KPC(replica));
    }
  }
  // step1: generate task from locality task
  if (OB_SUCC(ret)) {
    if (replica_stat_map_.count() <= 0) {
      if (OB_FAIL(try_generate_locality_task_from_locality_map())) {
        LOG_WARN("fail to do generate locality task from locality map", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected replica stat map", KR(ret));
      print_locality_information();
    }
  }
  // step2: generate modify paxos replica number task
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_generate_locality_task_from_paxos_replica_number())) {
      LOG_WARN("fail to generate task from paxos_replica_number", KR(ret));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::generate_locality_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_generate_locality_task())) {
    LOG_WARN("fail to prepare generate locality task", KR(ret));
  } else if (OB_FAIL(do_generate_locality_task())) {
    LOG_WARN("fail to do generate locality task", KR(ret));
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::generate_remove_replica_task(
    ReplicaStatDesc &replica_stat_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!replica_stat_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc));
  } else {
    void *raw_ptr = nullptr;
    RemoveReplicaLATask *task = nullptr;
    ObLSReplica *replica = replica_stat_desc.replica_;
    if (OB_ISNULL(replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica ptr is null", KR(ret), KP(replica));
    } else if (nullptr == (raw_ptr = allocator_.alloc(sizeof(RemoveReplicaLATask)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (nullptr == (task = new (raw_ptr) RemoveReplicaLATask())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct task failed", KR(ret));
    } else {
      task->remove_server_ = replica->get_server();
      task->replica_type_ = replica->get_replica_type();
      task->memstore_percent_ = replica->get_memstore_percent();
      task->member_time_us_ = replica->get_member_time_us();
      task->orig_paxos_replica_number_ = replica->get_paxos_replica_number();
      task->paxos_replica_number_ = replica->get_paxos_replica_number();
      if (OB_FAIL(task_array_.push_back(task))) {
        LOG_WARN("fail to push back", KR(ret), KPC(task));
      } else {
        LOG_INFO("success to push a remove replica task to task_array", KR(ret), KPC(task));
      }
    }
    // btw: no need to free memory when failed for arena, just destruct
    if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
      task->~RemoveReplicaLATask();
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::generate_type_transform_task(
    ReplicaStatDesc &replica_stat_desc,
    const ObReplicaType dst_replica_type,
    const int64_t dst_memstore_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!replica_stat_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc));
  } else {
    void *raw_ptr = nullptr;
    TypeTransformLATask *task = nullptr;
    share::ObLSReplica *replica = replica_stat_desc.replica_;
    DRUnitStatInfo *unit_stat_info = replica_stat_desc.unit_stat_info_;
    if (OB_UNLIKELY(nullptr == replica || nullptr == unit_stat_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica ptr is null", KR(ret));
    } else if (nullptr == (raw_ptr = allocator_.alloc(sizeof(TypeTransformLATask)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (nullptr == (task = new (raw_ptr) TypeTransformLATask())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct task failed", KR(ret));
    } else {
      task->zone_ = replica->get_zone();
      task->dst_server_ = replica->get_server();
      task->unit_id_ = unit_stat_info->get_unit_info().unit_.unit_id_;
      task->unit_group_id_ = unit_stat_info->get_unit_info().unit_.unit_group_id_;
      task->src_replica_type_ = replica->get_replica_type();
      task->src_memstore_percent_ = replica->get_memstore_percent();
      task->src_member_time_us_ = replica->get_member_time_us();
      task->dst_replica_type_ = dst_replica_type;
      task->dst_memstore_percent_ = dst_memstore_percent;
      task->dst_member_time_us_ = ObTimeUtility::current_time();
      if (OB_FAIL(task_array_.push_back(task))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        LOG_INFO("success to push a type transform task to task_array", KR(ret), KPC(task));
      }
    }
    // btw: no need to free memory when failed for arena, just destruct
    if (OB_FAIL(ret) && nullptr != task) {
      task->~TypeTransformLATask();
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::generate_add_replica_task(
    const common::ObZone &zone,
    ReplicaDesc &replica_desc)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < replica_desc.replica_num_; ++i) {
    void *raw_ptr = nullptr;
    AddReplicaLATask *task = nullptr;
    if (nullptr == (raw_ptr = allocator_.alloc(sizeof(AddReplicaLATask)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (nullptr == (task = new (raw_ptr) AddReplicaLATask())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct task failed", KR(ret));
    } else {
      task->zone_ = zone;
      task->replica_type_ = replica_desc.replica_type_;
      task->memstore_percent_ = replica_desc.memstore_percent_;
      task->member_time_us_ = ObTimeUtility::current_time();
      if (OB_FAIL(task_array_.push_back(task))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        LOG_INFO("success to push a add replica task to task_array", KR(ret), KPC(task));
      }
    }
    LOG_INFO("finish generate task from locality", KPC(task), K(replica_desc));
    // btw: no need to free memory when failed for arena, just destruct
    if (OB_FAIL(ret) && nullptr != task) {
      task->~AddReplicaLATask();
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::generate_modify_paxos_replica_number_task()
{
  int ret = OB_SUCCESS;

  void *raw_ptr = nullptr;
  ModifyPaxosReplicaNumberLATask *task = nullptr;
  if (nullptr == (raw_ptr = allocator_.alloc(sizeof(ModifyPaxosReplicaNumberLATask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (nullptr == (task = new (raw_ptr) ModifyPaxosReplicaNumberLATask())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("construct task failed", KR(ret));
  } else {
    if (OB_FAIL(task_array_.push_back(task))) {
      LOG_WARN("fail to push back", KR(ret));
    } else {
        LOG_INFO("success to push a modify paxos replica number task to task_array", KR(ret), KPC(task));
      }
  }
  // btw: no need to free memory when failed for arena, just destruct
  if (OB_FAIL(ret) && nullptr != task) {
    task->~ModifyPaxosReplicaNumberLATask();
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::build()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_UNLIKELY(nullptr == unit_mgr_
                  || nullptr == zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("LocalityAlignment not init", KR(ret), KP(unit_mgr_), KP(zone_mgr_));
  } else if (OB_FAIL(locality_map_.create(LOCALITY_MAP_BUCKET_NUM, "LocAlign"))) {
    LOG_WARN("fail to create locality map", KR(ret));
  } else if (OB_FAIL(generate_paxos_replica_number())) {
    LOG_WARN("fail to generate paxos_replica_number", KR(ret));
  } else if (OB_FAIL(build_locality_stat_map())) {
    LOG_WARN("fail to build locality map", KR(ret));
  } else if (OB_FAIL(build_replica_stat_map())) {
    LOG_WARN("fail to build replica map", KR(ret));
  } else if (OB_FAIL(generate_locality_task())) {
    LOG_WARN("fail to generate locality task", KR(ret));
  } else if (OB_FAIL(dr_ls_info_.get_tenant_id(tenant_id))) {
    LOG_WARN("fail to get tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(unit_provider_.init(gen_user_tenant_id(tenant_id),
          dr_ls_info_, unit_mgr_))) {
    LOG_WARN("fail to init unit provider", KR(ret), K(tenant_id), K_(dr_ls_info));
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_review_remove_replica_task(
    UnitProvider &unit_provider,
    LATask *this_task,
    const LATask *&output_task,
    bool &found)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_provider);
  RemoveReplicaLATask *my_task = reinterpret_cast<RemoveReplicaLATask *>(this_task);
  if (OB_ISNULL(this_task) || OB_ISNULL(my_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(this_task), KP(my_task));
  } else if (REPLICA_TYPE_FULL != my_task->replica_type_) {
    // no need to check when remove non-paxos replica
    my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
    my_task->paxos_replica_number_ = curr_paxos_replica_number_;
    output_task = my_task;
    found = true;
  } else {
    found = false;
    int64_t new_paxos_replica_number = 0;
    if (OB_FAIL(ObDRWorker::generate_disaster_recovery_paxos_replica_number(
            dr_ls_info_,
            curr_paxos_replica_number_,
            locality_paxos_replica_number_,
            MEMBER_CHANGE_SUB,
            new_paxos_replica_number,
            found))) {
      LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found));
    } else if (!found) {
      // bypass
    } else {
      my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
      my_task->paxos_replica_number_ = new_paxos_replica_number;
      output_task = my_task;
      found = true;
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_review_add_replica_task(
    UnitProvider &unit_provider,
    LATask *this_task,
    const LATask *&output_task,
    bool &found)
{
  int ret = OB_SUCCESS;
  const share::ObLSStatusInfo *ls_status_info = nullptr;
  AddReplicaLATask *my_task = reinterpret_cast<AddReplicaLATask *>(this_task);
  if (OB_UNLIKELY(nullptr == this_task || nullptr == my_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(this_task), KP(my_task));
  } else if (OB_FAIL(dr_ls_info_.get_ls_status_info(ls_status_info))) {
    LOG_WARN("fail to get log stream status info", KR(ret));
  } else if (OB_UNLIKELY(nullptr == ls_status_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls status info ptr is null", KR(ret), KP(ls_status_info));
  } else {
    found = false;
    share::ObUnitInfo unit_info;
    const common::ObZone &zone = my_task->zone_;
    int tmp_ret = unit_provider.allocate_unit(zone, ls_status_info->unit_group_id_, unit_info);
    if (OB_ITER_END == tmp_ret) {
      // bypass
    } else if (OB_SUCCESS == tmp_ret) {
      my_task->dst_server_ = unit_info.unit_.server_;
      my_task->unit_id_ = unit_info.unit_.unit_id_;
      my_task->unit_group_id_ = ls_status_info->unit_group_id_;
      if (ObReplicaTypeCheck::is_paxos_replica_V2(my_task->replica_type_)) {
        int64_t new_paxos_replica_number = 0;
        if (OB_FAIL(ObDRWorker::generate_disaster_recovery_paxos_replica_number(
                dr_ls_info_,
                curr_paxos_replica_number_,
                locality_paxos_replica_number_,
                MEMBER_CHANGE_ADD,
                new_paxos_replica_number,
                found))) {
          LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found));
        } else if (!found) {
          // bypass
        } else {
          my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
          my_task->paxos_replica_number_ = new_paxos_replica_number;
          output_task = my_task;
          found = true;
        }
      } else {
        my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
        my_task->paxos_replica_number_ = curr_paxos_replica_number_;
        output_task = my_task;
        found = true;
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get unit", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_review_type_transform_task(
    UnitProvider &unit_provider,
    LATask *this_task,
    const LATask *&output_task,
    bool &found)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_provider);
  TypeTransformLATask *my_task = reinterpret_cast<TypeTransformLATask *>(this_task);
  found = false;
  if (OB_UNLIKELY(nullptr == this_task || nullptr == my_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(this_task), KP(my_task));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(my_task->src_replica_type_)
             && ObReplicaTypeCheck::is_paxos_replica_V2(my_task->dst_replica_type_)) {
    my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
    my_task->paxos_replica_number_ = curr_paxos_replica_number_;
    output_task = my_task;
    found = true;
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(my_task->src_replica_type_)
             && !ObReplicaTypeCheck::is_paxos_replica_V2(my_task->dst_replica_type_)) {
    int64_t new_paxos_replica_number = 0;
    if (OB_FAIL(ObDRWorker::generate_disaster_recovery_paxos_replica_number(
            dr_ls_info_,
            curr_paxos_replica_number_,
            locality_paxos_replica_number_,
            MEMBER_CHANGE_SUB,
            new_paxos_replica_number,
            found))) {
      LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found));
    } else if (!found) {
      // bypass
    } else {
      my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
      my_task->paxos_replica_number_ = new_paxos_replica_number;
      output_task = my_task;
      found = true;
    }
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(my_task->src_replica_type_)
             && ObReplicaTypeCheck::is_paxos_replica_V2(my_task->dst_replica_type_)) {
    int64_t new_paxos_replica_number = 0;
    if (OB_FAIL(ObDRWorker::generate_disaster_recovery_paxos_replica_number(
            dr_ls_info_,
            curr_paxos_replica_number_,
            locality_paxos_replica_number_,
            MEMBER_CHANGE_ADD,
            new_paxos_replica_number,
            found))) {
      LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found));
    } else if (!found) {
      // bypass
    } else {
      my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
      my_task->paxos_replica_number_ = new_paxos_replica_number;
      output_task = my_task;
      found = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type transform task", KR(ret), KPC(this_task));
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_review_modify_paxos_replica_number_task(
    UnitProvider &unit_provider,
    LATask *this_task,
    const LATask *&output_task,
    bool &found)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_provider);
  ModifyPaxosReplicaNumberLATask *my_task = reinterpret_cast<ModifyPaxosReplicaNumberLATask *>(this_task);
  if (OB_UNLIKELY(nullptr == this_task || nullptr == my_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(this_task), KP(my_task));
  } else {
    int64_t new_paxos_replica_number = 0;
    if (OB_FAIL(ObDRWorker::generate_disaster_recovery_paxos_replica_number(
            dr_ls_info_,
            curr_paxos_replica_number_,
            locality_paxos_replica_number_,
            MEMBER_CHANGE_NOP,
            new_paxos_replica_number,
            found))) {
      LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found));
    } else if (!found) {
      // bypass
    } else {
      my_task->orig_paxos_replica_number_ = curr_paxos_replica_number_;
      my_task->paxos_replica_number_ = new_paxos_replica_number;
      output_task = my_task;
      found = true;
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_get_normal_locality_alignment_task(
    UnitProvider &unit_provider,
    const LATask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  bool found = false;
  int64_t index = 0;
  for (index = task_idx_; !found && OB_SUCC(ret) && index < task_array_.count(); ++index) {
    LATask *this_task = task_array_.at(index);
    if (OB_ISNULL(this_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this task ptr is null", KR(ret));
    } else {
      switch (this_task->get_task_type()) {
      case RemovePaxos:
      case RemoveNonPaxos:
        if (OB_FAIL(try_review_remove_replica_task(
                unit_provider,
                this_task,
                task,
                found))) {
          LOG_WARN("fail to try review remove replica task", KR(ret), KPC(this_task), K(found));
        } else {
          LOG_INFO("success to try review remove replica task", KR(ret), KPC(this_task), K(found));
        }
        break;
      case AddReplica:
        if (OB_FAIL(try_review_add_replica_task(
                unit_provider,
                this_task,
                task,
                found))) {
          LOG_WARN("fail to try review add replica task", KR(ret), KPC(this_task), K(found));
        } else {
          LOG_INFO("success to try review add replica task", KR(ret), KPC(this_task), K(found));
        }
        break;
      case TypeTransform:
        if (OB_FAIL(try_review_type_transform_task(
                unit_provider,
                this_task,
                task,
                found))) {
          LOG_WARN("fail to try review type transform task", KR(ret), KPC(this_task), K(found));
        } else {
          LOG_INFO("success to try review type transform task", KR(ret), KPC(this_task), K(found));
        }
        break;
      case ModifyPaxosReplicaNumber:
        if (OB_FAIL(try_review_modify_paxos_replica_number_task(
                unit_provider,
                this_task,
                task,
                found))) {
          LOG_WARN("fail to try review modify paxos replica number task", KR(ret), KPC(this_task), K(found));
        } else {
          LOG_INFO("success to try review modify paxos replica number task", KR(ret), KPC(this_task), K(found));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task type unexpected", KR(ret), KPC(this_task));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    task_idx_ = index;
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_get_readonly_all_server_locality_alignment_task(
    UnitProvider &unit_provider,
    const LATask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  for (LocalityMap::iterator iter = locality_map_.begin();
       OB_SUCC(ret) && iter != locality_map_.end();
       ++iter) {
    const common::ObZone &zone = iter->first;
    ReplicaDescArray *replica_desc_array = iter->second;
    const share::ObLSStatusInfo *ls_status_info = nullptr;
    if (OB_UNLIKELY(nullptr == replica_desc_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica desc array ptr is null", KR(ret));
    } else if (!replica_desc_array->is_readonly_all_server_) {
      // bypass
    } else if (OB_FAIL(dr_ls_info_.get_ls_status_info(ls_status_info))) {
      LOG_WARN("fail to get log stream info", KR(ret));
    } else if (OB_UNLIKELY(nullptr == ls_status_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls status info ptr is null", KR(ret), KP(ls_status_info));
    } else {
      share::ObUnitInfo unit_info;
      int tmp_ret = unit_provider.allocate_unit(zone, ls_status_info->unit_group_id_, unit_info);
      if (OB_ITER_END == tmp_ret) {
        // bypass
      } else if (OB_SUCCESS == tmp_ret) {
        add_replica_task_.zone_ = zone;
        add_replica_task_.dst_server_ = unit_info.unit_.server_;
        add_replica_task_.member_time_us_ = ObTimeUtility::current_time();
        add_replica_task_.unit_id_ = unit_info.unit_.unit_id_;
        add_replica_task_.unit_group_id_ = ls_status_info->unit_group_id_;
        add_replica_task_.replica_type_ = REPLICA_TYPE_READONLY;
        add_replica_task_.memstore_percent_ = replica_desc_array->readonly_memstore_percent_;
        add_replica_task_.orig_paxos_replica_number_ = curr_paxos_replica_number_;
        add_replica_task_.paxos_replica_number_ = curr_paxos_replica_number_;
        task = &add_replica_task_;
        break;
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to get unit", KR(ret), K(zone));
      }
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::get_next_locality_alignment_task(
    const LATask *&task)
{
  int ret = OB_SUCCESS;
  LATaskCmp cmp_operator(task_array_);
  if (OB_FAIL(cmp_operator.execute_sort())) {
    LOG_WARN("fail to sort task", KR(ret));
  } else {
    if (OB_FAIL(try_get_normal_locality_alignment_task(
            unit_provider_,
            task))) {
      LOG_WARN("fail to get normal locality alignment task", KR(ret));
    } else if (nullptr != task) {
      // got one
      LOG_INFO("success to get a normal task", KPC(task));
    } else if (OB_FAIL(try_get_readonly_all_server_locality_alignment_task(
            unit_provider_,
            task))) {
      LOG_WARN("fail to get readonly all server locality alignment task", KR(ret));
    } else if (nullptr == task) {
      ret = OB_ITER_END;
    } else {
      LOG_INFO("success to get a readonly all server task", KPC(task));
    }
  }
  return ret;
}

int ObDRWorker::UnitProvider::init(
    const uint64_t tenant_id,
    DRLSInfo &dr_ls_info,
    ObUnitManager *unit_mgr)
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || nullptr == unit_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(tenant_id),
             KP(unit_mgr));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("failed to get replica count", KR(ret));
  } else {
    const int64_t hash_count = max(replica_cnt, 1);
    if (OB_FAIL(unit_set_.create(hash::cal_next_prime(hash_count)))) {
      LOG_WARN("failed to create unit set", KR(ret), K(replica_cnt), K(hash_count));
    } else if (OB_FAIL(init_unit_set(dr_ls_info))) {
      LOG_WARN("failed to init unit set", KR(ret), K(dr_ls_info));
    } else {
      tenant_id_ = tenant_id;
      unit_mgr_ = unit_mgr;
      inited_ = true;
    }
  }
  return ret;
}

int ObDRWorker::UnitProvider::init_unit_set(
     DRLSInfo &dr_ls_info)
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              index,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_UNLIKELY(nullptr == ls_replica
                             || nullptr == server_stat_info
                             || nullptr == unit_stat_info
                             || nullptr == unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected", KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
      } else if ((ObReplicaTypeCheck::is_paxos_replica_V2(ls_replica->get_replica_type())
                  && ls_replica->get_in_member_list())
          || (!ObReplicaTypeCheck::is_paxos_replica_V2(ls_replica->get_replica_type()))) {
        if (OB_FAIL(unit_set_.set_refactored(unit_stat_info->get_unit_info().unit_.unit_id_))) {
          LOG_WARN("fail to set refactored", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::UnitProvider::inner_get_valid_unit_(
    const common::ObZone &zone,
    const common::ObArray<share::ObUnitInfo> &unit_array,
    share::ObUnitInfo &output_unit_info,
    const bool &force_get,
    bool &found)
{
  int ret = OB_SUCCESS;
  output_unit_info.reset();
  found = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(unit_mgr_) || OB_UNLIKELY(0 >= unit_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit mgr ptr is null", KR(ret), KP(unit_mgr_), "unit_count", unit_array.count());
  } else {
    bool server_is_active = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_array.count(); ++i) {
      server_is_active = false;
      const share::ObUnitInfo &unit_info = unit_array.at(i);
      const uint64_t unit_id = unit_info.unit_.unit_id_;
      int hash_ret = OB_SUCCESS;
      bool server_and_unit_status_is_valid = true;
      if (unit_info.unit_.zone_ != zone) {
        // bypass, because we do not support operation between different zones
      } else {
        if (!force_get) {
          if (OB_FAIL(SVR_TRACER.check_server_active(unit_info.unit_.server_, server_is_active))) {
            LOG_WARN("fail to check server active", KR(ret), "server", unit_info.unit_.server_);
          } else if (!server_is_active) {
            server_and_unit_status_is_valid = false;
            FLOG_INFO("server is not active", "server", unit_info.unit_.server_, K(server_is_active));
          } else if (!unit_info.unit_.is_active_status()) {
            server_and_unit_status_is_valid = false;
            FLOG_INFO("unit status is not normal", K(unit_info));
          } else {
            server_and_unit_status_is_valid = true;
          }
        }

        if (OB_FAIL(ret) || !server_and_unit_status_is_valid) {
        } else if (OB_HASH_EXIST == (hash_ret = unit_set_.set_refactored(unit_id, 0))) {
          FLOG_INFO("unit existed", K(unit_id));
        } else if (OB_FAIL(hash_ret)) {
          LOG_WARN("set refactored failed", KR(ret), KR(hash_ret));
        } else if (OB_FAIL(output_unit_info.assign(unit_info))) {
          LOG_WARN("fail to assign unit info", KR(ret), K(unit_info));
        } else {
          found = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::UnitProvider::allocate_unit(
    const common::ObZone &zone,
    const uint64_t unit_group_id,
    share::ObUnitInfo &unit_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    common::ObArray<ObUnitInfo> unit_array;
    bool found = false;
    bool force_get = true; // if unit_group_id is given, just allocate unit belongs to this unit group
    // 1. if unit_group_id is valid, try get valid unit in this unit group
    if (unit_group_id > 0) {
      force_get = true;
      if (OB_FAIL(unit_mgr_->get_unit_group(tenant_id_, unit_group_id, unit_array))) {
        LOG_WARN("fail to get unit group", KR(ret), K(tenant_id_), K(unit_group_id));
      } else if (OB_FAIL(inner_get_valid_unit_(zone, unit_array, unit_info, force_get, found))) {
        LOG_WARN("fail to get valid unit from certain unit group", KR(ret), K(zone), K(unit_array), K(force_get));
      }
    } else {
      // 2. if unit_group_id = 0, try get from all units
      unit_array.reset();
      force_get = false;
      if (OB_FAIL(unit_mgr_->get_all_unit_infos_by_tenant(tenant_id_, unit_array))) {
        LOG_WARN("fail to get ll unit infos by tenant", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(inner_get_valid_unit_(zone, unit_array, unit_info, force_get, found))) {
        LOG_WARN("fail to get valid unit from all units in tenant", KR(ret), K(zone), K(unit_array), K(force_get));
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ITER_END;
      LOG_WARN("fail to get valid unit", KR(ret), K(zone), K(found));
    }
  }
  return ret;
}

bool ObDRWorker::LATaskCmp::operator()(const LATask *left, const LATask *right)
{
  bool bool_ret = true;
  if (OB_SUCCESS != ret_) {
    // bypass
  } else if (nullptr == left || nullptr == right) {
    ret_ = OB_ERR_UNEXPECTED;
  } else {
    bool_ret = left->get_task_priority() < right->get_task_priority();
  }
  return bool_ret;
}

int ObDRWorker::LATaskCmp::execute_sort()
{
  std::sort(task_array_.begin(), task_array_.end(), *this);
  return ret_;
}

ObDRWorker::ObDRWorker(volatile bool &stop)
  : stop_(stop),
    inited_(false),
    dr_task_mgr_is_loaded_(false),
    self_addr_(),
    config_(nullptr),
    unit_mgr_(nullptr),
    zone_mgr_(nullptr),
    disaster_recovery_task_mgr_(nullptr),
    lst_operator_(nullptr),
    schema_service_(nullptr),
    rpc_proxy_(nullptr),
    sql_proxy_(nullptr),
    task_count_statistic_(),
    display_tasks_(),
    display_tasks_rwlock_(ObLatchIds::DISPLAY_TASKS_LOCK)
{
}

ObDRWorker::~ObDRWorker()
{
}

int ObDRWorker::init(
    common::ObAddr &self_addr,
    common::ObServerConfig &config,
    ObUnitManager &unit_mgr,
    ObZoneManager &zone_mgr,
    ObDRTaskMgr &task_mgr,
    share::ObLSTableOperator &lst_operator,
    share::schema::ObMultiVersionSchemaService &schema_service,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(self_addr));
  } else {
    self_addr_ = self_addr;
    config_ = &config;
    unit_mgr_ = &unit_mgr;
    zone_mgr_ = &zone_mgr;
    disaster_recovery_task_mgr_ = &task_mgr;
    lst_operator_ = &lst_operator;
    schema_service_ = &schema_service;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    display_tasks_.reset();

    inited_ = true;
  }
  return ret;
}

int ObDRWorker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDRWorker not init", KR(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("ObDRWorker stopped", KR(ret), K(stop_));
  }
  return ret;
}

int ObDRWorker::start()
{
  int ret = OB_SUCCESS;
  task_count_statistic_.reset();
  display_tasks_.reset();
  return ret;
}

void ObDRWorker::statistic_remain_dr_task()
{
  int tmp_ret = OB_SUCCESS;
  int64_t high_wait_cnt = 0;
  int64_t high_schedule_cnt = 0;
  int64_t low_wait_cnt = 0;
  int64_t low_schedule_cnt = 0;
  if (OB_ISNULL(disaster_recovery_task_mgr_)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(tmp_ret, "disaster_recovery_task_mgr_ is null", KR(tmp_ret));
  } else if (FALSE_IT(dr_task_mgr_is_loaded_ = disaster_recovery_task_mgr_->is_loaded())) {
    LOG_INFO("disaster_recovery_task_mgr_ is not loaded yet");
  } else if (!dr_task_mgr_is_loaded_) {
    LOG_INFO("disaster_recovery_task_mgr_ is not loaded yet");
  } else if (OB_SUCCESS != (tmp_ret = disaster_recovery_task_mgr_->get_all_task_count(
          high_wait_cnt,
          high_schedule_cnt,
          low_wait_cnt,
          low_schedule_cnt))) {
    LOG_WARN_RET(tmp_ret, "fail to get all task count", KR(tmp_ret));
  } else if (-1 != task_count_statistic_.remain_task_cnt_
             || 0 != high_wait_cnt + high_schedule_cnt + low_wait_cnt + low_schedule_cnt) {
    // remain_task_cnt_ == -1 means it is the initial stat
    // do not change remain_task_cnt while remain_task_cnt_ = -1 and others_cnt sums up to 0
    task_count_statistic_.set_remain_task_cnt(high_wait_cnt
                                              + high_schedule_cnt
                                              + low_wait_cnt
                                              + low_schedule_cnt);
    LOG_INFO("success to update statistics", K(high_wait_cnt), K(high_schedule_cnt),
             K(low_wait_cnt), K(low_schedule_cnt), K(task_count_statistic_));
  }
}

void ObDRWorker::statistic_total_dr_task(const int64_t task_cnt)
{
  if (!dr_task_mgr_is_loaded_) {
    LOG_INFO("disaster_recovery_task_mgr_ is not loaded yet");
  } else {
    task_count_statistic_.accumulate_task(task_cnt);
    if (task_count_statistic_.get_remain_task_cnt() < 0
        && task_cnt > 0) {
      LOG_INFO("disaster recovery start");
      ROOTSERVICE_EVENT_ADD("disaster_recovery", "disaster_recovery_start",
                            "start_time", ObTimeUtility::current_time());
      task_count_statistic_.set_remain_task_cnt(task_cnt);
    } else if (0 == task_count_statistic_.get_remain_task_cnt()
        && 0 == task_cnt) {
      LOG_INFO("disaster recovery finish",
              "total_task_one_round",
              task_count_statistic_.get_total_task_one_round());
      ROOTSERVICE_EVENT_ADD("disaster_recovery", "disaster_recovery_finish",
                            "finish_time", ObTimeUtility::current_time());
      task_count_statistic_.reset();
    }
  }
}

int ObDRWorker::check_tenant_locality_match(
    const uint64_t tenant_id,
    ObUnitManager &unit_mgr,
    ObZoneManager &zone_mgr,
    bool &locality_is_matched)
{
  int ret = OB_SUCCESS;
  locality_is_matched = true;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(nullptr == GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst operator ptr is null", KR(ret));
  } else {
    LOG_INFO("start try check tenant locality match", K(tenant_id));
    share::ObLSStatusOperator ls_status_operator;
    common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
    if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(tenant_id, ls_status_info_array, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to get all ls status", KR(ret), K(tenant_id));
    } else if (ls_status_info_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_status_info_array has no member", KR(ret), K(ls_status_info_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count() && locality_is_matched; ++i) {
        share::ObLSInfo ls_info;
        share::ObLSStatusInfo &ls_status_info = ls_status_info_array.at(i);
        bool filter_readonly_replicas_with_flag = true;
        DRLSInfo dr_ls_info(gen_user_tenant_id(tenant_id),
                            &unit_mgr,
                            &zone_mgr,
                            GCTX.schema_service_);
        if (ls_status_info.ls_is_creating()) {
          locality_is_matched = false;
        } else if (OB_FAIL(GCTX.lst_operator_->get(
                GCONF.cluster_id,
                ls_status_info.tenant_id_,
                ls_status_info.ls_id_,
                share::ObLSTable::COMPOSITE_MODE,
                ls_info))) {
          LOG_WARN("fail to get log stream info", KR(ret));
        } else if (OB_FAIL(dr_ls_info.init())) {
          LOG_WARN("fail to init dr log stream info", KR(ret));
        } else if (OB_FAIL(dr_ls_info.build_disaster_ls_info(
                               ls_info,
                               ls_status_info,
                               filter_readonly_replicas_with_flag))) {
          LOG_WARN("fail to generate dr log stream info", KR(ret), K(ls_info),
                   K(ls_status_info), K(filter_readonly_replicas_with_flag));
        } else if (OB_FAIL(check_ls_locality_match_(
                dr_ls_info, unit_mgr, zone_mgr, locality_is_matched))) {
          LOG_WARN("fail to try log stream disaster recovery", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::check_ls_locality_match_(
    DRLSInfo &dr_ls_info,
    ObUnitManager &unit_mgr,
    ObZoneManager &zone_mgr,
    bool &locality_is_matched)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  locality_is_matched = false;
  LOG_INFO("start to check ls locality match", K(dr_ls_info));
  LocalityAlignment locality_alignment(&unit_mgr,
                                       &zone_mgr,
                                       dr_ls_info);
  if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(locality_alignment.build())) {
    LOG_WARN("fail to build locality alignment", KR(ret));
  } else if (0 != locality_alignment.get_task_array_cnt()) {
    locality_is_matched = false;
  } else {
    locality_is_matched = true;
  }
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("the locality matched check for this logstream", KR(ret), K(locality_is_matched),
           K(dr_ls_info), "task_cnt", locality_alignment.get_task_array_cnt());
  return ret;
}

int ObDRWorker::try_disaster_recovery()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TRY_DISASTER_RECOVERY);
  ObCurTraceId::init(GCONF.self_addr_);
  ObArray<uint64_t> tenant_id_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObDRWorker stopped", KR(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_id_array))) {
    LOG_WARN("fail to get tenant id array", KR(ret));
  } else {
    LOG_INFO("start try disaster recovery");
    statistic_remain_dr_task();

    int64_t acc_dr_task = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_id_array.count(); ++i) {
      int64_t tenant_acc_dr_task = 0;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("DRWorker stopped", KR(ret));
      } else {
        const uint64_t tenant_id = tenant_id_array.at(i);
        const bool only_for_display = false;
        int tmp_ret = try_tenant_disaster_recovery(tenant_id, only_for_display, tenant_acc_dr_task);
        if (OB_SUCCESS == tmp_ret) {
          LOG_INFO("try tenant disaster recovery succ", K(tenant_id), K(only_for_display));
        } else {
          LOG_WARN("try tenant disaster recovery failed", KR(tmp_ret), K(tenant_id), K(only_for_display));
        }
      }
      acc_dr_task += tenant_acc_dr_task;
    }
    statistic_total_dr_task(acc_dr_task);
  }
  return ret;
}

int ObDRWorker::try_tenant_disaster_recovery(
    const uint64_t tenant_id,
    const bool only_for_display,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(lst_operator_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst operator ptr or sql proxy is null", KR(ret),
        KP(lst_operator_), KP(sql_proxy_));
  } else {
    LOG_INFO("start try tenant disaster recovery", K(tenant_id), K(only_for_display));
    share::ObLSStatusOperator ls_status_operator;
    common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
    if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(tenant_id,
            ls_status_info_array, *sql_proxy_))) {
      LOG_WARN("fail to get all ls status", KR(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
        share::ObLSInfo ls_info;
        share::ObLSStatusInfo &ls_status_info = ls_status_info_array.at(i);
        // this structure is used to generate migrtion/locality alignment/shrink unit tasks
        DRLSInfo dr_ls_info_without_flag(gen_user_tenant_id(tenant_id),
                                         unit_mgr_,
                                         zone_mgr_,
                                         schema_service_);
        // this structure is used to generate permanent offline tasks
        DRLSInfo dr_ls_info_with_flag(gen_user_tenant_id(tenant_id),
                                      unit_mgr_,
                                      zone_mgr_,
                                      schema_service_);
        int64_t ls_acc_dr_task = 0;
        int tmp_ret = OB_SUCCESS; // ignore ret for different ls
        LOG_INFO("start try ls disaster recovery", K(ls_status_info));
        if (OB_FAIL(check_stop())) {
          LOG_WARN("DRWorker stopped", KR(ret));
        } else {
          if (ls_status_info.ls_is_creating()) {
            // by pass, ls in creating
          } else if (OB_SUCCESS != (tmp_ret = lst_operator_->get(
                  GCONF.cluster_id,
                  ls_status_info.tenant_id_,
                  ls_status_info.ls_id_,
                  share::ObLSTable::COMPOSITE_MODE,
                  ls_info))) {
            LOG_WARN("fail to get log stream info", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = dr_ls_info_without_flag.init())) {
            LOG_WARN("fail to init dr log stream info", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = dr_ls_info_without_flag.build_disaster_ls_info(
                  ls_info, ls_status_info, true/*filter_readonly_replica_with_flag*/))) {
            LOG_WARN("fail to generate dr log stream info", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = dr_ls_info_with_flag.init())) {
            LOG_WARN("fail to init dr log stream info with flag", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = dr_ls_info_with_flag.build_disaster_ls_info(
                  ls_info, ls_status_info, false/*filter_readonly_replica_with_flag*/))) {
            LOG_WARN("fail to generate dr log stream info with flag", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = try_ls_disaster_recovery(
                  only_for_display, dr_ls_info_without_flag, ls_acc_dr_task, dr_ls_info_with_flag))) {
            LOG_WARN("fail to try log stream disaster recovery", KR(tmp_ret), K(only_for_display));
          }
        }
        acc_dr_task += ls_acc_dr_task;
      }
    }
  }
  return ret;
}

int ObDRWorker::try_assign_unit(
    DRLSInfo &dr_ls_info)
{
  int ret = OB_SUCCESS;
  /* TODO: (wenduo)
   * try assign unit_id/unit_group_id for replicas with invalid unit_id/unit_group_id
   */
  UNUSED(dr_ls_info);
  return ret;
}

int ObDRWorker::try_ls_disaster_recovery(
    const bool only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task_cnt,
    DRLSInfo &dr_ls_info_with_flag)
{
  int ret = OB_SUCCESS;
  ObRootBalanceHelp::BalanceController controller;
  ObString switch_config_str;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("DRWorker not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config ptr is null", KR(ret), KP(config_));
  } else if (FALSE_IT(switch_config_str = config_->__balance_controller.str())) {
    // shall never be here
  } else if (OB_FAIL(ObRootBalanceHelp::parse_balance_info(switch_config_str, controller))) {
    LOG_WARN("fail to parse balance info", KR(ret), K(switch_config_str));
  }

  // step0: set acc_dr_task_cnt
  acc_dr_task_cnt = 0;
  // step1: kick out the replica which is in some certain permanent-offline server
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    if (OB_FAIL(try_remove_permanent_offline_replicas(
            only_for_display, dr_ls_info_with_flag, acc_dr_task_cnt))) {
      LOG_WARN("fail to try remove permanent offline replicas",
               KR(ret), K(dr_ls_info_with_flag));
    }
  }
  // ATTENTION!!!
  // If this log stream has replicas only in member list, we need to have the
  // ability to let these replicas permanent offline. Because these replicas
  // may never be reported anymore(server is down) and disaster recovery module
  // regards these replicas as abnormal replicas and can not generate any task
  // for this log stream(locality alignment, replica migration etc.).
  // So we make sure log stream does not have replicas only in member_list AFTER try_remove_permanent_offline
  //
  // Also we DO NOT want to see replicas created during migration or rebuilding.
  // So we have to make sure those replicas with flag in learner_list not exists.
  // Please DO NOT change the order of try_remove_permanent_offline, filter_learner_with_flag, check_ls_only_in_member_list_or_with_flag_ and other operations.
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    bool is_only_in_member_list = true;
    if (OB_FAIL(check_ls_only_in_member_list_or_with_flag_(dr_ls_info))) {
      LOG_WARN("only_in_memberlist and flag replica check is failed", KR(ret), K(dr_ls_info));
    }
  }
  // step2: replicate to unit
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    if (OB_FAIL(try_replicate_to_unit(
            only_for_display, dr_ls_info, acc_dr_task_cnt))) {
      LOG_WARN("fail to try replicate to unit",
               KR(ret), K(dr_ls_info));
    }
  }
  // step3: locality alignment
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    if (OB_FAIL(try_locality_alignment(
            only_for_display, dr_ls_info, acc_dr_task_cnt))) {
      LOG_WARN("fail to try locality alignment",
               KR(ret), K(dr_ls_info));
    }
  }
  // step4: shrink resource pools
  // not supported now
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    if (OB_FAIL(try_shrink_resource_pools(
            only_for_display, dr_ls_info, acc_dr_task_cnt))) {
      LOG_WARN("fail to try shrink resource pools",
               KR(ret), K(dr_ls_info));
    }
  }
  // step5: cancel migrate unit
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    if (OB_FAIL(try_cancel_unit_migration(
            only_for_display, dr_ls_info, acc_dr_task_cnt))) {
      LOG_WARN("fail to try cancel unit migration",
               KR(ret), K(dr_ls_info));
    }
  }
  // step6: migrate to unit
  if (OB_SUCC(ret)
      && acc_dr_task_cnt <= 0) {
    if (OB_FAIL(try_migrate_to_unit(
            only_for_display, dr_ls_info, acc_dr_task_cnt))) {
      LOG_WARN("fail to try migrate to unit",
               KR(ret), K(dr_ls_info));
    }
  }
  return ret;
}

int ObDRWorker::generate_task_key(
    const DRLSInfo &dr_ls_info,
    ObDRTaskKey &task_key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    uint64_t tenant_id = OB_INVALID_ID;
    ObLSID ls_id;
    if (OB_FAIL(dr_ls_info.get_ls_id(
            tenant_id, ls_id))) {
      LOG_WARN("fail to get log stream id", KR(ret));
    } else if (OB_FAIL(task_key.init(
            tenant_id,
            ls_id.id(),
            0/* set to 0 */,
            0/* set to 0 */,
            ObDRTaskKeyType::FORMAL_DR_KEY))) {
      LOG_WARN("fail to init task key", KR(ret), K(tenant_id), K(ls_id));
    }
  }
  return ret;
}

int ObDRWorker::check_has_leader_while_remove_replica(
    const common::ObAddr &server,
    DRLSInfo &dr_ls_info,
    bool &has_leader)
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret), K(dr_ls_info));
  } else {
    int64_t full_replica_count = 0;
    int64_t paxos_replica_num = 0;
    int64_t arb_replica_num = 0;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObLSID ls_id;
    ObReplicaType replica_type = REPLICA_TYPE_MAX;
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              index,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected", KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
      } else if (REPLICA_STATUS_NORMAL != ls_replica->get_replica_status()) {
        // bypass
      } else {
        if (server_stat_info->get_server() == server) {
          replica_type = ls_replica->get_replica_type();
        }
        if (ObReplicaTypeCheck::is_paxos_replica_V2(ls_replica->get_replica_type())) {
          ++paxos_replica_num;
        }
        if (REPLICA_TYPE_FULL == ls_replica->get_replica_type()) {
          ++full_replica_count;
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
      LOG_WARN("fail to get tenant and ls id", KR(ret), K(dr_ls_info));
    } else if (OB_FAIL(ObShareUtil::generate_arb_replica_num(
                           tenant_id,
                           ls_id,
                           arb_replica_num))) {
      LOG_WARN("fail to generate arb replica number", KR(ret), K(tenant_id), K(ls_id));
    } else if (!dr_ls_info.has_leader()) {
      has_leader = false;
    } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
      has_leader = true;
    } else {
      has_leader = true;
      if (REPLICA_TYPE_FULL == replica_type) {
        has_leader = full_replica_count >= 2;
      }
      if (has_leader) {
        has_leader = (paxos_replica_num - 1 + arb_replica_num) >= majority(dr_ls_info.get_schema_replica_cnt());
      }
    }
  }
  return ret;
}

int ObDRWorker::check_task_already_exist(
    const ObDRTaskKey &task_key,
    const DRLSInfo &dr_ls_info,
    const int64_t &priority,
    bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = true;
  bool sibling_task_executing = false;
  ObDRTaskPriority task_priority = priority == 0 ? ObDRTaskPriority::HIGH_PRI : ObDRTaskPriority::LOW_PRI;
  ObDRTaskPriority sibling_priority = priority == 0
                                      ? ObDRTaskPriority::LOW_PRI
                                      : ObDRTaskPriority::HIGH_PRI;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(disaster_recovery_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disaster recovery task mgr ptr is null", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->check_task_exist(
          task_key, task_priority, task_exist))) {
    LOG_WARN("fail to check task exist", KR(ret), K(task_key));
  } else if (task_exist) {
    FLOG_INFO("high prio task exist for this ls", K(task_key));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->check_task_in_executing(
          task_key, sibling_priority, sibling_task_executing))) {
    LOG_WARN("fail to check task in executing", KR(ret), K(task_key));
  } else if (sibling_task_executing) {
    task_exist = true;
    FLOG_INFO("has sibling task in executing for this ls", K(task_key));
  }
  return ret;
}

int ObDRWorker::check_can_generate_task(
    const int64_t acc_dr_task,
    const bool need_check_has_leader_while_remove_replica,
    const bool is_high_priority_task,
    const ObAddr &server_addr,
    DRLSInfo &dr_ls_info,
    ObDRTaskKey &task_key,
    bool &can_generate)
{
  int ret = OB_SUCCESS;
  bool task_exist = false;
  bool has_leader_while_remove_replica = false;
  int64_t task_pri = is_high_priority_task ? 0 : 1;
  can_generate = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (acc_dr_task != 0) {
    can_generate = false;
    LOG_INFO("can not generate task because another task is generated", K(dr_ls_info), K(is_high_priority_task));
  } else if (OB_FAIL(generate_task_key(dr_ls_info, task_key))) {
    LOG_WARN("fail to generate task key", KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(check_task_already_exist(task_key, dr_ls_info, task_pri, task_exist))) {
    LOG_WARN("task exist in task manager", KR(ret), K(task_key), K(dr_ls_info), K(is_high_priority_task));
  } else if (task_exist) {
    can_generate = false;
    LOG_INFO("can not generate task because already exist", K(dr_ls_info), K(is_high_priority_task));
  } else if (need_check_has_leader_while_remove_replica) {
    if (OB_FAIL(check_has_leader_while_remove_replica(
                         server_addr,
                         dr_ls_info,
                         has_leader_while_remove_replica))) {
      LOG_WARN("fail to check has leader while member change", KR(ret), K(dr_ls_info), K(server_addr));
    } else if (has_leader_while_remove_replica) {
      can_generate = true;
    } else {
      can_generate = false;
      LOG_INFO("can not generate task because has no leader while remove replica", K(dr_ls_info), K(is_high_priority_task));
    }
  } else {
    can_generate = true;
  }
  return ret;
}

int ObDRWorker::construct_extra_infos_to_build_remove_replica_task(
    const DRLSInfo &dr_ls_info,
    share::ObTaskId &task_id,
    int64_t &new_paxos_replica_number,
    int64_t &old_paxos_replica_number,
    common::ObAddr &leader_addr,
    const ObReplicaType &replica_type)
{
  int ret = OB_SUCCESS;
  bool found_new_paxos_replica_number = false;
  bool is_paxos_replica = ObReplicaTypeCheck::is_paxos_replica_V2(replica_type);
  if (FALSE_IT(task_id.init(self_addr_))) {
  } else if (is_paxos_replica
             && OB_FAIL(generate_disaster_recovery_paxos_replica_number(
                         dr_ls_info,
                         dr_ls_info.get_paxos_replica_number(),
                         dr_ls_info.get_schema_replica_cnt(),
                         MEMBER_CHANGE_SUB,
                         new_paxos_replica_number,
                         found_new_paxos_replica_number))) {
    LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found_new_paxos_replica_number));
  } else if (is_paxos_replica && !found_new_paxos_replica_number) {
    LOG_WARN("paxos_replica_number not found", K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_leader(leader_addr))) {
    LOG_WARN("fail to get leader", KR(ret));
  } else {
    old_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
    new_paxos_replica_number = is_paxos_replica ? new_paxos_replica_number : old_paxos_replica_number;
  }
  return ret;
}

int ObDRWorker::generate_remove_permanent_offline_replicas_and_push_into_task_manager(
    const ObDRTaskKey task_key,
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const common::ObAddr &leader_addr,
    const ObReplicaMember &remove_member,
    const int64_t &old_paxos_replica_number,
    const int64_t &new_paxos_replica_number,
    int64_t &acc_dr_task,
    const ObReplicaType &replica_type)
{
  int ret = OB_SUCCESS;
  ObRemoveLSReplicaTask remove_replica_task;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(remove_replica_task.build(
                task_key,
                tenant_id,
                ls_id,
                task_id,
                0,/*schdule_time*/
                0,/*generate_time*/
                GCONF.cluster_id,
                0/*transmit_data_size*/,
                obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                false/*skip change member list*/,
                ObDRTaskPriority::HIGH_PRI,
                ObString(drtask::REMOVE_PERMANENT_OFFLINE_REPLICA),
                leader_addr,
                remove_member,
                old_paxos_replica_number,
                new_paxos_replica_number,
                replica_type))) {
    LOG_WARN("fail to build remove member task", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(leader_addr),
             K(remove_member), K(old_paxos_replica_number), K(new_paxos_replica_number), K(replica_type));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_replica_task))) {
    LOG_WARN("fail to add task", KR(ret), K(remove_replica_task));
  } else {
    acc_dr_task++;
  }
  return ret;
}

int ObDRWorker::try_remove_permanent_offline_replicas(
    const bool only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  common::ObMemberList member_list;
  GlobalLearnerList learner_list;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  share::ObLSID ls_id;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet", KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet", KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_leader_and_member_list(leader, member_list, learner_list))) {
    LOG_WARN("fail to get leader and member list", KR(ret), K(dr_ls_info));
  } else if (OB_UNLIKELY(0 >= member_list.get_member_number())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader member list is unexpected", KR(ret), K(dr_ls_info), K(leader), K(member_list));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret), K(tenant_id), K(ls_id), K(dr_ls_info));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < member_list.get_member_number(); ++index) {
      ObMember member_to_remove;
      common::ObReplicaType replica_type = REPLICA_TYPE_FULL;
      if (OB_FAIL(member_list.get_member_by_index(index, member_to_remove))) {
          LOG_WARN("fail to get member by index", KR(ret), K(index), K(member_list));
      } else if (OB_FAIL(do_single_replica_permanent_offline_(
                             tenant_id,
                             ls_id,
                             dr_ls_info,
                             only_for_display,
                             replica_type,
                             member_to_remove,
                             acc_dr_task))) {
        LOG_WARN("fail to do single replica permanent offline task", KR(ret), K(tenant_id), K(ls_id),
                 K(dr_ls_info), K(only_for_display), K(replica_type), K(member_to_remove), K(acc_dr_task));
      }
    }
    // try generate permanent offline task for readonly replicas
    for (int64_t index = 0; OB_SUCC(ret) && index < learner_list.get_member_number(); ++index) {
      ObMember learner_to_remove;
      common::ObReplicaType replica_type = REPLICA_TYPE_READONLY;
      if (OB_FAIL(learner_list.get_member_by_index(index, learner_to_remove))) {
        LOG_WARN("fail to get learner by index", KR(ret), K(index));
      } else if (OB_FAIL(do_single_replica_permanent_offline_(
                             tenant_id,
                             ls_id,
                             dr_ls_info,
                             only_for_display,
                             replica_type,
                             learner_to_remove,
                             acc_dr_task))) {
        LOG_WARN("fail to do single replica permanent offline task for readonly replica", KR(ret), K(tenant_id),
                 K(ls_id), K(dr_ls_info), K(only_for_display), K(replica_type), K(learner_to_remove), K(acc_dr_task));
      }
    }
  }
  FLOG_INFO("finish try remove permanent offline replica", KR(ret), K(tenant_id), K(ls_id), K(acc_dr_task));
  return ret;
}

int ObDRWorker::do_single_replica_permanent_offline_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    DRLSInfo &dr_ls_info,
    const bool only_for_display,
    const ObReplicaType &replica_type,
    const ObMember &member_to_remove,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  bool is_offline = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!member_to_remove.is_valid()
                         || OB_INVALID_TENANT_ID == tenant_id
                         || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_to_remove), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(SVR_TRACER.check_server_permanent_offline(member_to_remove.get_server(), is_offline))) {
    LOG_WARN("fail to check server permanent offline", KR(ret), K(member_to_remove));
  } else if (is_offline) {
    FLOG_INFO("found ls replica need to permanent offline", K(tenant_id), K(ls_id), K(member_to_remove), K(replica_type), K(dr_ls_info));
    share::ObTaskId task_id;
    int64_t new_paxos_replica_number = 0;
    int64_t old_paxos_replica_number = 0;
    common::ObAddr leader_addr;
    const common::ObAddr source_server; // not useful
    const bool need_check_has_leader_while_remove_replica = ObReplicaTypeCheck::is_paxos_replica_V2(replica_type);
    const bool is_high_priority_task = true;
    const int64_t memstore_percent = 100;
    ObDRTaskKey task_key;
    bool can_generate = false;
    ObReplicaMember remove_member(member_to_remove);
    //ObReplicaMember remove_member(member_to_remove.get_server(),
    //                              member_to_remove.get_timestamp(),
    //                              replica_type,
    //                              memstore_percent);
    ObDRTaskType task_type = ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)
                               ? ObDRTaskType::LS_REMOVE_PAXOS_REPLICA
                               : ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
    if (OB_FAIL(remove_member.set_replica_type(replica_type))) {
      LOG_WARN("fail to set replica type", KR(ret), K(replica_type), K(remove_member));
    } else if (OB_FAIL(construct_extra_infos_to_build_remove_replica_task(
                    dr_ls_info,
                    task_id,
                    new_paxos_replica_number,
                    old_paxos_replica_number,
                    leader_addr,
                    replica_type))) {
      LOG_WARN("fail to construct extra infos to build remove replica task");
    } else if (only_for_display) {
      // only for display, no need to execute this task
      ObLSReplicaTaskDisplayInfo display_info;
      if (OB_FAIL(display_info.init(
                      tenant_id,
                      ls_id,
                      task_type,
                      ObDRTaskPriority::HIGH_PRI,
                      member_to_remove.get_server(),
                      replica_type,
                      new_paxos_replica_number,
                      source_server,
                      REPLICA_TYPE_MAX/*source_replica_type*/,
                      old_paxos_replica_number,
                      leader_addr,
                      "remove permanent offline replica"))) {
        LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret), K(tenant_id), K(ls_id),
                 K(task_type), K(member_to_remove), K(replica_type), K(new_paxos_replica_number),
                 K(old_paxos_replica_number), K(leader_addr));
      } else if (OB_FAIL(add_display_info(display_info))) {
        LOG_WARN("fail to add display info", KR(ret), K(display_info));
      } else {
        LOG_INFO("success to add display info", KR(ret), K(display_info));
      }
    } else if (OB_FAIL(check_can_generate_task(
                           acc_dr_task,
                           need_check_has_leader_while_remove_replica,
                           is_high_priority_task,
                           member_to_remove.get_server(),
                           dr_ls_info,
                           task_key,
                           can_generate))) {
      LOG_WARN("fail to check can generate remove permanent offline task", KR(ret), K(acc_dr_task),
               K(need_check_has_leader_while_remove_replica), K(is_high_priority_task), K(member_to_remove),
               K(dr_ls_info), K(task_key), K(can_generate));
    } else if (can_generate) {
      if (OB_FAIL(generate_remove_permanent_offline_replicas_and_push_into_task_manager(
                          task_key,
                          tenant_id,
                          ls_id,
                          task_id,
                          leader_addr,
                          remove_member,
                          old_paxos_replica_number,
                          new_paxos_replica_number,
                          acc_dr_task,
                          replica_type))) {
        LOG_WARN("fail to generate remove permanent offline task", KR(ret), K(tenant_id), K(ls_id), K(leader_addr),
                 K(remove_member), K(old_paxos_replica_number), K(new_paxos_replica_number), K(replica_type));
      }
    }
  }
  return ret;
}

int ObDRWorker::check_need_generate_replicate_to_unit(
    const int64_t index,
    DRLSInfo &dr_ls_info,
    share::ObLSReplica *&ls_replica,
    DRServerStatInfo *&server_stat_info,
    DRUnitStatInfo *&unit_stat_info,
    DRUnitStatInfo *&unit_in_group_stat_info,
    bool &need_generate)
{
  int ret = OB_SUCCESS;
  need_generate = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(dr_ls_info.get_replica_stat(
              index,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
    LOG_WARN("fail to get replica stat", KR(ret));
  } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)
                 || (unit_stat_info->is_in_pool()
                     && OB_ISNULL(unit_stat_info->get_server_stat()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected", KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
  } else if (REPLICA_STATUS_NORMAL == ls_replica->get_replica_status()
             && unit_stat_info->is_in_pool()
             && !server_stat_info->is_alive()
             && unit_stat_info->get_unit_info().unit_.server_ != server_stat_info->get_server()
             && unit_stat_info->get_server_stat()->is_alive()
             && !unit_stat_info->get_server_stat()->is_block()) {
    need_generate = true;
    LOG_INFO("found replicate to unit replica", KPC(ls_replica));
  }
  return ret;
}

int ObDRWorker::construct_extra_infos_to_build_migrate_task(
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    const DRUnitStatInfo &unit_stat_info,
    const DRUnitStatInfo &unit_in_group_stat_info,
    const ObReplicaMember &dst_member,
    const ObReplicaMember &src_member,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    share::ObTaskId &task_id,
    ObReplicaMember &data_source,
    int64_t &data_size,
    ObDstReplica &dst_replica,
    bool &skip_change_member_list,
    int64_t &old_paxos_replica_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(task_id.init(self_addr_))) {
    //shall never be here
  } else if (OB_FAIL(choose_disaster_recovery_data_source(
                         zone_mgr_,
                         dr_ls_info,
                         dst_member,
                         src_member,
                         data_source,
                         data_size))) {
    LOG_WARN("fail to choose disaster recovery data source", KR(ret));
  } else if (OB_FAIL(dst_replica.assign(
                         unit_stat_info.get_unit_info().unit_.unit_id_,
                         unit_in_group_stat_info.get_unit_info().unit_.unit_group_id_,
                         ls_replica.get_zone(),
                         dst_member))) {
    LOG_WARN("fail to assign dst replica", KR(ret));
  } else if (OB_FAIL(ObDRTask::generate_skip_change_member_list(
                         ObDRTaskType::LS_MIGRATE_REPLICA,
                         ls_replica.get_replica_type(),
                         ls_replica.get_replica_type(),
                         skip_change_member_list))) {
    LOG_WARN("fail to generate skip change member list", KR(ret));
  } else {
    tenant_id = ls_replica.get_tenant_id();
    ls_id = ls_replica.get_ls_id();
    old_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
  }
  return ret;
}

int ObDRWorker::generate_replicate_to_unit_and_push_into_task_manager(
    const ObDRTaskKey task_key,
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const int64_t &data_size,
    const bool &skip_change_member_list,
    const ObDstReplica &dst_replica,
    const ObReplicaMember &src_member,
    const ObReplicaMember &data_source,
    const int64_t &old_paxos_replica_number,
    const char* task_comment,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObMigrateLSReplicaTask migrate_task;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(task_comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task comment is null", KR(ret));
  } else if (OB_FAIL(migrate_task.build(
                         task_key,
                         tenant_id,
                         ls_id,
                         task_id,
                         0,/*schedule_time*/
                         0,/*generate_time*/
                         GCONF.cluster_id,
                         data_size,
                         obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                         skip_change_member_list,
                         ObDRTaskPriority::HIGH_PRI,
                         task_comment,
                         dst_replica,
                         src_member,
                         data_source,
                         old_paxos_replica_number))) {
    LOG_WARN("fail to build migrate task", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(migrate_task))) {
    LOG_WARN("fail to add task", KR(ret), K(migrate_task));
  } else {
    ++acc_dr_task;
  }
  return ret;
}

int ObDRWorker::try_replicate_to_unit(
    const bool only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;

  ObDRTaskKey task_key;
  bool task_exist = false;
  int64_t replica_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet", KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet", KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret), K(dr_ls_info));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      bool need_generate = false;
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(check_need_generate_replicate_to_unit(
                      index,
                      dr_ls_info,
                      ls_replica,
                      server_stat_info,
                      unit_stat_info,
                      unit_in_group_stat_info,
                      need_generate))) {
        LOG_WARN("fail to check need generate replicate to unit task", KR(ret));
      } else if (need_generate) {
        ObReplicaMember dst_member(unit_stat_info->get_unit_info().unit_.server_,
                                   ObTimeUtility::current_time(),
                                   ls_replica->get_replica_type(),
                                   ls_replica->get_memstore_percent());
        if (OB_FAIL(generate_migrate_ls_task(
                only_for_display, drtask::REPLICATE_REPLICA, *ls_replica,
                *server_stat_info, *unit_stat_info, *unit_in_group_stat_info,
                dst_member, dr_ls_info, acc_dr_task))) {
          LOG_WARN("failed to generate migrate ls task", KR(ret), K(dst_member),
                   K(only_for_display), KPC(ls_replica));
        }
      }
    }
  }
  LOG_INFO("finish try replicate to unit", KR(ret), K(acc_dr_task));
  return ret;
}

int ObDRWorker::generate_migrate_ls_task(
      const bool only_for_display,
      const char* task_comment,
      const share::ObLSReplica &ls_replica,
      const DRServerStatInfo &server_stat_info,
      const DRUnitStatInfo &unit_stat_info,
      const DRUnitStatInfo &unit_in_group_stat_info,
      const ObReplicaMember &dst_member,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_replica.is_valid() || !dst_member.is_valid())
      || OB_ISNULL(task_comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(task_comment), K(ls_replica));
  } else {
    ObDRTaskKey task_key;
    bool task_exist = false;
    uint64_t tenant_id = 0;
    share::ObLSID ls_id;
    share::ObTaskId task_id;
    ObReplicaMember data_source;
    int64_t data_size = 0;
    bool skip_change_member_list = false;
    ObDstReplica dst_replica;
    int64_t old_paxos_replica_number = 0;
    const bool need_check_has_leader_while_remove_replica = false;
    const bool is_high_priority_task = true;
    bool can_generate = false;
    ObReplicaMember src_member(
        ls_replica.get_server(), ls_replica.get_member_time_us(),
        ls_replica.get_replica_type(), ls_replica.get_memstore_percent());
    if (OB_FAIL(construct_extra_infos_to_build_migrate_task(
            dr_ls_info, ls_replica, unit_stat_info, unit_in_group_stat_info,
            dst_member, src_member, tenant_id, ls_id, task_id, data_source,
            data_size, dst_replica, skip_change_member_list,
            old_paxos_replica_number))) {
      LOG_WARN("fail to construct extra infos to build migrate task", KR(ret));
    } else if (only_for_display) {
      ObLSReplicaTaskDisplayInfo display_info;
      if (OB_FAIL(display_info.init(
              tenant_id, ls_id, ObDRTaskType::LS_MIGRATE_REPLICA,
              ObDRTaskPriority::HIGH_PRI,
              unit_stat_info.get_unit_info().unit_.server_,
              ls_replica.get_replica_type(), old_paxos_replica_number,
              ls_replica.get_server(), ls_replica.get_replica_type(),
              old_paxos_replica_number,
              unit_stat_info.get_unit_info().unit_.server_,
              task_comment))) {
        LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret));
      } else if (OB_FAIL(add_display_info(display_info))) {
        LOG_WARN("fail to add display info", KR(ret), K(display_info));
      } else {
        LOG_INFO("success to add display info", KR(ret), K(display_info));
      }
    } else if (OB_FAIL(check_can_generate_task(
                   acc_dr_task, need_check_has_leader_while_remove_replica,
                   is_high_priority_task, server_stat_info.get_server(), dr_ls_info,
                   task_key, can_generate))) {
      LOG_WARN("fail to check can generate replicate to unit task", KR(ret));
    } else if (can_generate) {
      if (OB_FAIL(generate_replicate_to_unit_and_push_into_task_manager(
              task_key, tenant_id, ls_id, task_id, data_size,
              skip_change_member_list, dst_replica, src_member, data_source,
              old_paxos_replica_number, task_comment, acc_dr_task))) {
        LOG_WARN("fail to generate replicate to unit task", KR(ret));
      }
    }
  }
  return ret;
}

int ObDRWorker::try_generate_remove_replica_locality_alignment_task(
    DRLSInfo &dr_ls_info,
    const ObDRTaskKey &task_key,
    const LATask *task,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  bool task_exist = false;
  bool sibling_task_executing = false;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id;
  if (OB_UNLIKELY(!task_key.is_valid()) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key), KP(task));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret));
  } else {
    const RemoveReplicaLATask *my_task = reinterpret_cast<const RemoveReplicaLATask *>(task);
    ObReplicaMember remove_member(my_task->remove_server_,
                                  my_task->member_time_us_,
                                  my_task->replica_type_,
                                  my_task->memstore_percent_);
    ObRemoveLSReplicaTask remove_paxos_task;
    bool has_leader = false;
    common::ObAddr leader_addr;
    share::ObTaskId task_id;
    ObString comment_to_set = "";
    if (ObReplicaTypeCheck::is_paxos_replica_V2(my_task->replica_type_)) {
      comment_to_set.assign_ptr(drtask::REMOVE_LOCALITY_PAXOS_REPLICA,
                                strlen(drtask::REMOVE_LOCALITY_PAXOS_REPLICA));
    } else {
      comment_to_set.assign_ptr(drtask::REMOVE_LOCALITY_NON_PAXOS_REPLICA,
                                strlen(drtask::REMOVE_LOCALITY_NON_PAXOS_REPLICA));
    }

    if (FALSE_IT(task_id.init(self_addr_))) {
      //shall never be here
    } else if (OB_FAIL(check_has_leader_while_remove_replica(
            my_task->remove_server_,
            dr_ls_info,
            has_leader))) {
      LOG_WARN("fail to check has leader while member change", KR(ret), K(dr_ls_info));
    } else if (!has_leader) {
      LOG_INFO("may has no leader while member change", K(dr_ls_info));
    } else if (OB_FAIL(dr_ls_info.get_leader(leader_addr))) {
      LOG_WARN("fail to get leader", KR(ret));
    } else if (OB_FAIL(remove_paxos_task.build(
            task_key,
            tenant_id,
            ls_id,
            task_id,
            0,/*schedule_time*/
            0,/*generate_time*/
            GCONF.cluster_id,
            0,/*transmit data size*/
            obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
            false,/*skip change member list*/
            ObDRTaskPriority::HIGH_PRI,
            comment_to_set,
            leader_addr,
            remove_member,
            my_task->orig_paxos_replica_number_,
            my_task->paxos_replica_number_,
            my_task->replica_type_))) {
      LOG_WARN("fail to build task", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(task_id),
               K(leader_addr), K(remove_member), KPC(my_task));
    } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_paxos_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else {
      LOG_INFO("success to add a ObRemoveLSReplicaTask to task manager", KR(ret), K(remove_paxos_task));
      acc_dr_task++;
    }
  }
  return ret;
}

int ObDRWorker::try_generate_add_replica_locality_alignment_task(
    DRLSInfo &dr_ls_info,
    const ObDRTaskKey &task_key,
    const LATask *task,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  bool task_exist = false;
  bool sibling_task_executing = false;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id;
  if (OB_UNLIKELY(!task_key.is_valid() || nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key), KP(task));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret));
  } else {
    const AddReplicaLATask *my_task = reinterpret_cast<const AddReplicaLATask *>(task);
    int64_t data_size = 0;
    ObReplicaMember data_source;
    ObDstReplica dst_replica;
    ObReplicaMember dst_member(my_task->dst_server_,
                               my_task->member_time_us_,
                               my_task->replica_type_,
                               my_task->memstore_percent_);
    ObAddLSReplicaTask add_replica_task;
    share::ObTaskId task_id;
    ObString comment_to_set;
    if (ObReplicaTypeCheck::is_paxos_replica_V2(my_task->replica_type_)) {
      comment_to_set.assign_ptr(drtask::ADD_LOCALITY_PAXOS_REPLICA,
                                strlen(drtask::ADD_LOCALITY_PAXOS_REPLICA));
    } else {
      comment_to_set.assign_ptr(drtask::ADD_LOCALITY_NON_PAXOS_REPLICA,
                                strlen(drtask::ADD_LOCALITY_NON_PAXOS_REPLICA));
    }

    if (FALSE_IT(task_id.init(self_addr_))) {
      //shall never be here
    } else if (OB_FAIL(choose_disaster_recovery_data_source(
            zone_mgr_,
            dr_ls_info,
            dst_member,
            ObReplicaMember(),/*empty*/
            data_source,
            data_size))) {
      LOG_WARN("fail to choose disaster recovery data source", KR(ret));
    } else if (OB_FAIL(dst_replica.assign(
            my_task->unit_id_,
            my_task->unit_group_id_,
            my_task->zone_,
            dst_member))) {
      LOG_WARN("fail to assign dst replica", KR(ret));
    } else if (OB_FAIL(add_replica_task.build(
            task_key,
            tenant_id,
            ls_id,
            task_id,
            0,/*schedule_time*/
            0,/*generate_time*/
            GCONF.cluster_id,
            data_size,
            obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
            false,/*skip change member list*/
            ObDRTaskPriority::HIGH_PRI,
            comment_to_set,
            dst_replica,
            data_source,
            my_task->orig_paxos_replica_number_,
            my_task->paxos_replica_number_))) {
      LOG_WARN("fail to build add replica task", KR(ret));
    } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(add_replica_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else {
      LOG_INFO("success to add a ObAddLSReplicaTask to task manager", KR(ret), K(add_replica_task));
      acc_dr_task++;
    }
  }
  return ret;
}

int ObDRWorker::try_generate_type_transform_locality_alignment_task(
    DRLSInfo &dr_ls_info,
    const ObDRTaskKey &task_key,
    const LATask *task,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  bool task_exist = false;
  bool sibling_task_executing = false;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id;
  if (OB_UNLIKELY(!task_key.is_valid() || nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key), KP(task));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret));
  } else {
    const TypeTransformLATask *my_task = reinterpret_cast<const TypeTransformLATask *>(task);
    bool has_leader = false;
    int64_t data_size = 0;
    ObReplicaMember data_source;
    ObDstReplica dst_replica;
    bool skip_change_member_list = false;
    ObReplicaMember src_member(my_task->dst_server_,
                               my_task->src_member_time_us_,
                               my_task->src_replica_type_,
                               my_task->src_memstore_percent_);
    ObReplicaMember dst_member(my_task->dst_server_,
                               my_task->dst_member_time_us_,
                               my_task->dst_replica_type_,
                               my_task->dst_memstore_percent_);
    ObLSTypeTransformTask type_transform_task;
    share::ObTaskId task_id;
    if (FALSE_IT(task_id.init(self_addr_))) {
      //shall never be here
    } else if (OB_FAIL(check_has_leader_while_remove_replica(
            my_task->dst_server_,
            dr_ls_info,
            has_leader))) {
      LOG_WARN("fail to check has leader while member change", KR(ret), K(dr_ls_info));
    } else if (!has_leader) {
      LOG_INFO("may has no leader while member change", K(dr_ls_info));
    } else if (OB_FAIL(choose_disaster_recovery_data_source(
            zone_mgr_,
            dr_ls_info,
            dst_member,
            src_member,
            data_source,
            data_size))) {
      LOG_WARN("fail to choose disaster recovery data source", KR(ret));
    } else if (OB_FAIL(dst_replica.assign(
            my_task->unit_id_,
            my_task->unit_group_id_,
            my_task->zone_,
            dst_member))) {
      LOG_WARN("fail to assign dst replica", KR(ret));
    } else if (OB_FAIL(ObDRTask::generate_skip_change_member_list(
            ObDRTaskType::LS_TYPE_TRANSFORM,
            my_task->src_replica_type_,
            my_task->dst_replica_type_,
            skip_change_member_list))) {
      LOG_WARN("fail to generate skip change member list", KR(ret));
    } else if (OB_FAIL(type_transform_task.build(
            task_key,
            tenant_id,
            ls_id,
            task_id,
            0,/*schedule_time*/
            0,/*generate_time*/
            GCONF.cluster_id,
            data_size,
            obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
            false,/*skip change member list*/
            ObDRTaskPriority::HIGH_PRI,
            ObString(drtask::TRANSFORM_LOCALITY_REPLICA_TYPE),
            dst_replica,
            src_member,
            data_source,
            my_task->orig_paxos_replica_number_,
            my_task->paxos_replica_number_))) {
      LOG_WARN("fail to build type transform task", KR(ret));
    } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(type_transform_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else {
      LOG_INFO("success to add a ObLSTypeTransformTask to task manager", KR(ret), K(type_transform_task));
      acc_dr_task++;
    }
  }
  return ret;
}

int ObDRWorker::try_generate_modify_paxos_replica_number_locality_alignment_task(
    DRLSInfo &dr_ls_info,
    const ObDRTaskKey &task_key,
    const LATask *task,
    int64_t &acc_dr_task_cnt)
{
  int ret = OB_SUCCESS;
  bool task_exist = false;
  bool sibling_task_executing = false;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id;
  GlobalLearnerList learner_list;
  if (OB_UNLIKELY(!task_key.is_valid() || nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_key), KP(task));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret));
  } else {
    const ModifyPaxosReplicaNumberLATask *my_task = reinterpret_cast<const ModifyPaxosReplicaNumberLATask *>(task);
    common::ObAddr leader_addr;
    common::ObMemberList member_list;
    ObLSModifyPaxosReplicaNumberTask modify_paxos_replica_number_task;
    share::ObTaskId task_id;
    if (FALSE_IT(task_id.init(self_addr_))) {
      //shall never be here
    } else if (OB_FAIL(dr_ls_info.get_leader_and_member_list(
            leader_addr,
            member_list,
            learner_list))) {
      LOG_WARN("fail to get leader", KR(ret));
    } else if (OB_FAIL(modify_paxos_replica_number_task.build(
            task_key,
            tenant_id,
            ls_id,
            task_id,
            0,/*schedule_time*/
            0,/*generate_time*/
            GCONF.cluster_id,
            0,/*transmit data size*/
            obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
            true,/*skip change member list*/
            ObDRTaskPriority::HIGH_PRI,
            ObString(drtask::MODIFY_PAXOS_REPLICA_NUMBER),
            leader_addr,
            my_task->orig_paxos_replica_number_,
            my_task->paxos_replica_number_,
            member_list))) {
      LOG_WARN("fail to build a modify paxos replica number task", KR(ret));
    } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(modify_paxos_replica_number_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else {
      LOG_INFO("success to add a ObLSModifyPaxosReplicaNumberTask to task manager", KR(ret), K(modify_paxos_replica_number_task), K(member_list));
      acc_dr_task_cnt++;
    }
  }
  return ret;
}

int ObDRWorker::try_generate_locality_alignment_task(
    DRLSInfo &dr_ls_info,
    const LATask *task,
    int64_t &acc_dr_task_cnt)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(task));
  } else if (OB_FAIL(generate_task_key(dr_ls_info, task_key))) {
    LOG_WARN("fail to generate task key", KR(ret));
  } else {
    switch (task->get_task_type()) {
      case RemovePaxos:
      case RemoveNonPaxos: {
        if (OB_FAIL(try_generate_remove_replica_locality_alignment_task(
                dr_ls_info,
                task_key,
                task,
                acc_dr_task_cnt))) {
          LOG_WARN("fail to try generate remove replica task",
                    KR(ret), K(task_key), KPC(task));
        }
        break;
      }
      case AddReplica: {
        if (OB_FAIL(try_generate_add_replica_locality_alignment_task(
                dr_ls_info,
                task_key,
                task,
                acc_dr_task_cnt))) {
          LOG_WARN("fail to try generate add replica paxos task",
                    KR(ret), K(task_key), KPC(task));
        }
        break;
      }
      case TypeTransform: {
        if (OB_FAIL(try_generate_type_transform_locality_alignment_task(
                dr_ls_info,
                task_key,
                task,
                acc_dr_task_cnt))) {
          LOG_WARN("fail to try generate type transform paxos task",
                    KR(ret), K(task_key), KPC(task));
        }
        break;
      }
      case ModifyPaxosReplicaNumber: {
        if (OB_FAIL(try_generate_modify_paxos_replica_number_locality_alignment_task(
                dr_ls_info,
                task_key,
                task,
                acc_dr_task_cnt))) {
          LOG_WARN("fail to try generate modify paxos replica number task",
                    KR(ret), K(task_key), KPC(task));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task type", KR(ret), KPC(task));
        break;
      }
    }
  }
  return ret;
}

int ObDRWorker::record_task_plan_for_locality_alignment(
    DRLSInfo &dr_ls_info,
    const LATask *task)
{
  int ret = OB_SUCCESS;
  ObDRTaskType task_type = ObDRTaskType::MAX_TYPE;
  uint64_t tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id;
  ObReplicaType source_replica_type = REPLICA_TYPE_MAX;
  ObReplicaType target_replica_type = REPLICA_TYPE_MAX;
  ObDRTaskPriority task_priority = ObDRTaskPriority::MAX_PRI;
  common::ObAddr leader_addr;
  common::ObAddr source_svr;
  common::ObAddr target_svr;
  common::ObAddr execute_svr;
  int64_t source_replica_paxos_replica_number = OB_INVALID_COUNT;
  int64_t target_replica_paxos_replica_number = OB_INVALID_COUNT;
  int64_t data_size = 0;
  ObString comment = "";
  ObReplicaMember data_source;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is nullptr", KR(ret), KP(task));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret));
  } else if (OB_FAIL(dr_ls_info.get_leader(leader_addr))) {
    LOG_WARN("fail to get leader", KR(ret));
  } else {
    ObLSReplicaTaskDisplayInfo display_info;
    switch (task->get_task_type()) {
      case RemovePaxos:
      case RemoveNonPaxos: {
        const RemoveReplicaLATask *my_task = reinterpret_cast<const RemoveReplicaLATask *>(task);
        task_type = RemovePaxos == task->get_task_type() ? ObDRTaskType::LS_REMOVE_PAXOS_REPLICA : ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
        source_replica_type = REPLICA_TYPE_MAX;
        target_replica_type = my_task->replica_type_;
        task_priority = task_type == ObDRTaskType::LS_REMOVE_PAXOS_REPLICA ? ObDRTaskPriority::HIGH_PRI : ObDRTaskPriority::LOW_PRI;
        target_svr = my_task->remove_server_;
        execute_svr = leader_addr;
        source_replica_paxos_replica_number = my_task->orig_paxos_replica_number_;
        target_replica_paxos_replica_number = my_task->paxos_replica_number_;
        if (task_type == ObDRTaskType::LS_REMOVE_PAXOS_REPLICA) {
          comment.assign_ptr(drtask::REMOVE_LOCALITY_PAXOS_REPLICA, strlen(drtask::REMOVE_LOCALITY_PAXOS_REPLICA));
        } else {
          comment.assign_ptr(drtask::REMOVE_LOCALITY_NON_PAXOS_REPLICA, strlen(drtask::REMOVE_LOCALITY_NON_PAXOS_REPLICA));
        }
        break;
      }
      case AddReplica: {
        const AddReplicaLATask *my_task = reinterpret_cast<const AddReplicaLATask *>(task);
        ObReplicaMember dst_member(my_task->dst_server_,
                                   my_task->member_time_us_,
                                   my_task->replica_type_,
                                   my_task->memstore_percent_);
        if (OB_FAIL(choose_disaster_recovery_data_source(
            zone_mgr_,
            dr_ls_info,
            dst_member,
            ObReplicaMember(),/*empty*/
            data_source,
            data_size))) {
          LOG_WARN("fail to choose data source", KR(ret));
        } else {
          task_type = ObDRTaskType::LS_ADD_REPLICA;
          source_replica_type = data_source.get_replica_type();
          target_replica_type = my_task->replica_type_;
          task_priority = ObDRTaskPriority::HIGH_PRI;
          source_svr = data_source.get_server();
          target_svr = my_task->dst_server_;
          execute_svr = my_task->dst_server_;
          source_replica_paxos_replica_number = my_task->orig_paxos_replica_number_;
          target_replica_paxos_replica_number = my_task->paxos_replica_number_;
          if (ObReplicaTypeCheck::is_paxos_replica_V2(target_replica_type)) {
            comment.assign_ptr(drtask::ADD_LOCALITY_PAXOS_REPLICA, strlen(drtask::ADD_LOCALITY_PAXOS_REPLICA));
          } else {
            comment.assign_ptr(drtask::ADD_LOCALITY_NON_PAXOS_REPLICA, strlen(drtask::ADD_LOCALITY_NON_PAXOS_REPLICA));
          }
        }
        break;
      }
      case TypeTransform: {
        const TypeTransformLATask *my_task = reinterpret_cast<const TypeTransformLATask *>(task);
        ObReplicaMember src_member(my_task->dst_server_,
                                   my_task->src_member_time_us_,
                                   my_task->src_replica_type_,
                                   my_task->src_memstore_percent_);
        ObReplicaMember dst_member(my_task->dst_server_,
                                   my_task->dst_member_time_us_,
                                   my_task->dst_replica_type_,
                                   my_task->dst_memstore_percent_);
        if (OB_FAIL(choose_disaster_recovery_data_source(
            zone_mgr_,
            dr_ls_info,
            dst_member,
            ObReplicaMember(),/*empty*/
            data_source,
            data_size))) {
          LOG_WARN("fail to choose data source", KR(ret));
        } else {
          task_type = ObDRTaskType::LS_TYPE_TRANSFORM;
          source_replica_type = my_task->src_replica_type_;
          target_replica_type = my_task->dst_replica_type_;
          task_priority = ObDRTaskPriority::HIGH_PRI;
          source_svr = data_source.get_server();
          target_svr = my_task->dst_server_;
          execute_svr = my_task->dst_server_;
          source_replica_paxos_replica_number = my_task->orig_paxos_replica_number_;
          target_replica_paxos_replica_number = my_task->paxos_replica_number_;
          comment.assign_ptr(drtask::TRANSFORM_LOCALITY_REPLICA_TYPE, strlen(drtask::TRANSFORM_LOCALITY_REPLICA_TYPE));
        }
        break;
      }
      case ModifyPaxosReplicaNumber: {
        const ModifyPaxosReplicaNumberLATask *my_task = reinterpret_cast<const ModifyPaxosReplicaNumberLATask *>(task);
        task_type = ObDRTaskType::LS_MODIFY_PAXOS_REPLICA_NUMBER;
        source_replica_type = REPLICA_TYPE_FULL;
        target_replica_type = REPLICA_TYPE_FULL;
        task_priority = ObDRTaskPriority::HIGH_PRI;
        target_svr = leader_addr;
        execute_svr = leader_addr;
        source_replica_paxos_replica_number = my_task->orig_paxos_replica_number_;
        target_replica_paxos_replica_number = my_task->paxos_replica_number_;
        comment.assign_ptr(drtask::MODIFY_PAXOS_REPLICA_NUMBER, strlen(drtask::MODIFY_PAXOS_REPLICA_NUMBER));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task type", KR(ret), KPC(task));
        break;
      }
    }

    if (FAILEDx(display_info.init(
                    tenant_id,
                    ls_id,
                    task_type,
                    task_priority,
                    target_svr,
                    target_replica_type,
                    target_replica_paxos_replica_number,
                    source_svr,
                    source_replica_type,
                    source_replica_paxos_replica_number,
                    execute_svr,
                    comment))) {
      LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret), K(tenant_id), K(ls_id), K(task_type),
               K(task_priority), K(target_svr), K(target_replica_type), K(target_replica_paxos_replica_number),
               K(source_svr), K(source_replica_type), K(source_replica_paxos_replica_number), K(execute_svr), K(comment));
    } else if (OB_FAIL(add_display_info(display_info))) {
      FLOG_WARN("fail to add display info", KR(ret), K(display_info));
    } else {
      FLOG_INFO("success to add display info", KR(ret), K(display_info));
    }
  }
  return ret;
}

int ObDRWorker::try_locality_alignment(
    const bool only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TRY_LOCALITY_ALIGNMENT);
  LOG_INFO("try locality alignment", K(dr_ls_info), K(only_for_display));
  LocalityAlignment locality_alignment(unit_mgr_, zone_mgr_, dr_ls_info);
  const LATask *task = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", KR(ret), KP(config_));
  } else if (!config_->is_rereplication_enabled()) {
    // bypass
  } else if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(locality_alignment.build())) {
    LOG_WARN("fail to build locality alignment", KR(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(locality_alignment.get_next_locality_alignment_task(task))) {
      bool can_generate = false;
      const bool need_check_has_leader_while_remove_replica = false;
      const bool is_high_priority_task = task->get_task_type() != LATaskType::RemoveNonPaxos;
      ObAddr server_addr; //useless
      ObDRTaskKey task_key;
      if (OB_ISNULL(task)) {
        // bypass, there is no task to generate
      } else if (only_for_display) {
        if (OB_FAIL(record_task_plan_for_locality_alignment(dr_ls_info, task))) {
          LOG_WARN("fail to record task plan", KR(ret), KPC(task));
        } else {
          LOG_INFO("success to record task plan", KR(ret), KPC(task));
        }
      } else if (OB_FAIL(check_can_generate_task(
                               acc_dr_task,
                               need_check_has_leader_while_remove_replica,
                               is_high_priority_task,
                               server_addr,
                               dr_ls_info,
                               task_key,
                               can_generate))) {
        LOG_WARN("fail to check can generate locality alignment task", KR(ret));
      } else if (!can_generate) {
        //bypass
      } else if (OB_FAIL(try_generate_locality_alignment_task(
              dr_ls_info, task, acc_dr_task))) {
        LOG_WARN("fail to try generate locality alignment task", KR(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  // no need to print task key, since the previous log contains that
  LOG_INFO("finish try locality alignment", KR(ret), K(acc_dr_task));
  return ret;
}

int ObDRWorker::try_shrink_resource_pools(
    const bool &only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  const share::ObLSStatusInfo *ls_status_info = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", KR(ret), KP(config_));
  } else if (!config_->is_rereplication_enabled()) {
    // bypass
  } else if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_ls_status_info(ls_status_info))) {
    LOG_WARN("failed to get ls status info", KR(ret), K(dr_ls_info));
  } else if (OB_ISNULL(ls_status_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ls status info", KR(ret));
  } else {
    ObDRWorker::UnitProvider unit_provider;
    const uint64_t tenant_id = ls_status_info->tenant_id_;
    if (OB_FAIL(unit_provider.init(gen_user_tenant_id(tenant_id), dr_ls_info,
            unit_mgr_))) {
      LOG_WARN("fail to init unit provider", KR(ret), K(tenant_id), K(dr_ls_info));
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      bool need_generate = false;
      bool is_unit_in_group_related = false;
      if (OB_FAIL(dr_ls_info.get_replica_stat(
                         index,
                         ls_replica,
                         server_stat_info,
                         unit_stat_info,
                         unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret), K(index));
      } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)
                 || (unit_stat_info->is_in_pool() && OB_ISNULL(unit_stat_info->get_server_stat()))
                 || (unit_in_group_stat_info->is_in_pool() && OB_ISNULL(unit_in_group_stat_info->get_server_stat()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected",
                 KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
      } else if (REPLICA_STATUS_NORMAL == ls_replica->get_replica_status()
          && share::ObUnit::UNIT_STATUS_DELETING == unit_stat_info->get_unit_info().unit_.status_) {
        // replica is still in member_list, but unit is in DELETING status
        // If this is a duplicate log stream
        //   1.1 for R-replica: execute remove_learner task directly
        //   1.2 for F-replica: try to execute migrate-replica first,
        //                      if migrate-replica task can not generate then try to type_transform another R to F
        // If this is a normal log stream
        //   2.1 try to execute migrate-replica task for both R-replica and F-replica
        if (dr_ls_info.is_duplicate_ls()) {
          if (REPLICA_TYPE_READONLY == ls_replica->get_replica_type()) {
            // 1.1 try to generate and execute remove learner task
            if (OB_FAIL(try_remove_readonly_replica_for_deleting_unit_(
                            *ls_replica,
                            only_for_display,
                            dr_ls_info,
                            acc_dr_task))) {
              LOG_WARN("fail to try remove readonly replica for deleting unit", KPC(ls_replica),
                       K(only_for_display), K(dr_ls_info), K(acc_dr_task));
            }
          } else if (REPLICA_TYPE_FULL == ls_replica->get_replica_type()) {
            // 1.2 try to generate and execute migrate replica task
            int64_t previous_acc_dr_task = acc_dr_task; // to check whether migrate task generated
            bool migrate_task_generated = false;
            if (OB_FAIL(try_migrate_replica_for_deleting_unit_(
                            unit_provider,
                            dr_ls_info,
                            *ls_replica,
                            *ls_status_info,
                            *server_stat_info,
                            *unit_stat_info,
                            *unit_in_group_stat_info,
                            only_for_display,
                            acc_dr_task))) {
              LOG_WARN("fail to try migrate replica for deleting unit", KR(ret), K(dr_ls_info),
                       KPC(ls_replica), KPC(ls_status_info), KPC(server_stat_info),
                       KPC(unit_stat_info), KPC(unit_in_group_stat_info), K(only_for_display));
            } else if (FALSE_IT(migrate_task_generated = acc_dr_task != previous_acc_dr_task)) {
              // 1.2 A migrate task already generated, do nothing.
              //     If migrate-task not generated, try to do type transform
            } else if (OB_FAIL(try_type_transform_for_deleting_unit_(
                                   dr_ls_info,
                                   *ls_replica,
                                   only_for_display,
                                   acc_dr_task))) {
              LOG_WARN("fail to try type transform for deleting unit", KR(ret), K(dr_ls_info),
                       KPC(ls_replica), K(only_for_display));
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unexpected replica type", KR(ret), KPC(ls_replica));
          }
        } else {
          // generate task for normal log stream replica
          if (0 == ls_status_info->ls_group_id_
          || ls_status_info->unit_group_id_ != unit_stat_info->get_unit_info().unit_.unit_group_id_) {
            //If the Unit Group is in the DELETING status, we need to migrate out the LS that do not belong to that Unit Group.
            //LS belonging to this Unit Group will be automatically processed by Balance module
            // 2.1 try generate and execute migrate replica for normal log stream
            if (OB_FAIL(try_migrate_replica_for_deleting_unit_(
                            unit_provider,
                            dr_ls_info,
                            *ls_replica,
                            *ls_status_info,
                            *server_stat_info,
                            *unit_stat_info,
                            *unit_in_group_stat_info,
                            only_for_display,
                            acc_dr_task))) {
              LOG_WARN("fail to try migrate replica for deleting unit", KR(ret), K(dr_ls_info),
                       KPC(ls_replica), KPC(ls_status_info), KPC(server_stat_info),
                       KPC(unit_stat_info), KPC(unit_in_group_stat_info), K(only_for_display));
            }
          }
        }
      }
    }//end for each ls replica
  }
  LOG_INFO("finish try shrink resource pool", KR(ret), K(acc_dr_task));
  return ret;
}

int ObDRWorker::try_remove_readonly_replica_for_deleting_unit_(
    const share::ObLSReplica &ls_replica,
    const bool &only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_replica));
  } else {
    share::ObTaskId task_id;
    int64_t new_paxos_replica_number = 0;
    int64_t old_paxos_replica_number = 0;
    common::ObAddr leader_addr;
    const common::ObAddr source_server; // not useful
    const bool need_check_has_leader_while_remove_replica = false;
    const bool is_high_priority_task = true;
    const int64_t memstore_percent = 100;
    ObDRTaskKey task_key;
    bool can_generate = false;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    share::ObLSID ls_id;
    ObDRTaskType task_type = ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
    ObReplicaMember remove_learner(ls_replica.get_server(),
                                   ls_replica.get_member_time_us(),
                                   ls_replica.get_replica_type(),
                                   memstore_percent);
    if (OB_FAIL(construct_extra_infos_to_build_remove_replica_task(
                    dr_ls_info,
                    task_id,
                    new_paxos_replica_number,
                    old_paxos_replica_number,
                    leader_addr,
                    ls_replica.get_replica_type()))) {
              LOG_WARN("fail to construct extra infos to build remove replica task", KR(ret), K(dr_ls_info), K(ls_replica));
    } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
      LOG_WARN("fail to get ls id", KR(ret), K(dr_ls_info), K(tenant_id), K(ls_id));
    } else if (only_for_display) {
      // only for display, no need to execute this task
      ObLSReplicaTaskDisplayInfo display_info;
      if (OB_FAIL(display_info.init(
                      tenant_id,
                      ls_id,
                      task_type,
                      is_high_priority_task ? ObDRTaskPriority::HIGH_PRI : ObDRTaskPriority::LOW_PRI,
                      ls_replica.get_server(),
                      ls_replica.get_replica_type(),
                      new_paxos_replica_number,
                      source_server,
                      REPLICA_TYPE_MAX/*source_replica_type*/,
                      old_paxos_replica_number,
                      leader_addr,
                      "shrink unit task"))) {
        LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret), K(tenant_id), K(ls_id),
                 K(task_type), K(ls_replica), K(new_paxos_replica_number),
                 K(old_paxos_replica_number), K(leader_addr));
      } else if (OB_FAIL(add_display_info(display_info))) {
        LOG_WARN("fail to add display info", KR(ret), K(display_info));
      } else {
        LOG_INFO("success to add display info", KR(ret), K(display_info));
      }
    } else if (OB_FAIL(check_can_generate_task(
                           acc_dr_task,
                           need_check_has_leader_while_remove_replica,
                           is_high_priority_task,
                           ls_replica.get_server(),
                           dr_ls_info,
                           task_key,
                           can_generate))) {
      LOG_WARN("fail to check can generate remove permanent offline task", KR(ret), K(acc_dr_task),
               K(need_check_has_leader_while_remove_replica), K(is_high_priority_task), K(ls_replica),
               K(dr_ls_info), K(task_key), K(can_generate));
    } else if (can_generate) {
      ObRemoveLSReplicaTask remove_replica_task;
      if (OB_FAIL(remove_replica_task.build(
                      task_key,
                      tenant_id,
                      ls_id,
                      task_id,
                      0,/*schdule_time*/
                      0,/*generate_time*/
                      GCONF.cluster_id,
                      0/*transmit_data_size*/,
                      obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                      false/*skip change member list*/,
                      ObDRTaskPriority::HIGH_PRI,
                      "shrink unit task",
                      leader_addr,
                      remove_learner,
                      old_paxos_replica_number,
                      new_paxos_replica_number,
                      ls_replica.get_replica_type()))) {
        LOG_WARN("fail to build remove member task", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(leader_addr),
                 K(remove_learner), K(old_paxos_replica_number), K(new_paxos_replica_number));
      } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_replica_task))) {
        LOG_WARN("fail to add task", KR(ret), K(remove_replica_task));
      } else {
        acc_dr_task++;
      }
    }
  }
  return ret;
}

int ObDRWorker::try_migrate_replica_for_deleting_unit_(
    ObDRWorker::UnitProvider &unit_provider,
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    const share::ObLSStatusInfo &ls_status_info,
    const DRServerStatInfo &server_stat_info,
    const DRUnitStatInfo &unit_stat_info,
    const DRUnitStatInfo &unit_in_group_stat_info,
    const bool &only_for_display,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_replica.is_valid()
                         || !ls_status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_replica), K(ls_status_info));
  } else {
    share::ObUnitInfo dest_unit;
    if (OB_FAIL(unit_provider.allocate_unit(
                ls_replica.get_zone(),
                ls_status_info.unit_group_id_,
                dest_unit))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("can not allocate valid unit for this ls replica to do migration",
                 K(ls_replica), K(ls_status_info));
      } else {
        LOG_WARN("failed to allocate unit for this log stream", KR(ret),
                 K(ls_replica), K(ls_status_info));
      }
    } else {
      ObReplicaMember dst_member(
          dest_unit.unit_.server_,
          ObTimeUtility::current_time(),
          ls_replica.get_replica_type(),
          ls_replica.get_memstore_percent());
      if (OB_FAIL(generate_migrate_ls_task(
                      only_for_display, "shrink unit task", ls_replica,
                      server_stat_info, unit_stat_info,
                      unit_in_group_stat_info, dst_member, dr_ls_info,
                      acc_dr_task))) {
        LOG_WARN("failed to generate migrate ls task", KR(ret),
                 K(dst_member), K(only_for_display), K(ls_replica));
      }
    }
  }
  return ret;
}

int ObDRWorker::try_type_transform_for_deleting_unit_(
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    const bool &only_for_display,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObLSReplica target_replica;
  uint64_t target_unit_id = 0;
  uint64_t target_unit_group_id = 0;
  bool find_a_valid_readonly_replica = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(find_valid_readonly_replica_(
                         dr_ls_info,
                         ls_replica,
                         ls_replica.get_zone(),
                         target_replica,
                         target_unit_id,
                         target_unit_group_id,
                         find_a_valid_readonly_replica))) {
    LOG_WARN("fail to find a valid readonly replica", KR(ret), K(dr_ls_info), K(ls_replica));
  } else if (!find_a_valid_readonly_replica) {
    // do nothing, because no valid R-replica to do type transform
    LOG_INFO("no valid readonly replica found to do type transform task for deleting unit", K(dr_ls_info), K(ls_replica));
  } else {
    // try to generate type transform task
    share::ObTaskId task_id;
    ObDRTaskKey task_key;
    uint64_t tenant_id = 0;
    share::ObLSID ls_id;
    common::ObAddr leader_addr;
    int64_t old_paxos_replica_number = 0;
    int64_t new_paxos_replica_number = 0;
    ObReplicaMember data_source;
    int64_t data_size = 0;
    ObDstReplica dst_replica;
    bool can_generate  = false;
    ObReplicaMember src_member(target_replica.get_server(),
                               target_replica.get_member_time_us(),
                               target_replica.get_replica_type(),
                               target_replica.get_memstore_percent());
    ObReplicaMember dst_member(target_replica.get_server(),
                               target_replica.get_member_time_us(),
                               REPLICA_TYPE_FULL,
                               target_replica.get_memstore_percent());
    if (OB_FAIL(construct_extra_info_to_build_type_transform_task_(
                         dr_ls_info,
                         ls_replica,
                         dst_member,
                         src_member,
                         target_unit_id,
                         target_unit_group_id,
                         task_id,
                         tenant_id,
                         ls_id,
                         leader_addr,
                         data_source,
                         data_size,
                         dst_replica,
                         old_paxos_replica_number,
                         new_paxos_replica_number))) {
      LOG_WARN("fail to construct extra info to build a type transform task", KR(ret),
               K(dr_ls_info), K(ls_replica), K(dst_member), K(src_member),
               K(target_unit_id), K(target_unit_group_id));
    } else if (only_for_display) {
      ObLSReplicaTaskDisplayInfo display_info;
      if (OB_FAIL(display_info.init(
                      tenant_id,
                      ls_id,
                      ObDRTaskType::LS_TYPE_TRANSFORM,
                      ObDRTaskPriority::HIGH_PRI,
                      target_replica.get_server(),
                      REPLICA_TYPE_FULL/*target_replica_type*/,
                      new_paxos_replica_number,
                      data_source.get_server(),
                      REPLICA_TYPE_READONLY/*source_replica_type*/,
                      old_paxos_replica_number,
                      leader_addr,
                      "shrink unit number"))) {
        LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret), K(tenant_id),
                 K(ls_id), K(target_replica), K(new_paxos_replica_number), K(data_source),
                 K(old_paxos_replica_number), K(leader_addr));
      } else if (OB_FAIL(add_display_info(display_info))) {
        LOG_WARN("fail to add display info", KR(ret), K(display_info));
      } else {
        LOG_INFO("success to add display info", KR(ret), K(display_info));
      }
    } else if (OB_FAIL(check_can_generate_task(
                           acc_dr_task,
                           false/*need_check_has_leader_while_remove_replica*/,
                           true/*is_high_priority_task*/,
                           target_replica.get_server(),
                           dr_ls_info,
                           task_key,
                           can_generate))) {
      LOG_WARN("fail to check whether can generate task", KR(ret), K(acc_dr_task),
               K(target_replica), K(dr_ls_info));
    } else if (!can_generate) {
      LOG_INFO("can not generate type transform task");
    } else if (OB_FAIL(generate_type_transform_task_(
                           task_key,
                           tenant_id,
                           ls_id,
                           task_id,
                           data_size,
                           dst_replica,
                           src_member,
                           data_source,
                           old_paxos_replica_number,
                           new_paxos_replica_number,
                           acc_dr_task))) {
      LOG_WARN("fail to generate type transform task", KR(ret), K(task_key),
               K(tenant_id), K(ls_id), K(task_id), K(data_size), K(dst_replica),
               K(src_member), K(data_source), K(old_paxos_replica_number),
               K(new_paxos_replica_number), K(acc_dr_task));
    }
  }
  return ret;
}

int ObDRWorker::find_valid_readonly_replica_(
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &exclude_replica,
    const ObZone &target_zone,
    share::ObLSReplica &target_replica,
    uint64_t &unit_id,
    uint64_t &unit_group_id,
    bool &find_a_valid_readonly_replica)
{
  int ret = OB_SUCCESS;
  find_a_valid_readonly_replica = false;
  int64_t replica_cnt = 0;
  target_replica.reset();
  unit_id = 0;
  unit_group_id = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica count", KR(ret), K(dr_ls_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_cnt; i++) {
      share::ObLSReplica *replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              i,
              replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica related ptrs are null", KR(ret),
                 KP(replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
      } else if (target_zone != replica->get_zone()
                 || common::REPLICA_TYPE_READONLY != replica->get_replica_type()
                 || exclude_replica.get_server() == replica->get_server()) {
        // bypass
      } else if (replica->is_in_service()
                 && server_stat_info->is_alive()
                 && !server_stat_info->is_stopped()
                 && !replica->get_restore_status().is_restore_failed()
                 && unit_stat_info->get_server_stat()->is_alive()
                 && !unit_stat_info->get_server_stat()->is_block()) {
        if (OB_FAIL(target_replica.assign(*replica))) {
          LOG_WARN("fail to assign replica", KR(ret), KPC(replica));
        } else {
          unit_id = unit_stat_info->get_unit_info().unit_.unit_id_;
          unit_group_id = unit_stat_info->get_unit_info().unit_.unit_group_id_;
          find_a_valid_readonly_replica = true;
          LOG_INFO("find a valid readonly replica to do type transform", K(dr_ls_info),
                   K(exclude_replica), K(target_zone), K(target_replica));
          break;
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::construct_extra_info_to_build_type_transform_task_(
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    const ObReplicaMember &dst_member,
    const ObReplicaMember &src_member,
    const uint64_t &target_unit_id,
    const uint64_t &target_unit_group_id,
    share::ObTaskId &task_id,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    common::ObAddr &leader_addr,
    ObReplicaMember &data_source,
    int64_t &data_size,
    ObDstReplica &dst_replica,
    int64_t &old_paxos_replica_number,
    int64_t &new_paxos_replica_number)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  ls_id = OB_INVALID_ID;
  leader_addr.reset();
  dst_replica.reset();
  data_size = 0;
  old_paxos_replica_number = 0;
  new_paxos_replica_number = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_replica));
  } else if (FALSE_IT(task_id.init(self_addr_))) {
    //shall never be here
  } else if (OB_FAIL(dr_ls_info.get_leader(leader_addr))) {
    LOG_WARN("fail to get leader address", KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
    LOG_WARN("fail to get tenant and ls id", KR(ret), K(dr_ls_info), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(choose_disaster_recovery_data_source(
                         zone_mgr_,
                         dr_ls_info,
                         dst_member,
                         src_member,
                         data_source,
                         data_size))) {
    LOG_WARN("fail to choose disaster recovery data source", KR(ret), K(dr_ls_info),
             K(dst_member), K(src_member));
  } else if (OB_FAIL(dst_replica.assign(
                         target_unit_id,
                         target_unit_group_id,
                         ls_replica.get_zone(),
                         dst_member))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(target_unit_id), K(target_unit_group_id),
             K(ls_replica), K(dst_member));
  } else {
    old_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
    new_paxos_replica_number = dr_ls_info.get_paxos_replica_number() + 1;
  }
  return ret;
}

int ObDRWorker::generate_type_transform_task_(
    const ObDRTaskKey &task_key,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const int64_t data_size,
    const ObDstReplica &dst_replica,
    const ObReplicaMember &src_member,
    const ObReplicaMember &data_source,
    const int64_t old_paxos_replica_number,
    const int64_t new_paxos_replica_number,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObLSTypeTransformTask type_transform_task;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_FAIL(type_transform_task.build(
                 task_key,
                 tenant_id,
                 ls_id,
                 task_id,
                 0,/*schedule_time*/
                 0,/*generate_time*/
                 GCONF.cluster_id,
                 data_size,
                 obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                 false,/*skip change member list*/
                 ObDRTaskPriority::HIGH_PRI,
                 "shrink unit number",
                 dst_replica,
                 src_member,
                 data_source,
                 old_paxos_replica_number,
                 new_paxos_replica_number))) {
    LOG_WARN("fail to build type transform task", KR(ret), K(task_key), K(tenant_id), K(ls_id),
             K(data_size), K(dst_replica), K(src_member), K(data_source), K(old_paxos_replica_number),
             K(new_paxos_replica_number));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(type_transform_task))) {
    LOG_WARN("fail to add task", KR(ret), K(type_transform_task));
  } else {
    LOG_INFO("success to add a ObLSTypeTransformTask to task manager", KR(ret), K(type_transform_task));
    acc_dr_task++;
  }
  return ret;
}

int ObDRWorker::check_need_generate_cancel_unit_migration_task(
    const int64_t index,
    DRLSInfo &dr_ls_info,
    share::ObLSReplica *&ls_replica,
    DRServerStatInfo *&server_stat_info,
    DRUnitStatInfo *&unit_stat_info,
    DRUnitStatInfo *&unit_in_group_stat_info,
    bool &is_paxos_replica_related,
    bool &need_generate)
{
  int ret = OB_SUCCESS;
  need_generate = false;
  is_paxos_replica_related = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(dr_ls_info.get_replica_stat(
              index,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
    LOG_WARN("fail to get replica stat", KR(ret));
  } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)
                 || (unit_stat_info->is_in_pool()
                     && OB_ISNULL(unit_stat_info->get_server_stat()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected", KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
  } else if (unit_stat_info->is_in_pool()
          && unit_stat_info->get_unit_info().unit_.migrate_from_server_.is_valid()
          && unit_stat_info->get_server_stat()->is_block()
          && ls_replica->get_server() == unit_stat_info->get_server_stat()->get_server()) {
    if (ObReplicaTypeCheck::is_paxos_replica_V2(ls_replica->get_replica_type())
        && ((dr_ls_info.get_member_list_cnt() - 1) >= dr_ls_info.get_schema_replica_cnt())) {
      need_generate = true;
      is_paxos_replica_related = true;
    } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(ls_replica->get_replica_type())) {
      need_generate = true;
      is_paxos_replica_related = false;
    }
  }
  return ret;
}

int ObDRWorker::construct_extra_info_to_build_cancael_migration_task(
    const bool &is_paxos_replica_related,
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    share::ObTaskId &task_id,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    common::ObAddr &leader_addr,
    int64_t &old_paxos_replica_number,
    int64_t &new_paxos_replica_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(task_id.init(self_addr_))) {
    //shall never be here
  } else if (is_paxos_replica_related) {
    if (OB_FAIL(dr_ls_info.get_leader(leader_addr))) {
      LOG_WARN("fail to get leader address", KR(ret));
    } else {
      tenant_id = ls_replica.get_tenant_id();
      ls_id = ls_replica.get_ls_id();
      old_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
      new_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
    }
  } else {
    tenant_id = ls_replica.get_tenant_id();
    ls_id = ls_replica.get_ls_id();
  }
  return ret;
}

int ObDRWorker::generate_cancel_unit_migration_task(
    const bool &is_paxos_replica_related,
    const ObDRTaskKey &task_key,
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const common::ObAddr &leader_addr,
    const ObReplicaMember &remove_member,
    const int64_t &old_paxos_replica_number,
    const int64_t &new_paxos_replica_number,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObRemoveLSReplicaTask remove_member_task;
  ObString comment_to_set = "";
  ObReplicaType replica_type = is_paxos_replica_related ? REPLICA_TYPE_FULL : REPLICA_TYPE_READONLY;
  if (is_paxos_replica_related) {
    comment_to_set.assign_ptr(drtask::CANCEL_MIGRATE_UNIT_WITH_PAXOS_REPLICA,
                              strlen(drtask::CANCEL_MIGRATE_UNIT_WITH_PAXOS_REPLICA));
  } else {
    comment_to_set.assign_ptr(drtask::CANCEL_MIGRATE_UNIT_WITH_NON_PAXOS_REPLICA,
                              strlen(drtask::CANCEL_MIGRATE_UNIT_WITH_NON_PAXOS_REPLICA));
  }

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(remove_member_task.build(
                  task_key,
                  tenant_id,
                  ls_id,
                  task_id,
                  0,/*schedule_time*/
                  0,/*generate_time*/
                  GCONF.cluster_id,
                  0/*transmit_data_size*/,
                  obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                  false/*skip change member list*/,
                  ObDRTaskPriority::HIGH_PRI,
                  comment_to_set,
                  leader_addr,
                  remove_member,
                  old_paxos_replica_number,
                  new_paxos_replica_number,
                  replica_type))) {
    LOG_WARN("fail to build remove member task", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_member_task))) {
    LOG_WARN("fail to add task", KR(ret), K(remove_member_task));
  } else {
    ++acc_dr_task;
  }
  return ret;
}

int ObDRWorker::try_cancel_unit_migration(
    const bool only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;

  ObDRTaskKey task_key;
  bool task_exist = false;
  bool sibling_task_executing = false;
  int64_t replica_cnt = 0;
  DEBUG_SYNC(BEFORE_TRY_MIGRATE_UNIT);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", KR(ret), KP(config_));
  } else if (!config_->is_rereplication_enabled()) {
    // bypass
  } else if (OB_FAIL(generate_task_key(dr_ls_info, task_key))) {
    LOG_WARN("fail to generate task key", KR(ret));
  } else if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret), K(dr_ls_info));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      bool is_paxos_replica_related = false;
      bool need_generate = false;
      if (OB_FAIL(check_need_generate_cancel_unit_migration_task(
                      index,
                      dr_ls_info,
                      ls_replica,
                      server_stat_info,
                      unit_stat_info,
                      unit_in_group_stat_info,
                      is_paxos_replica_related,
                      need_generate))) {
        LOG_WARN("fail to check need generate cancel unit migration task", KR(ret));
      } else if (need_generate) {
        share::ObTaskId task_id;
        uint64_t tenant_id = 0;
        share::ObLSID ls_id;
        common::ObAddr leader_addr;
        int64_t old_paxos_replica_number = 0;
        int64_t new_paxos_replica_number = 0;
        bool can_generate = false;
        common::ObAddr source_svr; // not useful
        const bool need_check_has_leader_while_remove_replica = true;
        ObReplicaMember remove_member(ls_replica->get_server(),
                                      ls_replica->get_member_time_us(),
                                      ls_replica->get_replica_type(),
                                      ls_replica->get_memstore_percent());
        ObDRTaskType task_type = is_paxos_replica_related
                               ? ObDRTaskType::LS_REMOVE_PAXOS_REPLICA
                               : ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
        ObDRTaskPriority task_priority = is_paxos_replica_related
                                       ? ObDRTaskPriority::HIGH_PRI
                                       : ObDRTaskPriority::LOW_PRI;
        ObString comment_to_set = "";
        if (is_paxos_replica_related) {
          comment_to_set.assign_ptr(drtask::CANCEL_MIGRATE_UNIT_WITH_PAXOS_REPLICA,
                                    strlen(drtask::CANCEL_MIGRATE_UNIT_WITH_PAXOS_REPLICA));
        } else {
          comment_to_set.assign_ptr(drtask::CANCEL_MIGRATE_UNIT_WITH_NON_PAXOS_REPLICA,
                                    strlen(drtask::CANCEL_MIGRATE_UNIT_WITH_NON_PAXOS_REPLICA));
        }

        if (OB_FAIL(construct_extra_info_to_build_cancael_migration_task(
                        is_paxos_replica_related,
                        dr_ls_info,
                        *ls_replica,
                        task_id,
                        tenant_id,
                        ls_id,
                        leader_addr,
                        old_paxos_replica_number,
                        new_paxos_replica_number))) {
          LOG_WARN("fail to construct extra info to build cancel unit migration task", KR(ret));
        } else if (only_for_display) {
          ObLSReplicaTaskDisplayInfo display_info;
          if (OB_FAIL(display_info.init(
                        tenant_id,
                        ls_id,
                        task_type,
                        task_priority,
                        ls_replica->get_server(),
                        ls_replica->get_replica_type(),
                        new_paxos_replica_number,
                        source_svr,
                        REPLICA_TYPE_MAX,
                        old_paxos_replica_number,
                        leader_addr,
                        comment_to_set))) {
            LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret));
          } else if (OB_FAIL(add_display_info(display_info))) {
            LOG_WARN("fail to add display info", KR(ret), K(display_info));
          } else {
            LOG_INFO("success to add display info", KR(ret), K(display_info));
          }
        } else if (OB_FAIL(check_can_generate_task(
                               acc_dr_task,
                               need_check_has_leader_while_remove_replica,
                               is_paxos_replica_related,
                               ls_replica->get_server(),
                               dr_ls_info,
                               task_key,
                               can_generate))) {
          LOG_WARN("fail to check can generate cancel unit migration task", KR(ret));
        } else if (can_generate) {
          if (OB_FAIL(generate_cancel_unit_migration_task(
                          is_paxos_replica_related,
                          task_key,
                          tenant_id,
                          ls_id,
                          task_id,
                          leader_addr,
                          remove_member,
                          old_paxos_replica_number,
                          new_paxos_replica_number,
                          acc_dr_task))) {
            LOG_WARN("fail to build cancel unit migration task", KR(ret));
          }
        }
      }
    }
  }
  // no need to print task key, since the previous log contains that
  LOG_INFO("finish try cancel migrate unit", KR(ret), K(acc_dr_task));
  return ret;
}

int ObDRWorker::check_need_generate_migrate_to_unit_task(
    const int64_t index,
    DRLSInfo &dr_ls_info,
    share::ObLSReplica *&ls_replica,
    DRServerStatInfo *&server_stat_info,
    DRUnitStatInfo *&unit_stat_info,
    DRUnitStatInfo *&unit_in_group_stat_info,
    bool &need_generate,
    bool &is_unit_in_group_related)
{
  int ret = OB_SUCCESS;
  need_generate = false;
  is_unit_in_group_related = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(dr_ls_info.get_replica_stat(
                         index,
                         ls_replica,
                         server_stat_info,
                         unit_stat_info,
                         unit_in_group_stat_info))) {
    LOG_WARN("fail to get replica stat", KR(ret));
  } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)
                 || (unit_stat_info->is_in_pool() && OB_ISNULL(unit_stat_info->get_server_stat()))
                 || (unit_in_group_stat_info->is_in_pool() && OB_ISNULL(unit_in_group_stat_info->get_server_stat()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica stat unexpected",
                 KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
  } else if (REPLICA_STATUS_NORMAL == ls_replica->get_replica_status()
          && unit_in_group_stat_info->is_in_pool()
          && server_stat_info->get_server() != unit_in_group_stat_info->get_unit_info().unit_.server_
          && unit_in_group_stat_info->get_server_stat()->is_alive()
          && !unit_in_group_stat_info->get_server_stat()->is_block()) {
    need_generate = true;
    is_unit_in_group_related = true;
  } else if (REPLICA_STATUS_NORMAL == ls_replica->get_replica_status()
          && unit_stat_info->is_in_pool()
          && server_stat_info->get_server() != unit_stat_info->get_unit_info().unit_.server_
          && unit_stat_info->get_server_stat()->is_alive()
          && !unit_stat_info->get_server_stat()->is_block()) {
    need_generate = true;
    is_unit_in_group_related = false;
  }
  return ret;
}

int ObDRWorker::construct_extra_infos_for_generate_migrate_to_unit_task(
    DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    const DRUnitStatInfo &unit_stat_info,
    const DRUnitStatInfo &unit_in_group_stat_info,
    const ObReplicaMember &dst_member,
    const ObReplicaMember &src_member,
    const bool &is_unit_in_group_related,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    share::ObTaskId &task_id,
    ObReplicaMember &data_source,
    int64_t &data_size,
    ObDstReplica &dst_replica,
    bool &skip_change_member_list,
    int64_t &old_paxos_replica_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(task_id.init(self_addr_))) {
    //shall never be here
  } else if (OB_FAIL(choose_disaster_recovery_data_source(
                         zone_mgr_,
                         dr_ls_info,
                         dst_member,
                         src_member,
                         data_source,
                         data_size))) {
    LOG_WARN("fail to choose disaster recovery data source", KR(ret));
  } else if (OB_FAIL(dst_replica.assign(
                         is_unit_in_group_related
                         ? unit_in_group_stat_info.get_unit_info().unit_.unit_id_
                         : unit_stat_info.get_unit_info().unit_.unit_id_,
                         unit_in_group_stat_info.get_unit_info().unit_.unit_group_id_,
                         ls_replica.get_zone(),
                         dst_member))) {
    LOG_WARN("fail to assign dst replica", KR(ret));
  } else if (OB_FAIL(ObDRTask::generate_skip_change_member_list(
                         ObDRTaskType::LS_MIGRATE_REPLICA,
                         ls_replica.get_replica_type(),
                         ls_replica.get_replica_type(),
                         skip_change_member_list))) {
    LOG_WARN("fail to generate skip change member list", KR(ret));
  } else {
    tenant_id = ls_replica.get_tenant_id();
    ls_id = ls_replica.get_ls_id();
    old_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
  }
  return ret;
}

int ObDRWorker::generate_migrate_to_unit_task(
    const ObDRTaskKey task_key,
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const int64_t &data_size,
    const bool &skip_change_member_list,
    const ObDstReplica &dst_replica,
    const ObReplicaMember &src_member,
    const ObReplicaMember &data_source,
    const int64_t &old_paxos_replica_number,
    const bool is_unit_in_group_related,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObMigrateLSReplicaTask migrate_task;
  ObString comment_to_set;
  if (is_unit_in_group_related) {
    comment_to_set.assign_ptr(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_GROUP_NOT_MATCH,
                              strlen(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_GROUP_NOT_MATCH));
  } else {
    comment_to_set.assign_ptr(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH,
                              strlen(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH));
  }

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(migrate_task.build(
                         task_key,
                         tenant_id,
                         ls_id,
                         task_id,
                         0,/*schedule_time*/
                         0,/*generate_time*/
                         GCONF.cluster_id,
                         data_size,
                         obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                         skip_change_member_list,
                         ObDRTaskPriority::LOW_PRI,
                         comment_to_set,
                         dst_replica,
                         src_member,
                         data_source,
                         old_paxos_replica_number))) {
    LOG_WARN("fail to build migrate task", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(migrate_task))) {
    LOG_WARN("fail to add task", KR(ret), K(migrate_task));
  } else {
    ++acc_dr_task;
  }
  return ret;
}

int ObDRWorker::try_migrate_to_unit(
    const bool only_for_display,
    DRLSInfo &dr_ls_info,
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;

  ObDRTaskKey task_key;
  bool task_exist = false;
  bool sibling_task_executing = false;
  int64_t replica_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", KR(ret), KP(config_));
  } else if (!config_->is_rereplication_enabled()) {
    // bypass
  } else if (OB_FAIL(generate_task_key(dr_ls_info, task_key))) {
    LOG_WARN("fail to generate task key", KR(ret));
  } else if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret), K(dr_ls_info));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      bool need_generate = false;
      bool is_unit_in_group_related = false;
      if (OB_FAIL(check_need_generate_migrate_to_unit_task(
                      index,
                      dr_ls_info,
                      ls_replica,
                      server_stat_info,
                      unit_stat_info,
                      unit_in_group_stat_info,
                      need_generate,
                      is_unit_in_group_related))) {
        LOG_WARN("fail to check need generate migrate to unit task", KR(ret));
      } else if (need_generate) {
        uint64_t tenant_id = 0;
        share::ObLSID ls_id;
        share::ObTaskId task_id;
        ObReplicaMember data_source;
        int64_t data_size = 0;
        ObDstReplica dst_replica;
        bool skip_change_member_list = false;
        int64_t old_paxos_replica_number = 0;
        bool can_generate = false;
        const bool need_check_has_leader_while_remove_replica = false;
        const bool is_high_priority_task = false;
        ObReplicaMember src_member(ls_replica->get_server(),
                                   ls_replica->get_member_time_us(),
                                   ls_replica->get_replica_type(),
                                   ls_replica->get_memstore_percent());
        ObReplicaMember dst_member(is_unit_in_group_related
                                   ? unit_in_group_stat_info->get_unit_info().unit_.server_
                                   : unit_stat_info->get_unit_info().unit_.server_,
                                   ObTimeUtility::current_time(),
                                   ls_replica->get_replica_type(),
                                   ls_replica->get_memstore_percent());
        ObString comment_to_set = "";

        if (is_unit_in_group_related) {
          comment_to_set.assign_ptr(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_GROUP_NOT_MATCH,
                                    strlen(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_GROUP_NOT_MATCH));
        } else {
          comment_to_set.assign_ptr(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH,
                                    strlen(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH));
        }
        if (OB_FAIL(construct_extra_infos_for_generate_migrate_to_unit_task(
                        dr_ls_info,
                        *ls_replica,
                        *unit_stat_info,
                        *unit_in_group_stat_info,
                        dst_member,
                        src_member,
                        is_unit_in_group_related,
                        tenant_id,
                        ls_id,
                        task_id,
                        data_source,
                        data_size,
                        dst_replica,
                        skip_change_member_list,
                        old_paxos_replica_number))) {
          LOG_WARN("fail to construct extra infos for generate migrate to unit task", KR(ret));
        } else if (only_for_display) {
          ObLSReplicaTaskDisplayInfo display_info;
          if (OB_FAIL(display_info.init(
                        tenant_id,
                        ls_id,
                        ObDRTaskType::LS_MIGRATE_REPLICA,
                        ObDRTaskPriority::LOW_PRI,
                        is_unit_in_group_related
                          ? unit_in_group_stat_info->get_unit_info().unit_.server_
                          : unit_stat_info->get_unit_info().unit_.server_,
                        ls_replica->get_replica_type(),
                        old_paxos_replica_number,
                        ls_replica->get_server(),
                        ls_replica->get_replica_type(),
                        old_paxos_replica_number,
                        is_unit_in_group_related
                          ? unit_in_group_stat_info->get_unit_info().unit_.server_
                          : unit_stat_info->get_unit_info().unit_.server_,
                        comment_to_set))) {
            LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret));
          } else if (OB_FAIL(add_display_info(display_info))) {
            LOG_WARN("fail to add display info", KR(ret), K(display_info));
          } else {
            LOG_INFO("success to add display info", KR(ret), K(display_info));
          }
        } else if (OB_FAIL(check_can_generate_task(
                               acc_dr_task,
                               need_check_has_leader_while_remove_replica,
                               is_high_priority_task,
                               ls_replica->get_server(),
                               dr_ls_info,
                               task_key,
                               can_generate))) {
          LOG_WARN("fail to check can generate migrate to unir task", KR(ret));
        } else if (can_generate) {
          if (OB_FAIL(generate_migrate_to_unit_task(
                          task_key,
                          tenant_id,
                          ls_id,
                          task_id,
                          data_size,
                          skip_change_member_list,
                          dst_replica,
                          src_member,
                          data_source,
                          old_paxos_replica_number,
                          is_unit_in_group_related,
                          acc_dr_task))) {
            LOG_WARN("fail to generate migrate to unit task", KR(ret));
          }
        }
      }
    }
  }
  // no need to print task key, since the previous log contains that
  LOG_INFO("finish try migrate to unit", KR(ret), K(acc_dr_task));
  return ret;
}

int ObDRWorker::get_task_plan_display(
    common::ObSArray<ObLSReplicaTaskDisplayInfo> &task_plan)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(display_tasks_rwlock_);
  task_plan.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(task_plan.assign(display_tasks_))) {
    LOG_WARN("fail to get dsplay task stat", KR(ret), K_(display_tasks));
  }
  reset_task_plans_();
  return ret;
}

int ObDRWorker::add_display_info(const ObLSReplicaTaskDisplayInfo &display_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(display_tasks_rwlock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!display_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(display_info));
  } else if (OB_FAIL(display_tasks_.push_back(display_info))) {
    LOG_WARN("insert replica failed", KR(ret), K(display_info));
  }
  return ret;
}

int ObDRWorker::generate_disaster_recovery_paxos_replica_number(
    const DRLSInfo &dr_ls_info,
    const int64_t curr_paxos_replica_number,
    const int64_t locality_paxos_replica_number,
    const MemberChangeType member_change_type,
    int64_t &new_paxos_replica_number,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  int64_t member_list_cnt = dr_ls_info.get_member_list_cnt();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObLSID ls_id;
  if (OB_UNLIKELY(member_list_cnt <= 0
                  || curr_paxos_replica_number <= 0
                  || locality_paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(member_list_cnt),
             K(curr_paxos_replica_number),
             K(locality_paxos_replica_number));
  } else if (MEMBER_CHANGE_ADD == member_change_type) {
    const int64_t member_list_cnt_after = member_list_cnt + 1;
    if (curr_paxos_replica_number == locality_paxos_replica_number) {
      if (locality_paxos_replica_number >= member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number;
        found = true;
      } else if (locality_paxos_replica_number + 1 == member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number + 1;
        found = true;
      }
    } else if (curr_paxos_replica_number > locality_paxos_replica_number) {
      if (curr_paxos_replica_number >= member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number;
        found = true;
      } else {} // new member cnt greater than paxos_replica_number, not good
    } else { // curr_paxos_replica_number < locality_paxos_replica_number
      if (majority(curr_paxos_replica_number + 1) <= member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number + 1;
        found = true;
      } else {} // majority not satisfied
    }
  } else if (MEMBER_CHANGE_NOP == member_change_type) {
    if (curr_paxos_replica_number == locality_paxos_replica_number) {
      new_paxos_replica_number = curr_paxos_replica_number;
      found = true;
    } else if (curr_paxos_replica_number > locality_paxos_replica_number) {
      if (member_list_cnt <= curr_paxos_replica_number - 1) {
        new_paxos_replica_number = curr_paxos_replica_number - 1;
        found = true;
      }
    } else { // curr_paxos_replica_number < locality_paxos_replica_number
      if (member_list_cnt > majority(curr_paxos_replica_number + 1)) {
        new_paxos_replica_number = curr_paxos_replica_number + 1;
        found = true;
      }
    }
  } else if (MEMBER_CHANGE_SUB == member_change_type) {
    int64_t member_list_cnt_after = 0;
    int64_t arb_replica_number = 0;
    if (OB_FAIL(dr_ls_info.get_ls_id(tenant_id, ls_id))) {
      LOG_WARN("fail to get tenant and ls id", KR(ret), K(dr_ls_info));
    } else if (OB_FAIL(ObShareUtil::generate_arb_replica_num(
                           tenant_id,
                           ls_id,
                           arb_replica_number))) {
      LOG_WARN("fail to generate arb replica number", KR(ret), K(tenant_id), K(ls_id));
    }
    member_list_cnt_after = member_list_cnt - 1 + arb_replica_number;
    if (OB_FAIL(ret)) {
    } else if (curr_paxos_replica_number == locality_paxos_replica_number) {
      if (majority(curr_paxos_replica_number) <= member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number;
        found = true;
      } else {} // majority not satisfied
    } else if (curr_paxos_replica_number > locality_paxos_replica_number) {
      if (majority(curr_paxos_replica_number - 1) <= member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number - 1;
        found = true;
      } else {} // majority not satisfied
    } else { // curr_paxos_replica_number < locality_paxos_replica_number
      if (majority(curr_paxos_replica_number) <= member_list_cnt_after) {
        new_paxos_replica_number = curr_paxos_replica_number;
        found = true;
      } else {} // majority not satisfied
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid member change type", KR(ret), K(member_change_type));
  }
  FLOG_INFO("finish generating disaster recovery paxos replica number", KR(ret),
           K(dr_ls_info), K(found), K(member_list_cnt), K(curr_paxos_replica_number),
           K(locality_paxos_replica_number), K(member_change_type), K(new_paxos_replica_number));
  return ret;
}

int ObDRWorker::choose_disaster_recovery_data_source(
    ObZoneManager *zone_mgr,
    DRLSInfo &dr_ls_info,
    const ObReplicaMember &dst_member,
    const ObReplicaMember &src_member,
    ObReplicaMember &data_source,
    int64_t &data_size)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  ObRegion dst_region;
  ObDataSourceCandidateChecker type_checker(dst_member.get_replica_type());
  int64_t replica_cnt = 0;
  share::ObLSReplica *ls_replica = nullptr;
  DRServerStatInfo *server_stat_info = nullptr;
  DRUnitStatInfo *unit_stat_info = nullptr;
  DRUnitStatInfo *unit_in_group_stat_info = nullptr;
  ObZone dst_zone;

  if (OB_ISNULL(zone_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_mgr is null", KR(ret), KP(zone_mgr));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst_member.get_server(), dst_zone))) {
    LOG_WARN("fail to get server zone", KR(ret), K(dst_member.get_server()));
  } else if (OB_FAIL(zone_mgr->get_region(dst_zone, dst_region))) {
    LOG_WARN("fail to get region", KR(ret), K(dst_zone));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret));
  } else {
    ObLSReplica *src_replica = nullptr;
    // try task offline src
    for (int64_t i = 0;
        OB_SUCC(ret) && i < replica_cnt && src_member.is_valid() && nullptr == src_replica;
        ++i) {
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              i,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica status ptr is null", KR(ret),
                  KP(ls_replica),
                  KP(server_stat_info),
                  KP(unit_stat_info),
                  KP(unit_in_group_stat_info));
      } else if (ls_replica->is_in_service()
          && server_stat_info->is_alive()
          && !server_stat_info->is_stopped()
          && type_checker.is_candidate(ls_replica->get_replica_type())
          && ls_replica->get_server() == src_member.get_server()
          && !ls_replica->get_restore_status().is_restore_failed()) {
        src_replica = ls_replica;
        break;
      }
    }
    // try the same zone replica
    for (int64_t i = 0;
         OB_SUCC(ret) && i < replica_cnt && nullptr == src_replica && !dst_zone.is_empty();
         ++i) {
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              i,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica status ptr is null", KR(ret),
                  KP(ls_replica),
                  KP(server_stat_info),
                  KP(unit_stat_info),
                  KP(unit_in_group_stat_info));
      } else if (ls_replica->is_in_service()
          && server_stat_info->is_alive()
          && !server_stat_info->is_stopped()
          && type_checker.is_candidate(ls_replica->get_replica_type())
          && ls_replica->get_zone() == dst_zone
          && ls_replica->get_server() != dst_member.get_server()
          && !ls_replica->get_restore_status().is_restore_failed()) {
        src_replica = ls_replica;
        break;
      }
    }
    // try the same region replica
    for (int64_t i = 0;
         OB_SUCC(ret) && i < replica_cnt && nullptr == src_replica && !dst_region.is_empty();
         ++i) {
      common::ObRegion ls_region;
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              i,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica status ptr is null", KR(ret),
                  KP(ls_replica),
                  KP(server_stat_info),
                  KP(unit_stat_info),
                  KP(unit_in_group_stat_info));
      } else if (OB_SUCCESS != zone_mgr->get_region(ls_replica->get_zone(), ls_region)) {
        // ignore ret
        LOG_WARN("fail to get region", KPC(ls_replica));
      } else if (ls_replica->is_in_service()
          && server_stat_info->is_alive()
          && !server_stat_info->is_stopped()
          && type_checker.is_candidate(ls_replica->get_replica_type())
          && ls_region == dst_region
          && ls_replica->get_server() != dst_member.get_server()
          && !ls_replica->get_restore_status().is_restore_failed()) {
        src_replica = ls_replica;
        break;
      }
    }
    // try any qualified replica
    for (int64_t i = 0;
         OB_SUCC(ret) && i < replica_cnt && nullptr == src_replica;
         ++i) {
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              i,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_ISNULL(ls_replica)
                 || OB_ISNULL(server_stat_info)
                 || OB_ISNULL(unit_stat_info)
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica status ptr is null", KR(ret),
                  KP(ls_replica),
                  KP(server_stat_info),
                  KP(unit_stat_info),
                  KP(unit_in_group_stat_info));
      } else if (ls_replica->is_in_service()
          && server_stat_info->is_alive()
          && !server_stat_info->is_stopped()
          && type_checker.is_candidate(ls_replica->get_replica_type())
          && ls_replica->get_server() != dst_member.get_server()
          && !ls_replica->get_restore_status().is_restore_failed()) {
        src_replica = ls_replica;
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (nullptr != src_replica) {
        data_source = ObReplicaMember(src_replica->get_server(),
                                      src_replica->get_member_time_us(),
                                      src_replica->get_replica_type(),
                                      src_replica->get_memstore_percent());
        data_size = src_replica->get_required_size();
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no valid source candidates", KR(ret));
      }
    }
  }
  return ret;
}

int ObDRWorker::check_ls_only_in_member_list_or_with_flag_(
    const DRLSInfo &dr_ls_info)
{
  int ret = OB_SUCCESS;
  const share::ObLSReplica *leader_replica = nullptr;
  share::ObLSInfo inner_ls_info;
  if (OB_FAIL(dr_ls_info.get_inner_ls_info(inner_ls_info))) {
    LOG_WARN("fail to get inner ls info", KR(ret), K(dr_ls_info));
  } else if (OB_UNLIKELY(!inner_ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inner_ls_info));
  } else if (OB_FAIL(inner_ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader", KR(ret), K(inner_ls_info));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica is null", KR(ret));
  } else if (OB_UNLIKELY(0 >= leader_replica->get_member_list().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader member list has no member", KR(ret), "member_lsit", leader_replica->get_member_list());
  } else {
    // check member list
    for (int64_t i = 0; OB_SUCC(ret) && i < leader_replica->get_member_list().count(); ++i) {
      const share::ObLSReplica *replica = nullptr;
      const common::ObAddr &server = leader_replica->get_member_list().at(i).get_server();
      const int64_t member_time_us = leader_replica->get_member_list().at(i).get_timestamp();
      if (OB_FAIL(inner_ls_info.find(server, replica))) {
        LOG_WARN("fail to find replica", KR(ret), K(inner_ls_info), K(server));
      }
    }
    // check learner list
    for (int64_t index = 0; OB_SUCC(ret) && index < leader_replica->get_learner_list().get_member_number(); ++index) {
      common::ObMember learner_to_check;
      const share::ObLSReplica *replica = nullptr;
      if (OB_FAIL(leader_replica->get_learner_list().get_member_by_index(index, learner_to_check))) {
        LOG_WARN("fail to get learner by index", KR(ret), K(index));
      } else if (learner_to_check.is_migrating()) {
        if (OB_FAIL(inner_ls_info.find(learner_to_check.get_server(), replica))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // good, learner with flag should not appear in inner_ls_info
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to find replica", KR(ret), K(inner_ls_info), K(learner_to_check));
          }
        } else {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("read only replica with flag should not appear in inner_ls_info", KR(ret), K(learner_to_check), K(inner_ls_info));
        }
      } else if (OB_FAIL(inner_ls_info.find(learner_to_check.get_server(), replica))) {
        LOG_WARN("fail to find read only replica", KR(ret), K(inner_ls_info), K(learner_to_check));
      }
    }
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
