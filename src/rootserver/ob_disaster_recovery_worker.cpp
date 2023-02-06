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
    comment_("")
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
      if (OB_UNLIKELY(nullptr == my_desc_array)) {
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
      if (OB_FAIL(ret) && nullptr != my_desc_array) {
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
                                                 ObServerManager *server_mgr,
                                                 ObZoneManager *zone_mgr,
                                                 DRLSInfo &dr_ls_info)
  : task_idx_(0),
    add_replica_task_(),
    unit_mgr_(unit_mgr),
    server_mgr_(server_mgr),
    zone_mgr_(zone_mgr),
    dr_ls_info_(dr_ls_info),
    task_array_(),
    curr_paxos_replica_number_(0),
    locality_paxos_replica_number_(0),
    locality_map_(),
    replica_stat_map_(),
    unit_set_(),
    unit_provider_(unit_set_),
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
  if (OB_UNLIKELY(nullptr == unit_mgr_)) {
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
      } else if (OB_UNLIKELY(nullptr == zone_replica_desc)) {
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
        // readonly replica, normal
        for (int64_t j = 0; OB_SUCC(ret) && j < readonly_locality.count(); ++j) {
          const ReplicaAttr &replica_attr = readonly_locality.at(j);
          if (replica_attr.num_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replica num unexpected", KR(ret), K(zone), K(readonly_locality));
          } else if (ObLocalityDistribution::ALL_SERVER_CNT == replica_attr.num_) {
            // bypass, postpone processing
          } else if (OB_FAIL(zone_replica_desc->push_back(ReplicaDesc(REPLICA_TYPE_READONLY,
                                                                      replica_attr.memstore_percent_,
                                                                      replica_attr.num_)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        // readonly replica, all_server
        for (int64_t j = 0; OB_SUCC(ret) && j < readonly_locality.count(); ++j) {
          const ReplicaAttr &replica_attr = readonly_locality.at(j);
          if (replica_attr.num_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("replica num unexpected", KR(ret), K(zone), K(readonly_locality));
          } else if (ObLocalityDistribution::ALL_SERVER_CNT != replica_attr.num_) {
            // bypass, processed before
          } else {
            zone_replica_desc->is_readonly_all_server_ = true;
            zone_replica_desc->readonly_memstore_percent_ = replica_attr.memstore_percent_;
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
      } else if (OB_UNLIKELY(nullptr == replica
                             || nullptr == server_stat_info
                             || nullptr == unit_stat_info
                             || nullptr == unit_in_group_stat_info)) {
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
  int ret = OB_SUCCESS;
  share::ObLSReplica *replica = nullptr;
  DRServerStatInfo *server_stat_info = nullptr;
  DRUnitStatInfo *unit_stat_info = nullptr;
  if (OB_UNLIKELY(!replica_stat_desc.is_valid()
                  || index >= replica_stat_map_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc), K(index),
             "replica_stat_map_count", replica_stat_map_.count());
  } else if (OB_UNLIKELY(nullptr == (replica = replica_stat_desc.replica_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica ptr is null", KR(ret), K(replica_stat_desc));
  } else if (OB_UNLIKELY(nullptr == (server_stat_info = replica_stat_desc.server_stat_info_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat info ptr is null", KR(ret));
  } else if (OB_UNLIKELY(nullptr == (unit_stat_info = replica_stat_desc.unit_stat_info_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat info ptr is null", KR(ret));
  } else {
    const common::ObZone &zone = replica->get_zone();
    ReplicaDescArray *zone_replica_desc = nullptr;
    int tmp_ret = locality_map_.get_refactored(zone, zone_replica_desc);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // zone not exist, not match
    } else if (OB_SUCCESS == tmp_ret) {
      if (OB_UNLIKELY(nullptr == zone_replica_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone replica desc ptr is null", KR(ret), K(zone));
      } else {
        for (int64_t i = zone_replica_desc->count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          bool found = false;
          ReplicaDesc &replica_desc = zone_replica_desc->at(i);
          if (unit_stat_info->is_in_pool()
              && server_stat_info->get_server() == unit_stat_info->get_unit_info().unit_.server_
              && replica->get_replica_type() == replica_desc.replica_type_
              && replica->get_memstore_percent() == replica_desc.memstore_percent_
              && replica_desc.replica_num_ > 0) {
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
          if (OB_SUCC(ret)
              && !found
              && replica->get_replica_type() == replica_desc.replica_type_
              && replica->get_memstore_percent() == replica_desc.memstore_percent_
              && replica_desc.replica_num_ > 0) {
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
    } else {
      const ObReplicaType replica_type = replica_stat_desc.replica_->get_replica_type();
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
        if (!replica_stat_desc.replica_->get_in_member_list()) {
          if (OB_FAIL(replica_stat_map_.remove(i))) {
            LOG_WARN("fail to remove task", KR(ret));
          }
        } else {
          if (OB_FAIL(try_remove_match(replica_stat_desc, i))) {
            LOG_WARN("fail to try remove match", KR(ret));
          }
        } 
      } else {
        if (OB_FAIL(try_remove_match(replica_stat_desc, i))) {
          LOG_WARN("fail to try remove match", KR(ret));
        }
      }
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
      if (OB_FAIL(generate_remove_paxos_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove paxos task", KR(ret));
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
        if (OB_FAIL(generate_remove_paxos_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove paxos task", KR(ret));
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
      if (OB_FAIL(generate_remove_paxos_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove paxos task", KR(ret));
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
        if (OB_FAIL(generate_remove_paxos_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove paxos task", KR(ret));
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
      if (OB_FAIL(generate_remove_paxos_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove paxos task", KR(ret));
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
        if (OB_FAIL(generate_remove_paxos_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove paxos task", KR(ret));
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
      if (OB_FAIL(generate_remove_nonpaxos_task(replica_stat_desc))) {
        LOG_WARN("fail to generate remove paxos task", KR(ret));
      } else if (OB_FAIL(replica_stat_map_.remove(index))) {
        LOG_WARN("fail to remove", KR(ret));
      }
    } else if (OB_SUCCESS == tmp_ret && nullptr != zone_replica_desc) {
      bool found = false;
      std::sort(zone_replica_desc->begin(), zone_replica_desc->end());
      for (int64_t i = zone_replica_desc->count() - 1; !found && OB_SUCC(ret) && i >= 0; --i) {
        ReplicaDesc &replica_desc = zone_replica_desc->at(i);
        if (REPLICA_TYPE_READONLY == replica_desc.replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica type unexpected", KR(ret), K(dr_ls_info_));
        } else if (REPLICA_TYPE_FULL == replica_desc.replica_type_) {
          if (OB_FAIL(generate_type_transform_task(
                  replica_stat_desc,
                  replica_desc.replica_type_,
                  replica_desc.memstore_percent_))) {
            LOG_WARN("fail to generate type transform task", KR(ret), K(replica_stat_desc));
          } else if (OB_FAIL(zone_replica_desc->remove(i))) {
            LOG_WARN("fail to remove", KR(ret), K(i), K(replica), K(zone_replica_desc));
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
        if (OB_FAIL(replica_stat_map_.remove(index))) {
          LOG_WARN("fail to remove", KR(ret), K(index), K(replica), K(replica_stat_map_));
        }
      } else {
        if (OB_FAIL(generate_remove_nonpaxos_task(replica_stat_desc))) {
          LOG_WARN("fail to generate remove paxos task", KR(ret));
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

int ObDRWorker::LocalityAlignment::try_generate_locality_task_from_locality_map()
{
  int ret = OB_SUCCESS;
  LocalityMap::iterator iter = locality_map_.begin();
  for (; iter != locality_map_.end(); ++iter) {
    ReplicaDescArray *replica_desc_array = iter->second;
    if (OB_UNLIKELY(nullptr == replica_desc_array)) {
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
    } else if (OB_UNLIKELY(nullptr == replica)) {
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
        LOG_WARN("fail to generate locality task from logonly replica", KR(ret));
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

int ObDRWorker::LocalityAlignment::generate_remove_paxos_task(
    ReplicaStatDesc &replica_stat_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!replica_stat_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc));
  } else {
    void *raw_ptr = nullptr;
    RemovePaxosLATask *task = nullptr;
    ObLSReplica *replica = replica_stat_desc.replica_;
    if (OB_UNLIKELY(nullptr == replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica ptr is null", KR(ret));
    } else if (nullptr == (raw_ptr = allocator_.alloc(sizeof(RemovePaxosLATask)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (nullptr == (task = new (raw_ptr) RemovePaxosLATask())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct task failed", KR(ret));
    } else {
      task->remove_server_ = replica->get_server();
      task->replica_type_ = replica->get_replica_type();
      task->memstore_percent_ = replica->get_memstore_percent();
      task->member_time_us_ = replica->get_member_time_us();
      if (OB_FAIL(task_array_.push_back(task))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        LOG_INFO("success to push a remove paxos task to task_array", KR(ret), KPC(task));
      }
    }
    // btw: no need to free memory when failed for arena, just destruct
    if (OB_FAIL(ret) && nullptr != task) {
      task->~RemovePaxosLATask();
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::generate_remove_nonpaxos_task(
    ReplicaStatDesc &replica_stat_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!replica_stat_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_stat_desc));
  } else {
    void *raw_ptr = nullptr;
    RemoveNonPaxosLATask *task = nullptr;
    ObLSReplica *replica = replica_stat_desc.replica_;
    if (OB_UNLIKELY(nullptr == replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica ptr is null", KR(ret));
    } else if (nullptr == (raw_ptr = allocator_.alloc(sizeof(RemoveNonPaxosLATask)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (nullptr == (task = new (raw_ptr) RemoveNonPaxosLATask())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("construct task failed", KR(ret));
    } else {
      task->remove_server_ = replica->get_server();
      task->replica_type_ = replica->get_replica_type();
      task->memstore_percent_ = replica->get_memstore_percent();
      task->member_time_us_ = replica->get_member_time_us();
      if (OB_FAIL(task_array_.push_back(task))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        LOG_INFO("success to push a remove non paxos task to task_array", KR(ret), KPC(task));
      }
    }
    // btw: no need to free memory when failed for arena, just destruct
    if (OB_FAIL(ret) && nullptr != task) {
      task->~RemoveNonPaxosLATask();
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
    ObLSReplica *replica = replica_stat_desc.replica_;
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
                  || nullptr == server_mgr_
                  || nullptr == zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("LocalityAlignment not init",
             KR(ret), KP(unit_mgr_), KP(server_mgr_), KP(zone_mgr_));
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
  } else if (OB_FAIL(unit_set_.create(UNIT_SET_BUCKET_NUM))) {
    LOG_WARN("fail to create unit set", KR(ret));
  } else if (OB_FAIL(init_unit_set(unit_set_))) {
    LOG_WARN("fail to init unit set", KR(ret));
  } else if (OB_FAIL(unit_provider_.init(gen_user_tenant_id(tenant_id), unit_mgr_, server_mgr_))) {
    LOG_WARN("fail to init unit provider", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::init_unit_set(
    common::hash::ObHashSet<int64_t> &unit_set)
{
  int ret = OB_SUCCESS;
  int64_t replica_cnt = 0;
  if (OB_FAIL(dr_ls_info_.get_replica_cnt(replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < replica_cnt; ++index) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(dr_ls_info_.get_replica_stat(
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
        if (OB_FAIL(unit_set.set_refactored(unit_stat_info->get_unit_info().unit_.unit_id_))) {
          LOG_WARN("fail to set refactored", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDRWorker::LocalityAlignment::try_review_remove_paxos_task(
    UnitProvider &unit_provider,
    LATask *this_task,
    const LATask *&output_task,
    bool &found)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_provider);
  RemovePaxosLATask *my_task = reinterpret_cast<RemovePaxosLATask *>(this_task);
  if (OB_UNLIKELY(nullptr == this_task || nullptr == my_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(this_task), KP(my_task));
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

int ObDRWorker::LocalityAlignment::try_review_remove_nonpaxos_task(
    UnitProvider &unit_provider,
    LATask *this_task,
    const LATask *&output_task,
    bool &found)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_provider);
  RemoveNonPaxosLATask *my_task = reinterpret_cast<RemoveNonPaxosLATask *>(this_task);
  if (OB_UNLIKELY(nullptr == this_task || nullptr == my_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(this_task), KP(my_task));
  } else {
    output_task = my_task;
    found = true;
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
    int tmp_ret = unit_provider.get_unit(zone, ls_status_info->unit_group_id_, unit_info);
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
    if (OB_UNLIKELY(nullptr == this_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this task ptr is null", KR(ret));
    } else {
      switch (this_task->get_task_type()) {
      case RemovePaxos:
        if (OB_FAIL(try_review_remove_paxos_task(
                unit_provider,
                this_task,
                task,
                found))) {
          LOG_WARN("fail to try review remove paxos task", KR(ret), KPC(this_task), K(found));
        } else {
          LOG_INFO("success to try review remove paxos task", KR(ret), KPC(this_task), K(found));
        }
        break;
      case RemoveNonPaxos:
        if (OB_FAIL(try_review_remove_nonpaxos_task(
                unit_provider,
                this_task,
                task,
                found))) {
          LOG_WARN("fail to try review remove nonpaxos task", KR(ret), KPC(this_task), K(found));
        } else {
          LOG_INFO("success to try review remove nonpaxos task", KR(ret), KPC(this_task), K(found));
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
      int tmp_ret = unit_provider.get_unit(zone, ls_status_info->unit_group_id_, unit_info);
      if (OB_ITER_END == tmp_ret) {
        // bypass
      } else if (OB_SUCCESS == tmp_ret) {
        add_replica_task_.zone_ = zone;
        add_replica_task_.dst_server_ = unit_info.unit_.server_;
        add_replica_task_.unit_id_ = unit_info.unit_.unit_id_;
        add_replica_task_.unit_group_id_ = ls_status_info->unit_group_id_;
        add_replica_task_.replica_type_ = REPLICA_TYPE_READONLY;
        add_replica_task_.memstore_percent_ = replica_desc_array->readonly_memstore_percent_;
        add_replica_task_.orig_paxos_replica_number_ = curr_paxos_replica_number_;
        add_replica_task_.paxos_replica_number_ = curr_paxos_replica_number_;
        task = &add_replica_task_;
        if (OB_FAIL(unit_set_.set_refactored(unit_info.unit_.unit_id_))) {
          LOG_WARN("fail to set refactored", KR(ret));
        }
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
    ObUnitManager *unit_mgr,
    ObServerManager *server_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || nullptr == unit_mgr
                         || nullptr == server_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(tenant_id),
             KP(unit_mgr),
             KP(server_mgr));
  } else {
    tenant_id_ = tenant_id;
    unit_mgr_ = unit_mgr;
    server_mgr_ = server_mgr;
    inited_ = true;
  }
  return ret;
}

int ObDRWorker::UnitProvider::get_unit(
    const common::ObZone &zone,
    const uint64_t unit_group_id,
    share::ObUnitInfo &unit_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(unit_mgr_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is null", KR(ret), KP(unit_mgr_), KP(server_mgr_));
  } else {
    common::ObArray<ObUnitInfo> unit_array;
    bool found = false;
    if (unit_group_id > 0) {
      if (OB_FAIL(unit_mgr_->get_unit_group(tenant_id_, unit_group_id, unit_array))) {
        LOG_WARN("fail to get unit group", KR(ret), K(tenant_id_), K(unit_group_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < unit_array.count(); ++i) {
          bool is_active = false;
          const share::ObUnitInfo &this_info = unit_array.at(i);
          const uint64_t unit_id = this_info.unit_.unit_id_;
          int hash_ret = OB_SUCCESS;
          if (this_info.unit_.zone_ != zone) {
            // bypass, because we only support migrate in same zone
          } else if (OB_FAIL(server_mgr_->check_server_active(this_info.unit_.server_, is_active))) {
            LOG_WARN("fail to check server active", KR(ret), "server", this_info.unit_.server_);
          } else if (!is_active) {
            LOG_INFO("server is not active", "server", this_info.unit_.server_, K(is_active));
            break; // server not active
          } else if (OB_HASH_EXIST == (hash_ret = unit_set_.exist_refactored(unit_id))) {
            LOG_INFO("unit existed", K(unit_id));
            break;
          } else if (OB_HASH_NOT_EXIST != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("exist refactored failed", KR(ret), KR(hash_ret));
          } else if (OB_FAIL(unit_info.assign(this_info))) {
            LOG_WARN("fail to assign unit info", KR(ret));
          } else {
            found = true;
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      unit_array.reset();
      if (OB_FAIL(unit_mgr_->get_all_unit_infos_by_tenant(tenant_id_, unit_array))) {
        LOG_WARN("fail to get ll unit infos by tenant", KR(ret), K(tenant_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < unit_array.count(); ++i) {
          bool is_active = false;
          const share::ObUnitInfo &this_info = unit_array.at(i);
          const uint64_t unit_id = this_info.unit_.unit_id_;
          int hash_ret = OB_SUCCESS;
          if (this_info.unit_.zone_ != zone) {
            // bypass, because only support migrate in same zone
          } else if (OB_FAIL(server_mgr_->check_server_active(this_info.unit_.server_, is_active))) {
            LOG_WARN("fail to check server active", KR(ret), "server", this_info.unit_.server_);
          } else if (!is_active) {
            LOG_INFO("server is not active", "server", this_info.unit_.server_, K(is_active));
          } else if (OB_HASH_EXIST == (hash_ret = unit_set_.exist_refactored(unit_id))) {
            LOG_INFO("unit existed", K(unit_id));
          } else if (OB_HASH_NOT_EXIST != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("exist refactored failed", KR(ret), KR(hash_ret));
          } else if (OB_FAIL(unit_info.assign(this_info))) {
            LOG_WARN("fail to assign unit info", KR(ret));
          } else {
            found = true;
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ITER_END;
      LOG_WARN("fail to get valid unit", KR(ret), K(found));
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
    server_mgr_(nullptr),
    zone_mgr_(nullptr),
    disaster_recovery_task_mgr_(nullptr),
    lst_operator_(nullptr),
    schema_service_(nullptr),
    rpc_proxy_(nullptr),
    sql_proxy_(nullptr),
    task_count_statistic_(),
    display_tasks_()
{
}

ObDRWorker::~ObDRWorker()
{
}

int ObDRWorker::init(
    common::ObAddr &self_addr,
    common::ObServerConfig &config,
    ObUnitManager &unit_mgr,
    ObServerManager &server_mgr,
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
    server_mgr_ = &server_mgr;
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
    ObServerManager &server_mgr,
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
        DRLSInfo dr_ls_info(gen_user_tenant_id(tenant_id),
                            &unit_mgr,
                            &server_mgr,
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
                ls_info, ls_status_info))) {
          LOG_WARN("fail to generate dr log stream info", KR(ret));
        } else if (OB_FAIL(check_ls_locality_match_(
                dr_ls_info, unit_mgr, server_mgr, zone_mgr, locality_is_matched))) {
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
    ObServerManager &server_mgr,
    ObZoneManager &zone_mgr,
    bool &locality_is_matched)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  locality_is_matched = false;
  LOG_INFO("start to check ls locality match", K(dr_ls_info));
  LocalityAlignment locality_alignment(&unit_mgr,
                                       &server_mgr,
                                       &zone_mgr,
                                       dr_ls_info);
  const LATask *task = nullptr;
  if (!dr_ls_info.has_leader()) {
    LOG_WARN("has no leader, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (dr_ls_info.get_paxos_replica_number() <= 0) {
    LOG_WARN("paxos_replica_number is invalid, maybe not report yet",
             KR(ret), K(dr_ls_info));
  } else if (OB_FAIL(locality_alignment.build())) {
    LOG_WARN("fail to build locality alignment", KR(ret));
  } else if (OB_SUCCESS != (tmp_ret = locality_alignment.get_next_locality_alignment_task(task))) {
    if (OB_ITER_END == tmp_ret) {
      ret = OB_SUCCESS;
      locality_is_matched = true;
    } else {
      ret = OB_SUCCESS;
      LOG_WARN("fail to get next locality alignment task", KR(tmp_ret));
    }
  } else {
    locality_is_matched = false;
  }
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("the locality matched check for this logstream", KR(ret), K(locality_is_matched),
           K(dr_ls_info), KPC(task));
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
        DRLSInfo dr_ls_info(gen_user_tenant_id(tenant_id),
                            unit_mgr_,
                            server_mgr_,
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
          } else if (OB_SUCCESS != (tmp_ret = dr_ls_info.init())) {
            LOG_WARN("fail to init dr log stream info", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = dr_ls_info.build_disaster_ls_info(
                  ls_info, ls_status_info))) {
            LOG_WARN("fail to generate dr log stream info", KR(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = try_ls_disaster_recovery(
                  only_for_display, dr_ls_info, ls_acc_dr_task))) {
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
    int64_t &acc_dr_task_cnt)
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
            only_for_display, dr_ls_info, acc_dr_task_cnt))) {
      LOG_WARN("fail to try remove permanent offline replicas",
               KR(ret), K(dr_ls_info));
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

int ObDRWorker::check_need_generate_remove_permanent_offline_replicas(
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
                 || OB_ISNULL(unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected", KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
  } else if (REPLICA_STATUS_NORMAL == ls_replica->get_replica_status()
             && ObReplicaTypeCheck::is_paxos_replica_V2(ls_replica->get_replica_type())
             && server_stat_info->is_permanent_offline()) {
    need_generate = true;
    LOG_INFO("found permanent offline ls replica", KPC(ls_replica));
  }
  return ret;
}

int ObDRWorker::check_can_generate_task(
    const int64_t acc_dr_task,
    const bool need_check_has_leader_while_remove_replica,
    const bool is_high_priority_task,
    const DRServerStatInfo &server_stat_info,
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
                         server_stat_info.get_server(),
                         dr_ls_info,
                         has_leader_while_remove_replica))) {
      LOG_WARN("fail to check has leader while member change", KR(ret), K(dr_ls_info), K(server_stat_info));
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

int ObDRWorker::construct_extra_infos_to_build_remove_paxos_replica_task(
    const DRLSInfo &dr_ls_info,
    const share::ObLSReplica &ls_replica,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    share::ObTaskId &task_id,
    int64_t &new_paxos_replica_number,
    int64_t &old_paxos_replica_number,
    common::ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  bool found_new_paxos_replica_number = false;
  if (FALSE_IT(task_id.init(self_addr_))) {
  } else if (OB_FAIL(generate_disaster_recovery_paxos_replica_number(
                         dr_ls_info,
                         dr_ls_info.get_paxos_replica_number(),
                         dr_ls_info.get_schema_replica_cnt(),
                         MEMBER_CHANGE_SUB,
                         new_paxos_replica_number,
                         found_new_paxos_replica_number))) {
      LOG_WARN("fail to generate disaster recovery paxos_replica_number", KR(ret), K(found_new_paxos_replica_number));
  } else if (!found_new_paxos_replica_number) {
    LOG_WARN("paxos_replica_number not found", K(dr_ls_info));
  } else if (OB_FAIL(dr_ls_info.get_leader(leader_addr))) {
    LOG_WARN("fail to get leader", KR(ret));
  } else {
    tenant_id = ls_replica.get_tenant_id();
    ls_id = ls_replica.get_ls_id();
    old_paxos_replica_number = dr_ls_info.get_paxos_replica_number();
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
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObRemoveLSPaxosReplicaTask remove_member_task;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(remove_member_task.build(
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
                "remove permanent offline replica",
                leader_addr,
                remove_member,
                old_paxos_replica_number,
                new_paxos_replica_number))) {
    LOG_WARN("fail to build remove member task", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_member_task))) {
    LOG_WARN("fail to add task", KR(ret), K(remove_member_task));
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

  ObDRTaskKey task_key;
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
      bool can_generate = false;
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(check_need_generate_remove_permanent_offline_replicas(
                      index,
                      dr_ls_info,
                      ls_replica,
                      server_stat_info,
                      unit_stat_info,
                      unit_in_group_stat_info,
                      need_generate))) {
        LOG_WARN("fail to check need generate remove permanent offline task", KR(ret));
      } else if (need_generate) {
        uint64_t tenant_id;
        share::ObLSID ls_id;
        share::ObTaskId task_id;
        int64_t new_paxos_replica_number;
        int64_t old_paxos_replica_number;
        common::ObAddr leader_addr;
        const common::ObAddr source_server; // not useful
        const bool need_check_has_leader_while_remove_replica = true;
        const bool is_high_priority_task = true;
        ObReplicaMember remove_member(ls_replica->get_server(),
                                      ls_replica->get_member_time_us(),
                                      ls_replica->get_replica_type(),
                                      ls_replica->get_memstore_percent());
        if (OB_FAIL(construct_extra_infos_to_build_remove_paxos_replica_task(
                        dr_ls_info,
                        *ls_replica,
                        tenant_id,
                        ls_id,
                        task_id,
                        new_paxos_replica_number,
                        old_paxos_replica_number,
                        leader_addr))) {
          LOG_WARN("fail to construct extra infos to build remove paxos replica task", KR(ret));
        } else if (only_for_display) {
          ObLSReplicaTaskDisplayInfo display_info;
          if (OB_FAIL(display_info.init(
                        tenant_id,
                        ls_id,
                        ObDRTaskType::LS_REMOVE_PAXOS_REPLICA,
                        ObDRTaskPriority::HIGH_PRI,
                        ls_replica->get_server(),
                        REPLICA_TYPE_FULL,
                        new_paxos_replica_number,
                        source_server,
                        REPLICA_TYPE_MAX,
                        old_paxos_replica_number,
                        leader_addr,
                        "remove permanent offline replica"))) {
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
                               *server_stat_info,
                               dr_ls_info,
                               task_key,
                               can_generate))) {
          LOG_WARN("fail to check can generate remove permanent offline task", KR(ret));
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
                          acc_dr_task))) {
            LOG_WARN("fail to generate remove permanent offline task", KR(ret));
          }
        }
      }
    }
  }
  // no need to print task key, since the previous log contains that
  LOG_INFO("finish try remove permanent offline replica", KR(ret), K(acc_dr_task));
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
                         server_mgr_,
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
    int64_t &acc_dr_task)
{
  int ret = OB_SUCCESS;
  ObMigrateLSReplicaTask migrate_task;
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
                         ObDRTaskPriority::HIGH_PRI,
                         "replicate to unit task",
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
      bool can_generate = false;
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
        uint64_t tenant_id;
        share::ObLSID ls_id;
        share::ObTaskId task_id;
        ObReplicaMember data_source;
        int64_t data_size = 0;
        bool skip_change_member_list = false;
        ObDstReplica dst_replica;
        int64_t old_paxos_replica_number;
        const bool need_check_has_leader_while_remove_replica = false;
        const bool is_high_priority_task = true;
        ObReplicaMember dst_member(unit_stat_info->get_unit_info().unit_.server_,
                                   ObTimeUtility::current_time(),
                                   ls_replica->get_replica_type(),
                                   ls_replica->get_memstore_percent());
        ObReplicaMember src_member(ls_replica->get_server(),
                                   ls_replica->get_member_time_us(),
                                   ls_replica->get_replica_type(),
                                   ls_replica->get_memstore_percent());
        if (OB_FAIL(construct_extra_infos_to_build_migrate_task(
                        dr_ls_info,
                        *ls_replica,
                        *unit_stat_info,
                        *unit_in_group_stat_info,
                        dst_member,
                        src_member,
                        tenant_id,
                        ls_id,
                        task_id,
                        data_source,
                        data_size,
                        dst_replica,
                        skip_change_member_list,
                        old_paxos_replica_number))) {
          LOG_WARN("fail to construct extra infos to build migrate task", KR(ret));
        } else if (only_for_display) {
          ObLSReplicaTaskDisplayInfo display_info;
          if (OB_FAIL(display_info.init(
                        tenant_id,
                        ls_id,
                        ObDRTaskType::LS_MIGRATE_REPLICA,
                        ObDRTaskPriority::HIGH_PRI,
                        unit_stat_info->get_unit_info().unit_.server_,
                        ls_replica->get_replica_type(),
                        old_paxos_replica_number,
                        ls_replica->get_server(),
                        ls_replica->get_replica_type(),
                        old_paxos_replica_number,
                        unit_stat_info->get_unit_info().unit_.server_,
                        "replicate to unit task"))) {
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
                               *server_stat_info,
                               dr_ls_info,
                               task_key,
                               can_generate))) {
          LOG_WARN("fail to check can generate replicate to unit task", KR(ret));
        } else if (can_generate) {
          if (OB_FAIL(generate_replicate_to_unit_and_push_into_task_manager(
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
                          acc_dr_task))) {
            LOG_WARN("fail to generate replicate to unit task", KR(ret));
          }
        }
      }
    }
  }
  LOG_INFO("finish try replicate to unit", KR(ret), K(acc_dr_task));
  return ret;
}

int ObDRWorker::try_generate_remove_paxos_locality_alignment_task(
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
    const RemovePaxosLATask *my_task = reinterpret_cast<const RemovePaxosLATask *>(task);
    ObReplicaMember remove_member(my_task->remove_server_,
                                  my_task->member_time_us_,
                                  my_task->replica_type_,
                                  my_task->memstore_percent_);
    ObRemoveLSPaxosReplicaTask remove_paxos_task;
    bool has_leader = false;
    common::ObAddr leader_addr;
    share::ObTaskId task_id;
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
            "remove redundant paxos replica according to locality",
            leader_addr,
            remove_member,
            my_task->orig_paxos_replica_number_,
            my_task->paxos_replica_number_))) {
      LOG_WARN("fail to build task", KR(ret));
    } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_paxos_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else {
      LOG_INFO("success to add a ObRemoveLSPaxosReplicaTask to task manager", KR(ret), K(remove_paxos_task));
      acc_dr_task++;
    }
  }
  return ret;
}

int ObDRWorker::try_generate_remove_non_paxos_locality_alignment_task(
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
    const RemoveNonPaxosLATask *my_task = reinterpret_cast<const RemoveNonPaxosLATask *>(task);
    ObReplicaMember remove_member(my_task->remove_server_,
                                  my_task->member_time_us_,
                                  my_task->replica_type_,
                                  my_task->memstore_percent_);
    ObRemoveLSNonPaxosReplicaTask remove_non_paxos_task;
    share::ObTaskId task_id;
    if (FALSE_IT(task_id.init(self_addr_))) {
      //shall never be here
    } else if (OB_FAIL(remove_non_paxos_task.build(
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
            ObDRTaskPriority::LOW_PRI,
            "remove redundant non paxos replica according to locality",
            remove_member))) {
      LOG_WARN("fail to build task", KR(ret));
    } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_non_paxos_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else {
      LOG_INFO("success to add a ObRemoveLSNonPaxosReplicaTask to task manager", KR(ret), K(remove_non_paxos_task));
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
    if (FALSE_IT(task_id.init(self_addr_))) {
      //shall never be here
    } else if (OB_FAIL(choose_disaster_recovery_data_source(
            zone_mgr_,
            server_mgr_,
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
            "add paxos replica according to locality",
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
            server_mgr_,
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
            "type transform according to locality",
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
            member_list))) {
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
            "modify paxos replica number according to locality",
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
  if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(task));
  } else if (OB_FAIL(generate_task_key(dr_ls_info, task_key))) {
    LOG_WARN("fail to generate task key", KR(ret));
  } else {
    switch (task->get_task_type()) {
      case RemovePaxos: {
        if (OB_FAIL(try_generate_remove_paxos_locality_alignment_task(
                dr_ls_info,
                task_key,
                task,
                acc_dr_task_cnt))) {
          LOG_WARN("fail to try generate remove paxos task",
                    KR(ret), K(task_key), KPC(task));
        }
        break;
      }
      case RemoveNonPaxos: {
        if (OB_FAIL(try_generate_remove_non_paxos_locality_alignment_task(
                dr_ls_info,
                task_key,
                task,
                acc_dr_task_cnt))) {
          LOG_WARN("fail to try generate remove non paxos task",
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
      case RemovePaxos: {
        const RemovePaxosLATask *my_task = reinterpret_cast<const RemovePaxosLATask *>(task);
        task_type = ObDRTaskType::LS_REMOVE_PAXOS_REPLICA;
        source_replica_type = REPLICA_TYPE_MAX;
        target_replica_type = my_task->replica_type_;
        task_priority = ObDRTaskPriority::HIGH_PRI;
        target_svr = my_task->remove_server_;
        execute_svr = leader_addr;
        source_replica_paxos_replica_number = my_task->orig_paxos_replica_number_;
        target_replica_paxos_replica_number = my_task->paxos_replica_number_;
        comment = "remove redundant paxos replica according to locality";
        break;
      }
      case RemoveNonPaxos: {
        const RemoveNonPaxosLATask *my_task = reinterpret_cast<const RemoveNonPaxosLATask *>(task);
        task_type = ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
        source_replica_type = REPLICA_TYPE_MAX;
        target_replica_type = my_task->replica_type_;
        task_priority = ObDRTaskPriority::LOW_PRI;
        target_svr = my_task->remove_server_;
        execute_svr = my_task->remove_server_;
        source_replica_paxos_replica_number = OB_INVALID_COUNT;
        target_replica_paxos_replica_number = OB_INVALID_COUNT;
        comment = "remove redundant non paxos replica according to locality";
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
            server_mgr_,
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
          comment = "add paxos replica according to locality";
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
            server_mgr_,
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
          comment = "type transform according to locality";
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
        comment = "modify paxos replica number according to locality";
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
      LOG_WARN("fail to init a ObLSReplicaTaskDisplayInfo", KR(ret));
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
  LocalityAlignment locality_alignment(unit_mgr_,
                                       server_mgr_,
                                       zone_mgr_,
                                       dr_ls_info);
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
      DRServerStatInfo server_stat_info; //useless
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
                               server_stat_info,
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
  UNUSED(only_for_display);
  UNUSED(dr_ls_info);
  UNUSED(acc_dr_task);
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
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (is_paxos_replica_related) {
      ObRemoveLSPaxosReplicaTask remove_member_task;
      if (OB_FAIL(remove_member_task.build(
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
                  "cancel migrate unit remove paxos replica",
                  leader_addr,
                  remove_member,
                  old_paxos_replica_number,
                  new_paxos_replica_number))) {
        LOG_WARN("fail to build remove member task", KR(ret));
      } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_member_task))) {
        LOG_WARN("fail to add task", KR(ret), K(remove_member_task));
      } else {
        ++acc_dr_task;
      }
    } else {
      ObRemoveLSNonPaxosReplicaTask remove_non_paxos_task;
      if (OB_FAIL(remove_non_paxos_task.build(
                  task_key,
                  tenant_id,
                  ls_id,
                  task_id,
                  0,/*schedule_time*/
                  0,/*generate_time*/
                  GCONF.cluster_id,
                  0/*transmit_data_size*/,
                  obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,
                  true,/*skip change member list*/
                  ObDRTaskPriority::LOW_PRI,
                  "cancel migrate unit remove non paxos replica",
                  remove_member))) {
        LOG_WARN("fail to build remove member task", KR(ret));
      } else if (OB_FAIL(disaster_recovery_task_mgr_->add_task(remove_non_paxos_task))) {
        LOG_WARN("fail to add task", KR(ret), K(remove_non_paxos_task));
      } else {
        ++acc_dr_task;
      }
    }
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
        uint64_t tenant_id;
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
                        is_paxos_replica_related
                        ? ObDRTaskType::LS_REMOVE_PAXOS_REPLICA
                        : ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA,
                        is_paxos_replica_related
                        ? ObDRTaskPriority::HIGH_PRI
                        : ObDRTaskPriority::LOW_PRI,
                        ls_replica->get_server(),
                        ls_replica->get_replica_type(),
                        new_paxos_replica_number,
                        source_svr,
                        REPLICA_TYPE_MAX,
                        old_paxos_replica_number,
                        leader_addr,
                        is_paxos_replica_related
                        ? "cancel migrate unit remove paxos replica"
                        : "cancel migrate unit remove non paxos replica"))) {
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
                               *server_stat_info,
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
                         server_mgr_,
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
                         is_unit_in_group_related
                         ? "migrate replica due to unit group not match"
                         : "migrate replica due to unit not match",
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
        uint64_t tenant_id;
        share::ObLSID ls_id;
        share::ObTaskId task_id;
        ObReplicaMember data_source;
        int64_t data_size = 0;
        ObDstReplica dst_replica;
        bool skip_change_member_list = false;
        int64_t old_paxos_replica_number;
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
                        is_unit_in_group_related
                          ? "migrate replica due to unit group not match"
                          : "migrate replica due to unit not match"))) {
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
                               *server_stat_info,
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
  task_plan.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(task_plan.assign(display_tasks_))) {
    LOG_WARN("fail to get dsplay task stat", KR(ret), K_(display_tasks));
  }
  return ret;
}

int ObDRWorker::add_display_info(const ObLSReplicaTaskDisplayInfo &display_info)
{
  int ret = OB_SUCCESS;
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
  LOG_INFO("begin to generate disaster recovery paxos replica number", K(dr_ls_info),
           K(member_list_cnt), K(curr_paxos_replica_number), K(locality_paxos_replica_number),
           K(member_change_type), K(new_paxos_replica_number));
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
      } else {} // new member cnt greater than paxos_replica_number, not good
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
  return ret;
}

int ObDRWorker::choose_disaster_recovery_data_source(
    ObZoneManager *zone_mgr,
    ObServerManager *server_mgr,
    DRLSInfo &dr_ls_info,
    const ObReplicaMember &dst_member,
    const ObReplicaMember &src_member,
    ObReplicaMember &data_source,
    int64_t &data_size)
{
  int ret = OB_SUCCESS;
  ObZone dst_zone;
  ObRegion dst_region;
  ObDataSourceCandidateChecker type_checker(dst_member.get_replica_type());
  int64_t replica_cnt = 0;
  share::ObLSReplica *ls_replica = nullptr;
  DRServerStatInfo *server_stat_info = nullptr;
  DRUnitStatInfo *unit_stat_info = nullptr;
  DRUnitStatInfo *unit_in_group_stat_info = nullptr;

  if (OB_ISNULL(zone_mgr) || OB_ISNULL(server_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(zone_mgr), KP(server_mgr));
  } else if (OB_FAIL(server_mgr->get_server_zone(dst_member.get_server(), dst_zone))) {
    LOG_WARN("fail to get server zone", KR(ret), "server", dst_member.get_server());
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

} // end namespace rootserver
} // end namespace oceanbase
