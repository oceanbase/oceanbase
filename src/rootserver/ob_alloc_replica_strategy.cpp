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

#define USING_LOG_PREFIX RS
#include "ob_alloc_replica_strategy.h"
#include "rootserver/ob_zone_manager.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_zone_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_info.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"
#include "ob_unit_manager.h"
#include "ob_root_utils.h"
#include "ob_replica_addr.h"
#include "ob_balance_group_data.h"
#include <random>

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;

ObLocalityUtility::ObLocalityUtility(const ObZoneManager& zone_mgr, const share::schema::ZoneLocalityIArray& zloc,
    const common::ObIArray<common::ObZone>& zone_list)
    : zloc_(zloc), zone_tasks_(), shadow_zone_tasks_(), all_server_zones_(), zone_mgr_(zone_mgr), zone_list_(zone_list)
{}

ObLocalityUtility::~ObLocalityUtility()
{}

int ObLocalityUtility::init_zone_task(const bool with_paxos, const bool with_readonly)
{
  int ret = OB_SUCCESS;
  // first:push paxos task
  FOREACH_CNT_X(z, zloc_, OB_SUCCESS == ret)
  {  // for each zone
    const ObReplicaAttrSet& set = z->replica_attr_set_;
    if (OB_SUCC(ret) && with_paxos && set.get_full_replica_num() > 0 && z->zone_set_.count() <= 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < set.get_full_replica_attr_array().count(); ++i) {
        const ReplicaAttr& replica_attr = set.get_full_replica_attr_array().at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < replica_attr.num_; ++j) {
          ZoneTask task(z->zone_, REPLICA_TYPE_FULL, replica_attr.memstore_percent_);
          if (OB_FAIL(task.inner_task_.tmp_compatible_generate(ZoneReplicaDistTask::ReplicaNature::PAXOS,
                  z->zone_,
                  REPLICA_TYPE_FULL,
                  replica_attr.memstore_percent_))) {
            LOG_WARN("fail to tmp compatible generate", KR(ret));
          } else if (OB_FAIL(zone_tasks_.push_back(task))) {
            LOG_WARN("fail to push back", KR(ret), K(i));
          }
        }
      }
    }

    if (OB_SUCC(ret) && with_paxos && set.get_logonly_replica_num() > 0 && z->zone_set_.count() <= 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < set.get_logonly_replica_num(); ++i) {
        ZoneTask task(z->zone_, REPLICA_TYPE_LOGONLY, set.get_logonly_replica_attr_array().at(0).memstore_percent_);
        if (OB_FAIL(task.inner_task_.tmp_compatible_generate(ZoneReplicaDistTask::ReplicaNature::PAXOS,
                z->zone_,
                REPLICA_TYPE_LOGONLY,
                set.get_logonly_replica_attr_array().at(0).memstore_percent_))) {
          LOG_WARN("fail to tmp compatible generate", KR(ret));
        } else if (OB_FAIL(zone_tasks_.push_back(task))) {
          LOG_WARN("fail push task", K(i), KR(ret));
        }
      }
    }

    if (OB_SUCC(ret) && with_paxos && set.has_paxos_replica() && z->zone_set_.count() >= 2) {
      ZoneTask task;
      common::ObArray<rootserver::ObReplicaAddr> empty_addr_array;
      if (OB_FAIL(task.inner_task_.generate(ZoneReplicaDistTask::ReplicaNature::PAXOS, *z, empty_addr_array))) {
        LOG_WARN("fail to generate task", KR(ret));
      } else if (OB_FAIL(zone_tasks_.push_back(task))) {
        LOG_WARN("fail to push task", KR(ret));
      }
    }
  }

  // second:push non paxos
  FOREACH_CNT_X(z, zloc_, OB_SUCCESS == ret)
  {
    const ObReplicaAttrSet& set = z->replica_attr_set_;
    if (OB_SUCC(ret) && with_readonly && set.is_specific_readonly_replica()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < set.get_readonly_replica_num(); ++i) {
        ZoneTask task(z->zone_, REPLICA_TYPE_READONLY, set.get_readonly_replica_attr_array().at(0).memstore_percent_);
        if (OB_FAIL(task.inner_task_.tmp_compatible_generate(ZoneReplicaDistTask::ReplicaNature::NON_PAXOS,
                z->zone_,
                REPLICA_TYPE_READONLY,
                set.get_readonly_replica_attr_array().at(0).memstore_percent_))) {
          LOG_WARN("fail to tmp compatible generate", KR(ret));
        } else if (OB_FAIL(zone_tasks_.push_back(task))) {
          LOG_WARN("fail push task", K(i), KR(ret));
        }
      }
    }

    // process R{all_server}@[z1...]
    if (OB_SUCC(ret) && with_readonly && set.is_allserver_readonly_replica()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < z->zone_set_.count(); ++i) {
        if (has_exist_in_array(all_server_zones_, z->zone_set_.at(i))) {
          // bypass
        } else if (OB_FAIL(all_server_zones_.push_back(z->zone_set_.at(i)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityUtility::init(bool with_paxos, bool with_readonly)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_zone_task(with_paxos, with_readonly))) {
    LOG_WARN("fail to init zone task", KR(ret));
  }
  return ret;
}

void ObLocalityUtility::reset()
{
  zone_tasks_.reset();
  shadow_zone_tasks_.reset();
  all_server_zones_.reset();
}
///////////////////////////////////

ObAllocReplicaByLocality::ObAllocReplicaByLocality(const ObZoneManager& zone_mgr,
    ObZoneUnitsProvider& zone_units_provider, const share::schema::ZoneLocalityIArray& zloc,
    obrpc::ObCreateTableMode create_mode, const common::ObIArray<common::ObZone>& zone_list)
    : ObLocalityUtility(zone_mgr, zloc, zone_list),
      create_mode_(create_mode),
      zone_units_provider_(zone_units_provider),
      unit_set_(),
      zone_task_idx_(0),
      saved_zone_pos_(0),
      need_update_pg_cnt_(false)
{}

ObAllocReplicaByLocality::~ObAllocReplicaByLocality()
{}

int ObAllocReplicaByLocality::init(bool with_paxos, bool with_readonly)
{
  int ret = OB_SUCCESS;
  saved_zone_pos_ = 0;
  zone_task_idx_ = 0;

  if (OB_FAIL(unit_set_.create(MAX_SERVER_COUNT))) {
    LOG_WARN("fail create server set", K(MAX_SERVER_COUNT), KR(ret));
  } else if (OB_FAIL(ObLocalityUtility::init(with_paxos, with_readonly))) {
    LOG_WARN("fail to init locality utility", KR(ret));
  }
  return ret;
}

void ObAllocReplicaByLocality::reset()
{
  saved_zone_pos_ = 0;
  zone_task_idx_ = 0;
  unit_set_.destroy();
  ObLocalityUtility::reset();
}

// algorithm: Get all available units in the zone,
//            Find the first server that has not been assigned a replica,
//            and assign its corresponding unit to replica
int ObAllocReplicaByLocality::alloc_replica_in_zone(const int64_t unit_offset, const common::ObZone& zone,
    const common::ObReplicaType replica_type, const int64_t memstore_percent, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  const ObZoneUnitAdaptor* zu = NULL;
  if (OB_FAIL(zone_units_provider_.find_zone(zone, zu))) {
    // nop
  } else if (NULL == zu) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no avaliable zone for replica allocation request", KR(ret));
  } else if (0 >= zu->count()) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no available unit to alloc replica, maybe migrate blocked", KR(ret), K(zone));
  } else {
    int64_t idx = -1;
    ObZoneUnitAdaptor* my_zu = const_cast<ObZoneUnitAdaptor*>(zu);
    ret = my_zu->get_target_unit_idx(unit_offset, unit_set_, need_update_pg_cnt_, idx);
    if (OB_SUCCESS == ret) {
      if (idx < 0 || idx >= my_zu->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx unexpected", KR(ret), K(idx), "unit_cnt", my_zu->count(), K(zone));
      } else {
        replica_addr.reset();
        const ObUnit& unit = my_zu->at(idx)->unit_;
        replica_addr.unit_id_ = unit.unit_id_;
        replica_addr.addr_ = unit.server_;
        replica_addr.zone_ = unit.zone_;
        replica_addr.replica_type_ = replica_type;
        // overwrite ret
        if (OB_FAIL(replica_addr.set_memstore_percent(memstore_percent))) {
          LOG_WARN("fai lto set memstore percent", KR(ret));
        } else if (OB_FAIL(unit_set_.set_refactored(replica_addr.unit_id_))) {
          LOG_WARN("fail set server to server set", K(replica_addr), KR(ret), K(zone));
        } else if (OB_FAIL(my_zu->update_tg_pg_count(idx, need_update_pg_cnt_))) {
          LOG_WARN("fail to update tg pg count", KR(ret));
        }
      }
    } else if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
      LOG_INFO("not enough server to hold more replica",
          K(replica_type),
          "allocated_server_cnt",
          unit_set_.size(),
          KR(ret),
          K(zone));
    } else {
      // other error
      LOG_WARN("fail alloc replica", KR(ret), K(zone));
    }
  }
  return ret;
}

ObCreateTableReplicaByLocality::ObCreateTableReplicaByLocality(const ObZoneManager& zone_mgr,
    ObZoneUnitsProvider& zone_units_provider, const share::schema::ZoneLocalityIArray& zloc,
    obrpc::ObCreateTableMode create_mode, const common::ObIArray<common::ObZone>& zone_list, const int64_t seed,
    balancer::ObSinglePtBalanceContainer* pt_balance_container,
    common::ObSEArray<common::ObZone, 7>* high_priority_zone_array)
    : ObAllocReplicaByLocality(zone_mgr, zone_units_provider, zloc, create_mode, zone_list),
      pt_balance_container_(pt_balance_container),
      high_priority_zone_array_(high_priority_zone_array),
      curr_part_zone_array_(),
      curr_part_task_array_(),
      curr_part_task_idx_(0),
      seed_(seed)
{}

int ObCreateTableReplicaByLocality::init()
{
  int ret = OB_SUCCESS;

  const bool with_paxos = true;
  const bool with_readonly = true;
  seed_ = ObLocalityUtil::gen_locality_seed(seed_);
  if (nullptr != high_priority_zone_array_) {
    std::sort(high_priority_zone_array_->begin(), high_priority_zone_array_->end());
  }
  if (OB_FAIL(ObAllocReplicaByLocality::init(with_paxos, with_readonly))) {
    LOG_WARN("fail to init base class in ObCreateTableReplicaByLocality", KR(ret));
  }

  return ret;
}

int ObCreateTableReplicaByLocality::prepare_for_next_partition(const ObPartitionAddr& paddr)
{
  int ret = OB_SUCCESS;
  // These datas are reset before each partition is assigned replica
  zone_task_idx_ = 0;
  unit_set_.reuse();
  shadow_zone_tasks_.reuse();

  if (OB_FAIL(shadow_zone_tasks_.assign(zone_tasks_))) {
    LOG_WARN("fail to assign shadow zone tasks", KR(ret));
  } else {
    // put all in unit set
    for (int64_t i = 0; OB_SUCC(ret) && i < paddr.count(); ++i) {
      const ObReplicaAddr& replica_addr = paddr.at(i);
      int tmp_ret = unit_set_.set_refactored(replica_addr.unit_id_, 0 /* don't overwrite*/);
      if (OB_SUCCESS == tmp_ret) {
        // good, set_refactored succeed
      } else if (OB_HASH_EXIST == tmp_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("more than one replica in the same unit, unexpected", KR(ret), K(replica_addr));
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to set server to server set", KR(ret), K(replica_addr));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < paddr.count(); ++i) {
      const ObReplicaAddr& replica_addr = paddr.at(i);
      bool found = false;
      for (int64_t j = 0; !found && OB_SUCC(ret) && j < shadow_zone_tasks_.count(); ++j) {
        ZoneTask& dist_task = shadow_zone_tasks_.at(j);
        bool has_task = false;
        if (OB_FAIL(dist_task.inner_task_.check_has_task(
                replica_addr.zone_, replica_addr.replica_type_, replica_addr.get_memstore_percent(), has_task))) {
          LOG_WARN("fail to check has task",
              KR(ret),
              "zone",
              replica_addr.zone_,
              "replica_type",
              replica_addr.replica_type_,
              "memstore_percent",
              replica_addr.get_memstore_percent());
        } else if (!has_task) {
          // by pass
        } else if (OB_FAIL(dist_task.inner_task_.erase_task(
                       replica_addr.zone_, replica_addr.get_memstore_percent(), replica_addr.replica_type_))) {
          LOG_WARN(
              "fail to erase task", KR(ret), "zone", replica_addr.zone_, "replica_type", replica_addr.replica_type_);
        } else {
          found = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(zone_units_provider_.prepare_for_next_partition(unit_set_))) {
        LOG_WARN("zone unit provider fail to prepare for next partition", KR(ret));
      } else if (shadow_zone_tasks_.count() <= 0) {
        // bypass
      } else if (OB_FAIL(prepare_for_next_task(shadow_zone_tasks_.at(0)))) {
        LOG_WARN("fail to prepare for next task", KR(ret));
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::get_next_replica(const ObPartitionKey& pkey,
    ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const bool non_partition_table, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  do {
    ret = OB_SUCCESS;
    if (zone_task_idx_ < shadow_zone_tasks_.count()) {
      if (curr_part_task_idx_ >= curr_part_task_array_.count()) {
        ++zone_task_idx_;
        if (zone_task_idx_ >= shadow_zone_tasks_.count()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(prepare_for_next_task(shadow_zone_tasks_.at(zone_task_idx_)))) {
          LOG_WARN("fail to prepare for next task", KR(ret));
        } else {
          ret = get_next_replica(pkey, ten_unit_arr, non_partition_table, replica_addr);
          if (OB_SUCC(ret)) {
            // good
          } else if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next replica", KR(ret));
          }
        }
      } else {
        const SingleReplica& single_replica = curr_part_task_array_.at(curr_part_task_idx_);
        if (curr_part_zone_array_.count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected zone array count", KR(ret));
        } else if (curr_part_zone_array_.count() == 1) {
          if (OB_FAIL(alloc_single_zone_task(pkey, single_replica, ten_unit_arr, non_partition_table, replica_addr))) {
            LOG_WARN("fail to alloc single zone task", KR(ret));
          }
        } else {
          if (OB_FAIL(
                  alloc_multiple_zone_task(pkey, single_replica, ten_unit_arr, non_partition_table, replica_addr))) {
            LOG_WARN("fail to alloc multiple zone task", KR(ret));
          }
        }
        if (OB_SUCCESS == ret || OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
          ++curr_part_task_idx_;
          LOG_INFO("alloc replica", KR(ret), K(pkey), K(replica_addr));
        }
      }
    } else {
      ret = OB_ITER_END;
    }
  } while (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret);
  return ret;
}

int ObCreateTableReplicaByLocality::map_y_axis(
    const int64_t axis, const int64_t scope, const int64_t bucket_num, int64_t& map_axis)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    int64_t idx = -1;
    int64_t offset = -1;
    const int64_t min_itl = scope / bucket_num;
    const int64_t max_itl = (scope == min_itl * bucket_num ? min_itl : min_itl + 1);
    const int64_t max_itl_cnt = scope - min_itl * bucket_num;
    const int64_t min_itl_cnt = ((min_itl == 0) ? 0 : ((scope - max_itl * max_itl_cnt) / min_itl));
    int64_t base_itl = axis / max_itl;
    if (base_itl < max_itl_cnt) {
      idx = base_itl;
      offset = axis % max_itl;
    } else {
      const int64_t base_sum = max_itl * max_itl_cnt;
      const int64_t remain = axis - base_sum;
      idx = max_itl_cnt + remain / min_itl;
      offset = remain % min_itl;
    }
    map_axis = offset * bucket_num + idx;
  }
  return ret;
}

int ObCreateTableReplicaByLocality::recalculate_y_axis(
    const int64_t axis, const int64_t scope, const int64_t bucket_num, int64_t& recal_axis)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    int64_t sum_base = -1;
    const int64_t min_itl = scope / bucket_num;
    const int64_t max_itl = (scope == min_itl * bucket_num ? min_itl : min_itl + 1);
    const int64_t max_itl_cnt = scope - min_itl * bucket_num;
    const int64_t min_itl_cnt = ((min_itl == 0) ? 0 : ((scope - max_itl * max_itl_cnt) / min_itl));
    const int64_t new_axis = axis % scope;
    const int64_t group_idx = new_axis % bucket_num;
    const int64_t in_group_offset = new_axis / bucket_num;
    if (group_idx >= max_itl_cnt) {
      sum_base = (group_idx - max_itl_cnt) * min_itl + max_itl_cnt * max_itl;
    } else {
      sum_base = group_idx * max_itl;
    }
    recal_axis = in_group_offset + sum_base;
  }
  return ret;
}

int ObCreateTableReplicaByLocality::get_xy_index(const bool is_multiple_zone, const common::ObPartitionKey& pkey,
    const common::ObIArray<common::ObZone>& zone_array, int64_t& x_index, int64_t& y_index)
{
  int ret = OB_SUCCESS;
  balancer::HashIndexMapItem hash_index_item;
  if (nullptr == pt_balance_container_) {
    x_index = seed_ + curr_part_task_idx_;
    y_index = seed_;
  } else {
    int tmp_ret = pt_balance_container_->get_hash_index().get_partition_index(pkey, hash_index_item);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      x_index = seed_ + curr_part_task_idx_;
      y_index = seed_;
    } else if (OB_SUCCESS == tmp_ret) {
      int64_t x_axis = -1;
      int64_t y_axis = -1;
      int64_t y_offset = -1;
      int64_t y_capacity = -1;
      int64_t recal_y_axis = -1;
      common::ObZone target_zone;  // target zone maybe empty at last
      common::ObZone my_zone;
      const int64_t curr_part_zone_array_cnt = curr_part_zone_array_.count();
      const ObZoneUnitAdaptor* zu = nullptr;
      if (OB_UNLIKELY(curr_part_zone_array_cnt <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur part zone array cnt unexpected", KR(ret));
      } else if (OB_UNLIKELY(!hash_index_item.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hash index item is invalid", KR(ret), K(hash_index_item));
      } else if (OB_FAIL(hash_index_item.get_balance_group_zone_x_axis(zone_array.count(), x_axis))) {
        LOG_WARN("fail to get balance group zone axis", KR(ret));
      } else if (OB_FAIL(hash_index_item.get_balance_group_zone_y_axis(
                     is_multiple_zone, zone_array.count(), curr_part_task_idx_, y_axis, y_capacity, y_offset))) {
        LOG_WARN("fail to get balance group zone y axis", KR(ret));
      } else if (FALSE_IT(x_index = x_axis + curr_part_task_idx_)) {
        // shall never be here
      } else if (FALSE_IT(my_zone = curr_part_zone_array_.at(x_index % curr_part_zone_array_cnt))) {
        // shall never by here
      } else if (nullptr == high_priority_zone_array_ || !has_exist_in_array(*high_priority_zone_array_, my_zone)) {
        // This zone is not in the highest priority zone array, so target_zone is considered empty.
      } else if (OB_FAIL(hash_index_item.get_balance_group_zone(*high_priority_zone_array_, target_zone))) {
        LOG_WARN("fail to get balance group zone", KR(ret));
      }

      if (OB_FAIL(ret)) {
        // failed, bypass
      } else if (target_zone.is_empty()) {
        y_index = seed_ + y_offset + y_axis;
      } else if (OB_FAIL(zone_units_provider_.find_zone(target_zone, zu))) {
        LOG_WARN("fail to find zone", KR(ret), K(target_zone));
      } else if (OB_UNLIKELY(nullptr == zu)) {
        LOG_WARN("target zone unavailable", K(target_zone));
        y_index = seed_ + y_offset + y_axis;
      } else {
        int64_t mapped_y_axis = -1;
        if (OB_FAIL(map_y_axis(y_axis, y_capacity, zu->count(), mapped_y_axis))) {
          LOG_WARN("fail to map y axis", KR(ret));
        } else if (target_zone == my_zone) {
          y_index = seed_ + y_offset + mapped_y_axis;
        } else if (GCONF.__enable_identical_partition_distribution) {
          y_index = seed_ + y_offset + mapped_y_axis;
        } else if (OB_FAIL(recalculate_y_axis(mapped_y_axis, y_capacity, zu->count(), recal_y_axis))) {
          LOG_WARN("fail to recalculate y axis", KR(ret));
        } else {
          y_index = seed_ + y_offset + recal_y_axis;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get hash index unexpected", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_single_zone_paxos_replica(
    const SingleReplica& single_replica, const common::ObZone& zone, const int64_t y_index, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  const ObZoneUnitAdaptor* zu = nullptr;
  const ObUnitInfo* unit_info = nullptr;
  if (OB_FAIL(zone_units_provider_.find_zone(zone, zu))) {
    LOG_WARN("fail to find zone", KR(ret));
  } else if (nullptr == zu) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no avaliable zone for replica allocation request", KR(ret), K(zone));
  } else if (zu->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone unit count", KR(ret), K(zone));
  } else {
    int64_t my_y_index = y_index;
    const int64_t guard = my_y_index % zu->count();
    do {
      ret = OB_SUCCESS;
      if (nullptr == (unit_info = zu->at(my_y_index % zu->count()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit info ptr is null", KR(ret), KP(unit_info));
      } else {
        ret = unit_set_.exist_refactored(unit_info->unit_.unit_id_);
        if (OB_HASH_EXIST == ret) {
          // already has replica no this unit;
          ++my_y_index;
        } else if (OB_HASH_NOT_EXIST != ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica already exist on unit", KR(ret), "unit_id", unit_info->unit_.unit_id_);
        } else if (OB_FAIL(unit_set_.set_refactored(unit_info->unit_.unit_id_))) {  // rewrite ret
          LOG_WARN("fail to set refactored", KR(ret));
        } else if (OB_FAIL(replica_addr.set_memstore_percent(single_replica.memstore_percent_))) {
          LOG_WARN("fail to set memstore percent", KR(ret));
        } else {
          replica_addr.replica_type_ = single_replica.replica_type_;
          replica_addr.unit_id_ = unit_info->unit_.unit_id_;
          replica_addr.addr_ = unit_info->unit_.server_;
          replica_addr.zone_ = unit_info->unit_.zone_;
          break;
        }
      }
    } while (OB_HASH_EXIST == ret && my_y_index % zu->count() != guard);

    if (OB_HASH_EXIST == ret) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_WARN("not enough server to hold more replica", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_single_zone_nonpaxos_replica(
    const SingleReplica& single_replica, const common::ObZone& zone, const int64_t y_index, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  const ObZoneUnitAdaptor* zu = nullptr;
  const ObUnitInfo* unit_info = nullptr;
  if (OB_FAIL(zone_units_provider_.find_zone(zone, zu))) {
    LOG_WARN("fail to find zone", KR(ret));
  } else if (nullptr == zu) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no avaliable zone for replica allocation request", KR(ret), K(zone));
  } else if (zu->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone unit count", KR(ret), K(zone));
  } else {
    int64_t idx = y_index % zu->count();
    const int64_t guard = idx;
    do {
      if (nullptr == (unit_info = zu->at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", KR(ret), KP(unit_info));
      } else if (OB_HASH_EXIST == (ret = unit_set_.exist_refactored(unit_info->unit_.unit_id_))) {
        ++idx;
        idx %= zu->count();
      }
    } while (OB_HASH_EXIST == ret && idx != guard);

    if (OB_HASH_NOT_EXIST == ret) {
      // write ret
      if (OB_FAIL(unit_set_.set_refactored(unit_info->unit_.unit_id_))) {
        LOG_WARN("fail to set refactored", KR(ret));
      } else if (OB_FAIL(replica_addr.set_memstore_percent(single_replica.memstore_percent_))) {
        LOG_WARN("fail to set memstore percent", KR(ret));
      } else {
        replica_addr.replica_type_ = single_replica.replica_type_;
        replica_addr.unit_id_ = unit_info->unit_.unit_id_;
        replica_addr.addr_ = unit_info->unit_.server_;
        replica_addr.zone_ = unit_info->unit_.zone_;
      }
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_WARN("not enough server to hold more replica", KR(ret), K(zone));
    } else {
      LOG_WARN("fail to alloc replica", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_multiple_zone_paxos_replica(const SingleReplica& single_replica,
    const common::ObIArray<common::ObZone>& zone_array, const int64_t x_index, const int64_t y_index,
    ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  const ObZoneUnitAdaptor* zu = nullptr;
  const ObUnitInfo* unit_info = nullptr;
  const int64_t count = zone_array.count();
  const common::ObZone& zone = zone_array.at(x_index % count);
  if (OB_FAIL(zone_units_provider_.find_zone(zone, zu))) {
    LOG_WARN("fail to find zone", KR(ret));
  } else if (nullptr == zu) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no avaliable zone for replica allocation request", KR(ret), K(zone));
  } else if (zu->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone unit count", KR(ret), K(zone));
  } else if (nullptr == (unit_info = zu->at(y_index % zu->count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit info ptr is null", KR(ret), KP(unit_info));
  } else {
    int tmp_ret = unit_set_.exist_refactored(unit_info->unit_.unit_id_);
    if (OB_HASH_NOT_EXIST != tmp_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica already exist on unit", KR(ret), "unit_id", unit_info->unit_.unit_id_);
    } else if (OB_FAIL(unit_set_.set_refactored(unit_info->unit_.unit_id_))) {
      LOG_WARN("fail to set refactored", KR(ret));
    } else if (OB_FAIL(replica_addr.set_memstore_percent(single_replica.memstore_percent_))) {
      LOG_WARN("fail to set memstore percent", KR(ret));
    } else {
      replica_addr.replica_type_ = single_replica.replica_type_;
      replica_addr.unit_id_ = unit_info->unit_.unit_id_;
      replica_addr.addr_ = unit_info->unit_.server_;
      replica_addr.zone_ = unit_info->unit_.zone_;
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_non_part_single_zone_replica(const SingleReplica& single_replica,
    const common::ObPartitionKey& pkey, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  const common::ObZone& zone = curr_part_zone_array_.at(0);
  ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM> unit_array;
  if (OB_FAIL(zone_units_provider_.get_all_ptr_zone_units(unit_array))) {
    LOG_WARN("fail to get all zone units", KR(ret));
  } else if (OB_FAIL(do_alloc_non_part_replica(single_replica, pkey, unit_array, zone, ten_unit_arr, replica_addr))) {
    LOG_WARN("fail to do alloc non part replica", KR(ret));
  }
  return ret;
}

/*
 *  @descriptions:
 *    Used for replica locality setting of single partition, non-partition,
 *    single partition tablegroup, and non-partition tablegroup type
 *  @param[in] single_replica: replica info
 *             pkey: pkey of replica
 *             unit_array: Information of available units on all zones of this tenant
 *             zone: which zone the replica is in
 *             tenant_unit_arr: Infromations about the number of replicas of each type,
 *                              and the leaders count in all units of this tenant.
 *                              Using this value to determine which unit the new replica id placed on
 *  @param[out] replica_addr: replica location
 */
int ObCreateTableReplicaByLocality::do_alloc_non_part_replica(const SingleReplica& single_replica,
    const common::ObPartitionKey& pkey, const common::ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM>& unit_array,
    const common::ObZone& zone, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnitInfo*> unit_ptr_arr;
  common::ObArray<common::ObZone> zone_list;
  ObArray<TenantUnitRepCnt*> leader_rep_unit_arr;
  bool find_unit = false;
  // According to unit_array to obtain all unit information in this tenant is stored in unit_ptr_arr
  if (OB_FAIL(prepare_replica_info(unit_array, zone, unit_ptr_arr, zone_list))) {
    LOG_WARN("fail to prepare replica info", KR(ret));
  } else if (unit_ptr_arr.count() == 0) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("unit is null", KR(ret), K(pkey), K(zone), K(single_replica));
  } else if (FALSE_IT(std::sort(zone_list.begin(), zone_list.end()))) {
    // never be here
  } else if (OB_FAIL(get_replica_addr_array(
                 single_replica, ten_unit_arr, zone, leader_rep_unit_arr, pkey, unit_ptr_arr, zone_list))) {
    // According to ten_unit_arr and unit_ptr_arr to obtain the leader_rep_unit_arr,
    // leader_rep_unit_arr is a subset of unit_ptr_arr
    LOG_WARN("fail to get replica addr array", KR(ret), K(single_replica), K(pkey), K(zone));
  } else if (OB_FAIL(gen_replica_addr_by_leader_arr(
                 single_replica, unit_ptr_arr, leader_rep_unit_arr, replica_addr, find_unit))) {
    LOG_WARN("fail to gen replica addr by leader arr", KR(ret), K(single_replica), K(pkey), K(zone));
  } else if (find_unit) {
    // nothing todo
  } else if (unit_ptr_arr.count() == leader_rep_unit_arr.count()) {
    // there is no valid unit in leader_rep_unit_arr.
    // At this time, if the two numbers are equal,
    // it means that there is no suitable unit to end directly
    // Otherwise, randomly select a valid unit in unit_ptr_arr
  } else if (OB_FAIL(gen_replica_addr_by_unit_arr(single_replica, unit_ptr_arr, replica_addr, find_unit))) {
    LOG_WARN("fail to gen replica addr by unit arr", KR(ret), K(single_replica), K(pkey), K(zone));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to do alloc non part replica",
        KR(ret),
        K(single_replica),
        K(pkey),
        K(zone),
        "unit_count",
        unit_ptr_arr.count(),
        "leader_unit_count",
        leader_rep_unit_arr.count());
  } else if (!find_unit) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("not enough server to hold more replica",
        KR(ret),
        K(single_replica),
        K(pkey),
        K(zone),
        "unit_count",
        unit_ptr_arr.count(),
        "leader_unit_count",
        leader_rep_unit_arr.count());
  }
  return ret;
}

int ObCreateTableReplicaByLocality::gen_replica_addr_by_leader_arr(const SingleReplica& single_replica,
    const common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr,
    const common::ObIArray<TenantUnitRepCnt*>& leader_rep_unit_arr, ObReplicaAddr& replica_addr, bool& find_unit)
{
  int ret = OB_SUCCESS;
  find_unit = false;
  for (int64_t i = 0; i < leader_rep_unit_arr.count() && OB_SUCC(ret) && (!find_unit); ++i) {
    uint64_t unit_id = 0;
    if (OB_ISNULL(leader_rep_unit_arr.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader rep unit is null", KR(ret), K(i));
    } else if (FALSE_IT(unit_id = leader_rep_unit_arr.at(i)->unit_id_)) {
      // never be here
    } else if (FALSE_IT(ret = unit_set_.exist_refactored(unit_id))) {
      // never be here
    } else if (OB_HASH_EXIST == ret) {
      // replica already exist on this unit
      // This error is evaded by find_unit to prevent other functions from misreporting the error code
      ret = OB_SUCCESS;
      LOG_INFO("replica already exist on this unit", KR(ret), K(single_replica), K(unit_id));
    } else if (OB_HASH_NOT_EXIST != ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check unit in unit_set failed", KR(ret), K(single_replica), K(unit_id));
    } else if (OB_FAIL(unit_set_.set_refactored(unit_id))) {  // rewrite ret
      LOG_WARN("fail to set refactored", KR(ret), K(unit_id));
    } else if (OB_FAIL(replica_addr.set_memstore_percent(single_replica.memstore_percent_))) {
      LOG_WARN("fail to set memstore percent", KR(ret), K(single_replica), K(unit_id));
    } else {
      int64_t index = 0;
      for (; index < unit_ptr_arr.count() && OB_SUCC(ret) && (!find_unit); ++index) {
        const share::ObUnitInfo* unit_info = unit_ptr_arr.at(index);
        if (OB_ISNULL(unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", KR(ret));
        } else if (unit_info->unit_.unit_id_ == unit_id) {
          replica_addr.replica_type_ = single_replica.replica_type_;
          replica_addr.unit_id_ = unit_info->unit_.unit_id_;
          replica_addr.addr_ = unit_info->unit_.server_;
          replica_addr.zone_ = unit_info->unit_.zone_;
          find_unit = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (index == unit_ptr_arr.count() && !find_unit) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not find unit id",
              KR(ret),
              K(single_replica),
              K(unit_id),
              "unit_rep",
              *(leader_rep_unit_arr.at(i)),
              K(unit_ptr_arr.count()));
        }
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::gen_replica_addr_by_unit_arr(const SingleReplica& single_replica,
    const common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr, ObReplicaAddr& replica_addr, bool& find_unit)
{
  int ret = OB_SUCCESS;
  find_unit = false;
  for (int64_t index = 0; index < unit_ptr_arr.count() && OB_SUCC(ret) && (!find_unit); ++index) {
    const share::ObUnitInfo* unit_info = unit_ptr_arr.at(index);
    if (OB_ISNULL(unit_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit info is null", KR(ret));
    } else if (FALSE_IT(ret = unit_set_.exist_refactored(unit_info->unit_.unit_id_))) {
      // never be here
    } else if (OB_HASH_EXIST == ret) {
      // replica already exist on this unit
      ret = OB_SUCCESS;
      LOG_INFO("replica already exist on this unit",
          KR(ret),
          K(index),
          K(single_replica),
          "unit_id",
          unit_info->unit_.unit_id_);
    } else if (OB_HASH_NOT_EXIST != ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "check unit in unit_set failed", KR(ret), K(index), K(single_replica), "unit_id", unit_info->unit_.unit_id_);
    } else if (OB_FAIL(unit_set_.set_refactored(unit_info->unit_.unit_id_))) {  // rewrite ret
      LOG_WARN("fail to set refactored", KR(ret));
    } else if (OB_FAIL(replica_addr.set_memstore_percent(single_replica.memstore_percent_))) {
      LOG_WARN(
          "fail to set memstore percent", KR(ret), K(index), K(single_replica), "unit_id", unit_info->unit_.unit_id_);
    } else {
      replica_addr.replica_type_ = single_replica.replica_type_;
      replica_addr.unit_id_ = unit_info->unit_.unit_id_;
      replica_addr.addr_ = unit_info->unit_.server_;
      replica_addr.zone_ = unit_info->unit_.zone_;
      find_unit = true;
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::prepare_replica_info(
    const common::ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM>& unit_array, const common::ObZone& zone,
    common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr, common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> alive_zone_list;
  common::ObArray<uint64_t> unit_ids;
  for (int64_t i = 0; i < unit_array.count() && OB_SUCC(ret); ++i) {
    // Get unit_info on the corresponding zone
    const ObArray<share::ObUnitInfo*>& this_unit_infos = unit_array.at(i);
    for (int64_t j = 0; j < this_unit_infos.count() && OB_SUCC(ret); ++j) {
      share::ObUnitInfo* this_unit_info = this_unit_infos.at(j);
      if (OB_ISNULL(this_unit_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this unit info is null", KR(ret));
      } else if (this_unit_info->unit_.zone_ == zone) {
        if (has_exist_in_array(unit_ids, this_unit_info->unit_.unit_id_)) {
          // nothing todo
        } else if (OB_FAIL(unit_ptr_arr.push_back(this_unit_info))) {
          LOG_WARN("fail to push back", KR(ret), K(*this_unit_info));
        } else if (OB_FAIL(unit_ids.push_back(this_unit_info->unit_.unit_id_))) {
          LOG_WARN("fail to push back", KR(ret), K(*this_unit_info));
        }
      }
      if (OB_FAIL(ret)) {
        // nothing todo
      } else if (has_exist_in_array(alive_zone_list, this_unit_info->unit_.zone_)) {
        // nothing todo
      } else if (OB_FAIL(alive_zone_list.push_back(this_unit_info->unit_.zone_))) {
        LOG_WARN("fail to push back zone", KR(ret), "zone", this_unit_info->unit_.zone_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (curr_part_zone_array_.count() > 1) {  // mix zone
      for (int64_t i = 0; i < curr_part_zone_array_.count() && OB_SUCC(ret); ++i) {
        if (!has_exist_in_array(alive_zone_list, curr_part_zone_array_.at(i))) {
          // there is no usable unit in this zone
        } else if (OB_FAIL(zone_list.push_back(curr_part_zone_array_.at(i)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    } else {
      for (int64_t i = 0; i < zone_tasks_.count() && OB_SUCC(ret); ++i) {
        if (!has_exist_in_array(alive_zone_list, zone_tasks_.at(i).name_)) {
          // there is no usable unit in this zone
        } else if (zone_tasks_.at(i).replica_type_ == REPLICA_TYPE_FULL && zone_tasks_.at(i).memstore_percent_ != 0) {
          if (OB_FAIL(zone_list.push_back(zone_tasks_.at(i).name_))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::get_random_zone_num(
    const int64_t zone_list_count, const uint64_t tenant_id, const int64_t index_zone_id, int64_t& zone_id)
{
  int ret = OB_SUCCESS;
  uint32_t offset_base = 0;
  offset_base = murmurhash2(&tenant_id, sizeof(tenant_id), offset_base);
  offset_base = fnv_hash2(&tenant_id, sizeof(tenant_id), offset_base);
  const int64_t offset = (int64_t)offset_base + index_zone_id;
  const int64_t tmp_zone_id = offset % zone_list_count;
  zone_id = (tmp_zone_id + tenant_id) % zone_list_count;
  return ret;
}

int ObCreateTableReplicaByLocality::get_replica_addr_array(const SingleReplica& single_replica,
    ObIArray<TenantUnitRepCnt*>& ten_unit_arr, const common::ObZone& zone,
    common::ObArray<share::TenantUnitRepCnt*>& leader_rep_unit_arr, const common::ObPartitionKey& pkey,
    common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr, const common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  int64_t zone_id1 = 0;
  bool tmp_index = false;
  const uint64_t tenant_id = pkey.get_tenant_id();
  ObArray<TenantUnitRepCnt*> one_ten_unit_arr;
  // Calculate whether the replica to be allocated will become the leader
  if (ten_unit_arr.count() != 0) {
    if (!is_new_tablegroup_id(pkey.get_tablegroup_id())) {
      zone_id1 = ten_unit_arr.at(0)->unit_rep_cnt_.index_num_;
    } else {
      zone_id1 = ten_unit_arr.at(0)->non_table_cnt_;
    }
  }
  for (int64_t i = 0; i < ten_unit_arr.count() && OB_SUCC(ret); ++i) {
    for (int64_t j = 0; j < unit_ptr_arr.count() && OB_SUCC(ret); ++j) {
      if (unit_ptr_arr.at(j)->unit_.unit_id_ == ten_unit_arr.at(i)->unit_id_) {
        if (OB_FAIL(one_ten_unit_arr.push_back(ten_unit_arr.at(i)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  // Sort from small to large, always put replicas in the unit with the smallest number of replicas
  TenParCmp cmp(single_replica.replica_type_, single_replica.memstore_percent_);
  // Sort by total number of replicas
  std::sort(one_ten_unit_arr.begin(), one_ten_unit_arr.end(), cmp);
  if (OB_SUCC(ret)) {
    // Extract all pairs with the smallest number of all_replica_cnt
    if (OB_FAIL(get_final_replica_addr_array(single_replica, one_ten_unit_arr, leader_rep_unit_arr))) {
      LOG_WARN("fail to get final replica addr array", KR(ret));
    } else {
      int64_t zone_id = 0;
      const int64_t zone_list_count = zone_list.count();
      if (zone_list_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone list is null", KR(ret), K(single_replica), K(zone), K(pkey), K(zone_list));
      } else if (OB_FAIL(get_random_zone_num(zone_list_count, tenant_id, zone_id1, zone_id))) {
        LOG_WARN("fail to get random zone num", KR(ret));
      } else if (zone_list.at(zone_id) == zone || (single_replica.replica_type_ != REPLICA_TYPE_FULL &&
                                                      !is_equal_zero_zone(zone_list_count, tenant_id, zone_id))) {
        // The replica of L that is the same as the zone
        // where the first table is located is first placed on the unit with more leaders
        PairCmpRepLeaderAsc cmp;
        // Sort by leader from small to large,
        // the leader is placed on the unit with less leader
        std::sort(leader_rep_unit_arr.begin(), leader_rep_unit_arr.end(), cmp);
      } else {
        PairCmpRepLeaderDes cmp;
        // Sort by leader from large to small,
        // not the leader, put it on the unit with more leaders
        std::sort(leader_rep_unit_arr.begin(), leader_rep_unit_arr.end(), cmp);
      }
    }
  }
  return ret;
}

bool ObCreateTableReplicaByLocality::is_equal_zero_zone(
    const int64_t zone_list_count, const uint64_t tenant_id, const int64_t zone_id)
{
  bool ret = true;
  int64_t tmp_zone_id = 0;
  if (OB_FAIL(get_random_zone_num(zone_list_count, tenant_id, 0 /*first table No.*/, tmp_zone_id))) {
    LOG_WARN("fail to get random zone num", KR(ret));
  } else if (zone_id == tmp_zone_id) {
    ret = true;
  } else {
    ret = false;
  }
  return ret;
}

int ObCreateTableReplicaByLocality::get_final_replica_addr_array(const SingleReplica& single_replica,
    common::ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
    common::ObArray<share::TenantUnitRepCnt*>& leader_rep_unit_arr)
{
  int ret = OB_SUCCESS;
  if (ten_unit_arr.count() != 0) {
    // Extract all pairs with the smallest number of all_replica_cnt
    if (OB_FAIL(leader_rep_unit_arr.push_back(ten_unit_arr.at(0)))) {
      LOG_WARN("fail to push back", KR(ret));
    } else {
      for (int64_t i = 1; i < ten_unit_arr.count() && OB_SUCC(ret); ++i) {
        if (single_replica.replica_type_ == REPLICA_TYPE_FULL && single_replica.memstore_percent_ != 0) {
          if (ten_unit_arr.at(i)->unit_rep_cnt_.get_full_replica_cnt() ==
              ten_unit_arr.at(i - 1)->unit_rep_cnt_.get_full_replica_cnt()) {
            if (OB_FAIL(leader_rep_unit_arr.push_back(ten_unit_arr.at(i)))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          } else {
            break;  // Not the same
          }
        } else if (single_replica.replica_type_ == REPLICA_TYPE_FULL && single_replica.memstore_percent_ == 0) {
          if (ten_unit_arr.at(i)->unit_rep_cnt_.get_d_replica_cnt() ==
              ten_unit_arr.at(i - 1)->unit_rep_cnt_.get_d_replica_cnt()) {
            if (OB_FAIL(leader_rep_unit_arr.push_back(ten_unit_arr.at(i)))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          } else {
            break;
          }
        } else if (single_replica.replica_type_ == REPLICA_TYPE_LOGONLY) {
          if (ten_unit_arr.at(i)->unit_rep_cnt_.get_logonly_replica_cnt() ==
              ten_unit_arr.at(i - 1)->unit_rep_cnt_.get_logonly_replica_cnt()) {
            if (OB_FAIL(leader_rep_unit_arr.push_back(ten_unit_arr.at(i)))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          } else {
            break;
          }
        } else if (single_replica.replica_type_ == REPLICA_TYPE_READONLY) {
          if (ten_unit_arr.at(i)->unit_rep_cnt_.get_readonly_replica_cnt() ==
              ten_unit_arr.at(i - 1)->unit_rep_cnt_.get_readonly_replica_cnt()) {
            if (OB_FAIL(leader_rep_unit_arr.push_back(ten_unit_arr.at(i)))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_non_part_multiple_zone_paxos_replica(const SingleReplica& mix_single_replica,
    const common::ObPartitionKey& pkey, const common::ObIArray<common::ObZone>& zone_array, const int64_t x_index,
    ObIArray<TenantUnitRepCnt*>& ten_unit_arr, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey.get_tenant_id();
  // 1.mix and normal mixing.
  // zones in mix can't be leader, Adopt the original distribution zone method
  if (zone_array.count() > 1 && shadow_zone_tasks_.count() > 1) {
    common::ObZone zone;
    common::ObArray<common::ObZone> alive_zone_list;
    ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM> unit_array;
    if (OB_FAIL(zone_units_provider_.get_all_ptr_zone_units(unit_array))) {
      LOG_WARN("fail to get all zone units", KR(ret));
    } else if (OB_FAIL(get_alive_zone_list(unit_array, zone_array, alive_zone_list))) {
      LOG_WARN("fail to get alive zone list", KR(ret), K(mix_single_replica), K(pkey), K(zone_array), K(x_index));
    } else if (alive_zone_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone list is null", KR(ret), K(mix_single_replica), K(pkey), K(zone_array), K(x_index));
    } else if (FALSE_IT(std::sort(alive_zone_list.begin(), alive_zone_list.end()))) {
      // never be here
    } else if (FALSE_IT(zone = alive_zone_list.at(x_index % alive_zone_list.count()))) {
      // never be here
    } else if (OB_FAIL(
                   do_alloc_non_part_replica(mix_single_replica, pkey, unit_array, zone, ten_unit_arr, replica_addr))) {
      LOG_WARN("fail to do alloc non part replica", KR(ret));
    }
  } else {
    // 2.pure mix: shadow_zone_tasks_.count()==1
    // every zones can be leader
    if (OB_SUCC(ret)) {
      int64_t tmp_zone_id = 0;
      // If it is a table, put it at the position of 0,
      // if it is a tablegroup, put it at the end
      if (!is_new_tablegroup_id(pkey.get_tablegroup_id())) {
        tmp_zone_id = ten_unit_arr.at(0)->unit_rep_cnt_.index_num_;
      } else {
        tmp_zone_id = ten_unit_arr.at(0)->non_table_cnt_;
      }
      ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM> unit_array;
      common::ObArray<common::ObZone> alive_zone_list;
      if (OB_FAIL(zone_units_provider_.get_all_ptr_zone_units(unit_array))) {
        LOG_WARN("fail to get all zone units", KR(ret));
      } else if (OB_FAIL(get_alive_zone_list(unit_array, zone_array, alive_zone_list))) {
        LOG_WARN("fail to get alive zone list",
            KR(ret),
            K(mix_single_replica),
            K(pkey),
            K(zone_array),
            K(x_index),
            K(alive_zone_list));
      } else {
        common::ObArray<SingleReplica> single_replica_list;
        std::sort(alive_zone_list.begin(), alive_zone_list.end());
        int64_t tmp = 0;
        int64_t leader_id = 0;
        const int64_t zone_list_count = alive_zone_list.count();
        for (; tmp < alive_zone_list.count() && OB_SUCC(ret); ++tmp) {
          // Find the first replica that can become the leader
          if (curr_part_task_array_.at(tmp).replica_type_ == REPLICA_TYPE_FULL &&
              curr_part_task_array_.at(tmp).memstore_percent_ != 0) {
            if (OB_FAIL(single_replica_list.push_back(curr_part_task_array_.at(tmp)))) {
              LOG_WARN("fail to push back", KR(ret));
            } else {
              break;
            }
          }
        }
        for (int64_t i = 0; i < alive_zone_list.count() && OB_SUCC(ret); ++i) {
          // Put the rest in order
          if (i != tmp) {
            if (OB_FAIL(single_replica_list.push_back(curr_part_task_array_.at(i)))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (zone_list_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "zone list is null", KR(ret), K(mix_single_replica), K(zone_array), K(alive_zone_list), K(x_index));
          } else if (OB_FAIL(get_random_zone_num(zone_list_count, tenant_id, tmp_zone_id, leader_id))) {
            LOG_WARN("fail to get random zone num", KR(ret));
          } else {
            const int64_t rep_id = (curr_part_task_idx_ + leader_id) % alive_zone_list.count();
            // The zone where the replica to be allocated is located
            const common::ObZone& zone = alive_zone_list.at(rep_id);
            SingleReplica single_replica = single_replica_list.at(curr_part_task_idx_);
            if (OB_FAIL(
                    do_alloc_non_part_replica(single_replica, pkey, unit_array, zone, ten_unit_arr, replica_addr))) {
              LOG_WARN("fail to do alloc non part replica", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::get_alive_zone_list(const ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM>& unit_array,
    const common::ObIArray<common::ObZone>& zone_array, common::ObIArray<common::ObZone>& alive_zone_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < unit_array.count() && OB_SUCC(ret); ++i) {
    const ObArray<share::ObUnitInfo*>& this_unit_infos = unit_array.at(i);
    for (int64_t j = 0; j < this_unit_infos.count() && OB_SUCC(ret); ++j) {
      share::ObUnitInfo* this_unit_info = this_unit_infos.at(j);
      if (OB_ISNULL(this_unit_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this unit info is null", KR(ret), K(zone_array));
      } else if (has_exist_in_array(alive_zone_list, this_unit_info->unit_.zone_)) {
        // nothing todo
      } else if (!has_exist_in_array(zone_array, this_unit_info->unit_.zone_)) {
        // nothing todo
      } else if (OB_FAIL(alive_zone_list.push_back(this_unit_info->unit_.zone_))) {
        LOG_WARN("fail to push back zone", KR(ret), "zone", this_unit_info->unit_.zone_);
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_single_zone_task(const common::ObPartitionKey& pkey,
    const SingleReplica& single_replica, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, const bool non_partition_table,
    ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  int64_t x_index = -1;
  int64_t y_index = -1;
  replica_addr.reset();
  if (OB_UNLIKELY(1 != curr_part_zone_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("curr part zone array count unexpected", KR(ret));
  } else if (non_partition_table && GCTX.is_primary_cluster() && ten_unit_arr.count() > 0) {
    const uint64_t tenant_id = pkey.get_tenant_id();
    if (OB_FAIL(alloc_non_part_single_zone_replica(single_replica, pkey, ten_unit_arr, replica_addr))) {
      LOG_WARN("fail to alloc replica", KR(ret));
    }
  } else {
    const common::ObZone& zone = curr_part_zone_array_.at(0);
    // non paxos
    if (!ObReplicaTypeCheck::is_paxos_replica_V2(single_replica.replica_type_)) {
      if (OB_FAIL(get_xy_index(false /*is multiple zone*/, pkey, curr_part_zone_array_, x_index, y_index))) {
        LOG_WARN("fail to get xy index", KR(ret), K(pkey), "zone_count", curr_part_zone_array_.count());
      } else if (OB_FAIL(alloc_single_zone_nonpaxos_replica(single_replica, zone, y_index, replica_addr))) {
        LOG_WARN("fail to alloc replica", KR(ret));
      }
    }
    // paxos
    else if (nullptr == high_priority_zone_array_ || !has_exist_in_array(*high_priority_zone_array_, zone)) {
      if (OB_FAIL(get_xy_index(false /*is multiple zone*/, pkey, curr_part_zone_array_, x_index, y_index))) {
        LOG_WARN("fail to get xy index", KR(ret), K(pkey), "zone_count", curr_part_zone_array_.count());
      } else if (OB_FAIL(alloc_single_zone_paxos_replica(single_replica, zone, y_index, replica_addr))) {
        LOG_WARN("fail to alloc replica", KR(ret));
      }
    } else {
      if (OB_FAIL(get_xy_index(false /*is multiple zone*/, pkey, *high_priority_zone_array_, x_index, y_index))) {
        LOG_WARN("fail to get xy index", KR(ret), K(pkey), "zone_count", high_priority_zone_array_->count());
      } else if (OB_FAIL(alloc_single_zone_paxos_replica(single_replica, zone, y_index, replica_addr))) {
        LOG_WARN("fail to alloc replica", KR(ret));
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::alloc_multiple_zone_task(const common::ObPartitionKey& pkey,
    const SingleReplica& single_replica, ObIArray<TenantUnitRepCnt*>& ten_unit_arr, const bool non_partition_table,
    ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  int64_t x_index = -1;
  int64_t y_index = -1;
  replica_addr.reset();
  if (OB_UNLIKELY(curr_part_zone_array_.count() <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("curr part zone array count unexpected", KR(ret));
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(single_replica.replica_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected replica type", KR(ret), "replica_type", single_replica.replica_type_);
  } else if (OB_FAIL(get_xy_index(true /*is multiple zone*/, pkey, curr_part_zone_array_, x_index, y_index))) {
    LOG_WARN("fail to get xy index", KR(ret), K(pkey));
  } else {
    if (non_partition_table && GCTX.is_primary_cluster() && ten_unit_arr.count() > 0) {
      if (OB_FAIL(alloc_non_part_multiple_zone_paxos_replica(
              single_replica, pkey, curr_part_zone_array_, x_index, ten_unit_arr, replica_addr))) {
        LOG_WARN("fail to alloc replica", KR(ret));
      }
    } else {
      if (OB_FAIL(alloc_multiple_zone_paxos_replica(
              single_replica, curr_part_zone_array_, x_index, y_index, replica_addr))) {
        LOG_WARN("fail to alloc replica", KR(ret), K(single_replica));
      }
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::prepare_for_next_task(const ZoneTask& zone_task)
{
  int ret = OB_SUCCESS;
  curr_part_task_idx_ = 0;
  curr_part_zone_array_.reset();
  curr_part_task_array_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_task.inner_task_.get_zone_set().count(); ++i) {
    const common::ObZone& this_zone = zone_task.inner_task_.get_zone_set().at(i);
    if (OB_FAIL(curr_part_zone_array_.push_back(this_zone))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(curr_part_zone_array_.begin(), curr_part_zone_array_.end());
  }
  if (OB_SUCC(ret)) {
    const ObReplicaAttrSet& replica_attr_set = zone_task.inner_task_.get_replica_task_set();
    const ObIArray<ReplicaAttr>& full_set = replica_attr_set.get_full_replica_attr_array();
    const ObIArray<ReplicaAttr>& logonly_set = replica_attr_set.get_logonly_replica_attr_array();
    const ObIArray<ReplicaAttr>& readonly_set = replica_attr_set.get_readonly_replica_attr_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < full_set.count(); ++i) {
      const share::ReplicaAttr& replica_attr = full_set.at(i);
      const int64_t replica_num = replica_attr.num_;
      for (int64_t j = 0; OB_SUCC(ret) && j < replica_num; ++j) {
        if (OB_FAIL(
                curr_part_task_array_.push_back(SingleReplica(REPLICA_TYPE_FULL, replica_attr.memstore_percent_)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < logonly_set.count(); ++i) {
      const share::ReplicaAttr& replica_attr = logonly_set.at(i);
      const int64_t replica_num = replica_attr.num_;
      for (int64_t j = 0; OB_SUCC(ret) && j < replica_num; ++j) {
        if (OB_FAIL(
                curr_part_task_array_.push_back(SingleReplica(REPLICA_TYPE_LOGONLY, replica_attr.memstore_percent_)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < readonly_set.count(); ++i) {
      const share::ReplicaAttr& replica_attr = readonly_set.at(i);
      const int64_t replica_num = replica_attr.num_;
      for (int64_t j = 0; ObLocalityDistribution::ALL_SERVER_CNT != replica_num && OB_SUCC(ret) && j < replica_num;
           ++j) {
        if (OB_FAIL(curr_part_task_array_.push_back(
                SingleReplica(REPLICA_TYPE_READONLY, replica_attr.memstore_percent_)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    SingleReplicaSort op;
    std::sort(curr_part_task_array_.begin(), curr_part_task_array_.end(), op);
    if (OB_FAIL(op.get_ret())) {
      LOG_WARN("fail to sort curr part task array", KR(ret));
    }
  }
  return ret;
}

int ObCreateTableReplicaByLocality::fill_all_rest_server_with_replicas(ObPartitionAddr& paddr)
{
  int ret = OB_SUCCESS;
  ObReplicaAddr replica_addr;

  // R{all_server}@zone
  FOREACH_CNT_X(z, zloc_, OB_SUCCESS == ret)
  {
    const ObReplicaAttrSet& set = z->replica_attr_set_;
    const ObZone& zone = z->zone_;
    int64_t memstore_percent = -1;
    if (ObLocalityDistribution::ALL_SERVER_CNT != set.get_readonly_replica_num()) {
      // bypass, not all server readonly
    } else if (OB_FAIL(set.get_readonly_memstore_percent(memstore_percent))) {
      LOG_WARN("fail to get readonly memstore percent", KR(ret));
    } else {
      // Iterate until it can no longer be allocated the replica
      while (OB_SUCC(ret)) {
        if (OB_FAIL(alloc_replica_in_zone(0, zone, common::REPLICA_TYPE_READONLY, memstore_percent, replica_addr))) {
          // fail, print log afterwards
        } else if (OB_FAIL(paddr.push_back(replica_addr))) {
          LOG_WARN("fail push replica", K(replica_addr), KR(ret));
        }
      }
      // If you want to assign other types, you can check here
      if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail alloc replica in zone", K(zone), KR(ret));
      }
    }
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////
ObAddSpecificReplicaByLocality::ObAddSpecificReplicaByLocality(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
    ObZoneUnitsProvider& zone_units_provider, const share::schema::ZoneLocalityIArray& zloc,
    const common::ObIArray<common::ObZone>& zone_list)
    : ObAllocReplicaByLocality(zone_mgr, zone_units_provider, zloc, obrpc::OB_CREATE_TABLE_MODE_STRICT, zone_list),
      ts_(ts),
      logonly_zones_(),
      shadow_zone_tasks_local_(),
      logonly_zone_task_()
{}

int ObAddSpecificReplicaByLocality::init()
{
  int ret = OB_SUCCESS;
  logonly_zones_.reset();
  shadow_zone_tasks_local_.reset();
  logonly_zone_task_.reset();
  bool with_paxos = true;
  bool with_readonly = false;
  ObSEArray<ZoneUnit, common::MAX_ZONE_NUM> all_zone_unit;
  if (OB_FAIL(ObAllocReplicaByLocality::init(with_paxos, with_readonly))) {
    LOG_WARN("fail to init", KR(ret));
  } else if (OB_FAIL(zone_units_provider_.get_all_zone_units(all_zone_unit))) {
    LOG_WARN("fail to get all zone unit", KR(ret));
  } else {
    for (int64_t i = 0; i < all_zone_unit.count() && OB_SUCC(ret); i++) {
      const ZoneUnit& zu = all_zone_unit.at(i);
      for (int64_t j = 0; j < zu.all_unit_.count() && OB_SUCC(ret); j++) {
        UnitStat* us = zu.all_unit_.at(j);
        if (OB_ISNULL(us)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid us", KR(ret), K(us));
        } else if (REPLICA_TYPE_LOGONLY == us->info_.unit_.replica_type_) {
          if (OB_FAIL(logonly_zones_.push_back(us->info_.unit_.zone_))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; i < zone_tasks_.count() && OB_SUCC(ret); i++) {
      if (REPLICA_TYPE_LOGONLY == zone_tasks_.at(i).replica_type_ &&
          has_exist_in_array(logonly_zones_, zone_tasks_.at(i).name_)) {
        if (OB_FAIL(shadow_zone_tasks_local_.push_back(zone_tasks_.at(i)))) {
          // Only save tasks that should be stored in Lunit
          LOG_WARN("fail to push back", KR(ret), K(i), K(zone_tasks_));
        }
      }
    }
  }
  return ret;
}

int ObAddSpecificReplicaByLocality::prepare_for_next_partition(Partition& partition, const bool need_update_pg_cnt)
{
  int ret = OB_SUCCESS;
  need_update_pg_cnt_ = need_update_pg_cnt;
  logonly_zone_task_.reset();
  if (OB_FAIL(logonly_zone_task_.assign(shadow_zone_tasks_local_))) {
    LOG_WARN("fail to assign", KR(ret), K(shadow_zone_tasks_local_));
  }
  FOR_BEGIN_END_E(replica_addr, partition, ts_.all_replica_, OB_SUCC(ret))
  {
    if (!replica_addr->is_in_service()) {
      // nothing todo
    } else if (OB_FAIL(unit_set_.set_refactored(replica_addr->unit_->info_.unit_.unit_id_))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("two replica in same server, unexpected", K(*replica_addr), KR(ret));
      } else {
        LOG_WARN("fail set server to server set", K(*replica_addr), KR(ret));
      }
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_addr->replica_type_) && !replica_addr->is_in_service()) {
      // nothing todo
    } else if (REPLICA_TYPE_LOGONLY != replica_addr->replica_type_) {
      // nothing todo
    } else if (!has_exist_in_array(logonly_zones_, replica_addr->zone_)) {
      // nothing todo
    } else {
      int64_t remove_idx = -1;
      if (remove_idx < 0 && logonly_zone_task_.count() > 0 && OB_SUCC(ret)) {
        ARRAY_FOREACH_X(logonly_zone_task_, idx, cnt, remove_idx < 0)
        {
          ZoneTask& t = logonly_zone_task_.at(idx);
          if (t.replica_type_ == replica_addr->replica_type_ && t.name_ == replica_addr->zone_) {
            remove_idx = idx;
          }
        }
        // TODO: Performance optimization
        // Record all idx to be deleted,
        // and delete from back to front in a unified manner,
        // which can reduce copying
        //
        // or logonly_zone_task_ uses linked list. DLink
        if (remove_idx >= 0) {
          ret = logonly_zone_task_.remove(remove_idx);
        }
      }
    }
  }
  return ret;
}

int ObAddSpecificReplicaByLocality::get_next_replica(
    const common::ObArray<Partition*>::iterator& pp, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  // TODO: The region branch can be split into zone tasks in advance to improve performance
  if (zone_task_idx_ < logonly_zone_task_.count()) {
    bool need_random_add_replica = true;
    int64_t next = zone_task_idx_++ % logonly_zone_task_.count();  // next task
    int64_t guard = next;
    do {
      if (OB_FAIL(align_add_replica(pp, replica_addr, next, need_random_add_replica))) {
        LOG_WARN("fail to align add replica", KR(ret));
        if (GCTX.is_standby_cluster() && zone_list_.count() == 1) {
          break;
          // standby and single zone must be aligned
        }
      }
      // Ignore this error code.
      // if the result fails using the random strategy to add replica
      // if the result success set need_random_add_replica to false
      if (OB_FAIL(ret) || need_random_add_replica) {
        ZoneTask& task = logonly_zone_task_.at(next);
        ObZone& zone = task.name_;
        if (OB_FAIL(alloc_replica_in_zone(
                ts_.get_seq_generator().next_seq(), zone, task.replica_type_, task.memstore_percent_, replica_addr))) {
          LOG_WARN("fail allocate replica in zone", K(zone), K(task), KR(ret));
          next++;
          // There is no server available in the current zone, find the next task
          next %= logonly_zone_task_.count();
        } else {
          LOG_INFO("alloc replica in zone finish", K(zone), K(task), K(zone_task_idx_), K(logonly_zone_task_));
        }
      } else {
      }  // nothing todo
    } while (OB_FAIL(ret) && next != guard);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAddSpecificReplicaByLocality::align_add_replica(const common::ObArray<Partition*>::iterator& p,
    ObReplicaAddr& replica_addr, const int64_t& next, bool& need_random_add_replica)
{
  int ret = OB_SUCCESS;
  ObArray<Partition*>::iterator pp = p;
  do {
    __typeof(pp) before_partition = pp - 1;
    if ((before_partition) < ts_.sorted_partition_.begin() || !is_same_tg(*(*pp), *(*before_partition)) ||
        !(*pp)->can_rereplicate() || (*before_partition)->partition_id_ != (*pp)->partition_id_) {
      break;
    }
    pp = before_partition;
  } while (OB_SUCC(ret));
  do {
    ZoneTask& task = logonly_zone_task_.at(next);
    ObZone& zone = task.name_;
    // In which zone does the partition lack a
    FOR_BEGIN_END(r, *(*pp), ts_.all_replica_)
    {
      if (r->zone_ == zone && ObReplicaTypeCheck::is_log_replica((r)->replica_type_) && (r)->is_in_service() &&
          (r)->server_->can_migrate_in()) {
        need_random_add_replica = false;
        // find the matching replica, no need to add replica
        replica_addr.reset();
        replica_addr.unit_id_ = r->unit_->info_.unit_.unit_id_;
        replica_addr.addr_ = (r)->server_->server_;
        replica_addr.zone_ = (r)->zone_;
        replica_addr.replica_type_ = (r)->replica_type_;
        if (OB_FAIL(replica_addr.set_memstore_percent((r)->memstore_percent_))) {
          LOG_WARN("fai lto set memstore percent", KR(ret));
        } else if (OB_FAIL(unit_set_.set_refactored(replica_addr.unit_id_))) {
          LOG_WARN("fail set server to server set", K(replica_addr), KR(ret), K(zone));
        } else {
          LOG_INFO("find replica", K(replica_addr), K(*r));
        }
        break;  // Jump out when found
      } else {
      }  // nothing todo
    }    // end for replica
    if (OB_SUCC(ret)) {
      if (!need_random_add_replica) {
        break;  // Jump out when found
      } else {
        __typeof__(pp) next_partition = pp + 1;
        if ((next_partition) >= ts_.sorted_partition_.end() ||
            (*next_partition)->partition_id_ != (*pp)->partition_id_ || (*next_partition)->is_primary() ||
            !is_same_tg(*(*pp), *(*next_partition)) || !(*pp)->can_rereplicate()) {
          break;
        }
        pp = next_partition;
      }
    }
  } while (OB_SUCC(ret));
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////

ObAddPaxosReplicaByLocality::ObAddPaxosReplicaByLocality(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
    ObZoneUnitsProvider& zone_units_provider, ObZoneUnitsProvider& logonly_zone_units_provider,
    const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list)
    : ObAllocReplicaByLocality(zone_mgr, zone_units_provider, zloc, obrpc::OB_CREATE_TABLE_MODE_STRICT, zone_list),
      ts_(ts),
      logonly_zone_unit_provider_(logonly_zone_units_provider)
{}

int ObAddPaxosReplicaByLocality::init()
{
  bool with_paxos = true;
  bool with_readonly = false;
  return ObAllocReplicaByLocality::init(with_paxos, with_readonly);
}

int ObAddPaxosReplicaByLocality::prepare_for_next_partition(Partition& partition, const bool need_update_pg_cnt)
{
  int ret = OB_SUCCESS;
  need_update_pg_cnt_ = need_update_pg_cnt;
  zone_task_idx_ = 0;
  unit_set_.reuse();
  shadow_zone_tasks_.reuse();

  if (OB_FAIL(shadow_zone_tasks_.assign(zone_tasks_))) {
    LOG_WARN("fail copy zone tasks", K_(zone_tasks), KR(ret));
  }
  FOR_BEGIN_END_E(replica_addr, partition, ts_.all_replica_, OB_SUCC(ret))
  {
    if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_addr->replica_type_) && !replica_addr->is_in_service()) {
      // nothing todo
    } else if (OB_FAIL(unit_set_.set_refactored(replica_addr->unit_->info_.unit_.unit_id_))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("two replica in same server, unexpected", K(*replica_addr), KR(ret));
      } else {
        LOG_WARN("fail set server to server set", K(*replica_addr), KR(ret));
      }
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_addr->replica_type_) &&
               !logonly_zone_unit_provider_.exist(
                   replica_addr->unit_->info_.unit_.zone_, replica_addr->unit_->info_.unit_.unit_id_)) {
      // only_in_member_list will process in build_only_in_memberlist_replic
      // delete the replica which is not yet recycled from the task list,
      // the rest is the recovered replica
      // Delete from zone task first, then delete from region
      int64_t remove_idx = -1;
      if (remove_idx < 0 && shadow_zone_tasks_.count() > 0 && OB_SUCC(ret)) {
        ARRAY_FOREACH_X(shadow_zone_tasks_, idx, cnt, remove_idx < 0)
        {
          // Priority treatment of the same type paxos replicas
          ZoneTask& t = shadow_zone_tasks_.at(idx);
          if (t.replica_type_ == replica_addr->replica_type_ && t.name_ == replica_addr->zone_) {
            remove_idx = idx;
          }
        }
        ARRAY_FOREACH_X(shadow_zone_tasks_, idx, cnt, remove_idx < 0)
        {
          ZoneTask& t = shadow_zone_tasks_.at(idx);
          if (ObReplicaTypeCheck::is_paxos_replica_V2(t.replica_type_) && t.name_ == replica_addr->zone_) {
            remove_idx = idx;
          }
        }
        if (remove_idx >= 0) {
          ret = shadow_zone_tasks_.remove(remove_idx);
        }
      }
    }
  }

  return ret;
}

int ObAddPaxosReplicaByLocality::get_next_replica(
    const common::ObArray<Partition*>::iterator& pp, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  if (zone_task_idx_ < shadow_zone_tasks_.count()) {
    // 1. First traverse other partitions of the same p_g,
    // If there is no shortage, use the replica of the partition
    bool need_random_add_replica = true;
    int64_t next = zone_task_idx_++ % shadow_zone_tasks_.count();
    int64_t guard = next;
    do {
      // For each task, do p_g alignment and rereplica first, and then rereplica randomly
      if (OB_FAIL(align_add_replica(pp, replica_addr, next, need_random_add_replica))) {
        LOG_WARN("fail to align add replica", KR(ret));
        if (GCTX.is_standby_cluster() && zone_list_.count() == 1) {
          break;
          // Standby database and single zone, must be aligned
        }
      }
      // Ignore the error code,
      // if fail, making rereplica randomly
      // if succ, seting need_random_add_replica to false
      if (OB_FAIL(ret) || need_random_add_replica) {
        ZoneTask& task = shadow_zone_tasks_.at(next);
        ObZone& zone = task.name_;
        if (OB_FAIL(alloc_replica_in_zone(
                ts_.get_seq_generator().next_seq(), zone, task.replica_type_, task.memstore_percent_, replica_addr))) {
          LOG_WARN("fail allocate replica in zone", K(zone), K(task), KR(ret));
          next++;
          next %= shadow_zone_tasks_.count();
        } else {
          LOG_INFO("alloc replica in zone finish", K(zone), K(task), K(zone_task_idx_), K(shadow_zone_tasks_));
        }
      } else {
      }  // nothing todo
    } while (OB_FAIL(ret) && next != guard);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAddPaxosReplicaByLocality::align_add_replica(const common::ObArray<Partition*>::iterator& p,
    ObReplicaAddr& replica_addr, const int64_t& next, bool& need_random_add_replica)
{
  int ret = OB_SUCCESS;
  ObArray<Partition*>::iterator pp = p;
  // Find the first partition of p_g where the partition is located
  do {
    __typeof(pp) before_partition = pp - 1;
    if ((before_partition) < ts_.sorted_partition_.begin() || !is_same_tg(*(*pp), *(*before_partition)) ||
        !(*pp)->can_rereplicate() || (*before_partition)->partition_id_ != (*pp)->partition_id_) {
      break;
    }
    pp = before_partition;
  } while (OB_SUCC(ret));
  do {
    // Starting from the first partition, determine whether there is a replica
    ZoneTask& task = shadow_zone_tasks_.at(next);
    ObZone& zone = task.name_;
    FOR_BEGIN_END(r, *(*pp), ts_.all_replica_)
    {
      if (r->zone_ == zone && ObReplicaTypeCheck::is_can_elected_replica((r)->replica_type_) && (r)->is_in_service() &&
          (r)->server_->can_migrate_in()) {
        need_random_add_replica = false;
        // If a matching replica is found, there is no need to make a replica
        replica_addr.reset();
        replica_addr.unit_id_ = r->unit_->info_.unit_.unit_id_;
        replica_addr.addr_ = (r)->server_->server_;
        replica_addr.zone_ = (r)->zone_;
        replica_addr.replica_type_ = (r)->replica_type_;
        if (OB_FAIL(replica_addr.set_memstore_percent((r)->memstore_percent_))) {
          LOG_WARN("fai lto set memstore percent", KR(ret));
        } else if (OB_FAIL(unit_set_.set_refactored(replica_addr.unit_id_))) {
          LOG_WARN("fail set server to server set", K(replica_addr), KR(ret), K(zone));
        } else {
          LOG_INFO("find replica", K(replica_addr), K(*r));
        }
        break;
      } else {
      }  // nothing todo
    }    // end for replica
    if (OB_SUCC(ret)) {
      if (!need_random_add_replica) {
        break;
      } else {
        __typeof__(pp) next_partition = pp + 1;
        if ((next_partition) >= ts_.sorted_partition_.end() ||
            (*next_partition)->partition_id_ != (*pp)->partition_id_ || (*next_partition)->is_primary() ||
            !is_same_tg(*(*pp), *(*next_partition)) || !(*pp)->can_rereplicate()) {
          break;
        }
        pp = next_partition;
      }
    }
  } while (OB_SUCC(ret));
  return ret;
}
//////////////////////////////////////

ObAddReadonlyReplicaByLocality::ObAddReadonlyReplicaByLocality(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
    ObZoneUnitsProvider& zone_units_provider, const share::schema::ZoneLocalityIArray& zloc,
    const common::ObIArray<common::ObZone>& zone_list)
    : ObAllocReplicaByLocality(zone_mgr, zone_units_provider, zloc, obrpc::OB_CREATE_TABLE_MODE_STRICT, zone_list),
      ts_(ts)
{}

int ObAddReadonlyReplicaByLocality::init()
{
  bool with_paxos = false;
  bool with_readonly = true;
  return ObAllocReplicaByLocality::init(with_paxos, with_readonly);
}

int ObAddReadonlyReplicaByLocality::prepare_for_next_partition(Partition& partition, const bool need_update_pg_cnt)
{
  int ret = OB_SUCCESS;
  need_update_pg_cnt_ = need_update_pg_cnt;
  zone_task_idx_ = 0;
  unit_set_.reuse();
  shadow_zone_tasks_.reuse();

  if (OB_FAIL(shadow_zone_tasks_.assign(zone_tasks_))) {
    LOG_WARN("fail copy zone tasks", K_(zone_tasks), KR(ret));
  }

  FOR_BEGIN_END_E(replica_addr, partition, ts_.all_replica_, OB_SUCC(ret))
  {
    if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_addr->replica_type_) && !replica_addr->is_in_service()) {
      // nothing todo
    } else if (OB_FAIL(unit_set_.set_refactored(replica_addr->unit_->info_.unit_.unit_id_))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("two replica in same server, unexpected", K(*replica_addr), KR(ret));
      } else {
        LOG_WARN("fail set server to server set", K(*replica_addr), KR(ret));
      }
    } else if (ObReplicaTypeCheck::is_readonly_replica(replica_addr->replica_type_) && replica_addr->is_in_service()) {
      int64_t remove_idx = -1;
      if (remove_idx < 0 && shadow_zone_tasks_.count() > 0 && OB_SUCC(ret)) {
        ARRAY_FOREACH_X(shadow_zone_tasks_, idx, cnt, remove_idx < 0)
        {
          ZoneTask& t = shadow_zone_tasks_.at(idx);
          if (t.replica_type_ == replica_addr->replica_type_ && t.name_ == replica_addr->zone_) {
            remove_idx = idx;
          }
        }
        if (remove_idx >= 0) {
          ret = shadow_zone_tasks_.remove(remove_idx);
        }
      }
    }
  }

  return ret;
}

int ObAddReadonlyReplicaByLocality::get_next_replica(
    const common::ObArray<Partition*>::iterator& pp, ObReplicaAddr& replica_addr)
{
  int ret = OB_SUCCESS;
  if (zone_task_idx_ < shadow_zone_tasks_.count()) {
    int64_t next = zone_task_idx_++ % shadow_zone_tasks_.count();
    int64_t guard = next;
    bool need_random_add_replica = true;
    do {
      if (OB_FAIL(align_add_replica(pp, replica_addr, next, need_random_add_replica))) {
        LOG_WARN("fail to align add replica", KR(ret));
        if (GCTX.is_standby_cluster() && zone_list_.count() == 1) {
          break;
        }
      }
      if (OB_FAIL(ret) || need_random_add_replica) {
        ZoneTask& task = shadow_zone_tasks_.at(next);
        ObZone& zone = task.name_;
        if (OB_FAIL(alloc_replica_in_zone(
                ts_.get_seq_generator().next_seq(), zone, task.replica_type_, task.memstore_percent_, replica_addr))) {
          LOG_WARN("fail allocate replica in zone", K(zone), K(task), KR(ret));
          next++;
          next %= shadow_zone_tasks_.count();
        } else {
          LOG_INFO("alloc replica in zone finish", K(zone), K(task), K(zone_task_idx_), K(shadow_zone_tasks_));
        }
      } else {
      }  // nothing todo
    } while (OB_FAIL(ret) && next != guard);
  } else if (all_server_zones_.count() > 0) {
    //
    // ALL_SERVER mode
    // If a zone has no resources, try the next one
    bool need_random_add_replica = true;
    int64_t next = saved_zone_pos_++ % all_server_zones_.count();
    int64_t guard = next;
    do {
      if (OB_FAIL(ret) || need_random_add_replica) {
        if (OB_FAIL(alloc_replica_in_zone(ts_.get_seq_generator().next_seq(),
                all_server_zones_.at(next),
                REPLICA_TYPE_READONLY,
                100,
                // FIXME: Fill in the correct memstore percent in the subsequent supplementary copy logic
                replica_addr))) {
          if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
            next++;
            next %= all_server_zones_.count();
          } else {
            LOG_WARN("fail allocate replica in zone", "zone", all_server_zones_.at(next), KR(ret));
            break;
          }
        }
      } else {
      }  // nothing todo
    } while (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret && next != guard);
    if (next == guard && OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
      // all R{all_server}@zone/region have been traversed
      // There are no machines that need to supplement replicas, terminate
      ret = OB_ITER_END;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAddReadonlyReplicaByLocality::align_add_replica(const common::ObArray<Partition*>::iterator& p,
    ObReplicaAddr& replica_addr, const int64_t& next, bool& need_random_add_replica)
{
  int ret = OB_SUCCESS;
  ObArray<Partition*>::iterator pp = p;
  do {
    __typeof(pp) before_partition = pp - 1;
    if ((before_partition) < ts_.sorted_partition_.begin() || !is_same_tg(*(*pp), *(*before_partition)) ||
        !(*pp)->can_rereplicate() || (*before_partition)->partition_id_ != (*pp)->partition_id_) {
      break;
    }
    pp = before_partition;
  } while (OB_SUCC(ret));
  do {
    if (p == pp) {
      // Skip the partition of the missing replica
    } else if (zone_task_idx_ < shadow_zone_tasks_.count()) {
      ZoneTask& task = shadow_zone_tasks_.at(next);
      ObZone& zone = task.name_;  // In which zone does the partition lack a replica
      FOR_BEGIN_END(r, *(*pp), ts_.all_replica_)
      {
        if (r->zone_ == zone && ObReplicaTypeCheck::is_readonly_replica((r)->replica_type_) && (r)->is_in_service() &&
            (r)->server_->can_migrate_in()) {
          need_random_add_replica = false;
          replica_addr.reset();
          replica_addr.unit_id_ = r->unit_->info_.unit_.unit_id_;
          replica_addr.addr_ = (r)->server_->server_;
          replica_addr.zone_ = (r)->zone_;
          replica_addr.replica_type_ = (r)->replica_type_;
          if (OB_FAIL(replica_addr.set_memstore_percent((r)->memstore_percent_))) {
            LOG_WARN("fai lto set memstore percent", KR(ret));
          } else if (OB_FAIL(unit_set_.set_refactored(replica_addr.unit_id_))) {
            LOG_WARN("fail set server to server set", K(replica_addr), KR(ret), K(zone));
          } else {
            LOG_INFO("find replica", K(replica_addr), K(*r));
          }
          break;
        } else {
        }  // nothing todo
      }    // end for replica
    } else {
      break;
    }
    if (OB_SUCC(ret)) {
      if (!need_random_add_replica) {
        break;
      } else {
        __typeof__(pp) next_partition = pp + 1;
        if ((next_partition) >= ts_.sorted_partition_.end() ||
            (*next_partition)->partition_id_ != (*pp)->partition_id_ || (*next_partition)->is_primary() ||
            !is_same_tg(*(*pp), *(*next_partition)) || !(*pp)->can_rereplicate()) {
          break;
        }
        pp = next_partition;
      }
    }
  } while (OB_SUCC(ret));
  return ret;
}

//////////////////////////
ObDeleteReplicaUtility::ObDeleteReplicaUtility(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
    const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list)
    : ObFilterLocalityUtility(zone_mgr, ts, zloc, zone_list)
{}

int ObDeleteReplicaUtility::init()
{
  return ObFilterLocalityUtility::init();
}

void ObDeleteReplicaUtility::reset()
{
  ObFilterLocalityUtility::reset();
}

int ObDeleteReplicaUtility::get_delete_task(const Partition& partition, bool& need_delete, Replica& replica)
{
  int ret = OB_SUCCESS;
  FilterResult result;
  if (OB_FAIL(filter_locality(partition))) {
    LOG_WARN("fail to filter locality", KR(ret));
  } else {
    ObIArray<FilterResult>& results = get_filter_result();
    if (0 == results.count()) {
      // nothing todo
    } else if (has_add_task(results) || has_type_transform_task(results)) {
      // nothing todo
    } else if (OB_FAIL(get_one_delete_task(results, result))) {
      LOG_WARN("fail to get one delete task", KR(ret), K(results));
    }
    // solve the scene that a paxos replica is not on the unit,
    // and have to add a paxos replica
    if (OB_SUCC(ret) && !result.is_valid() && has_add_task(results)) {
      // Ignore the type transfrom task,
      // the premise of the type transform task is that there is no copy task
      const FilterResult* delete_task = NULL;
      for (int64_t i = 0; i < results.count() && OB_SUCC(ret) && OB_ISNULL(delete_task); i++) {
        if (results.at(i).is_delete_task_with_invalid_unit()) {
          if (ObReplicaTypeCheck::is_paxos_replica_V2(results.at(i).get_replica().replica_type_)) {
            for (int64_t j = 0; j < results.count() && OB_SUCC(ret); j++) {
              if (results.at(j).is_add_task() &&
                  ObReplicaTypeCheck::is_paxos_replica_V2(results.at(j).get_dest_type()) &&
                  results.at(j).get_zone() == results.at(i).get_zone()) {
                delete_task = &results.at(i);
              }
            }       // end for
          } else {  // end if
          }
        } else {
        }
      }
    }
  }
  if (OB_SUCC(ret) && result.is_valid()) {
    need_delete = true;
    replica = result.get_replica();
    LOG_INFO("get one delete task", K(partition), K(replica));
  }
  return ret;
}

int ObDeleteReplicaUtility::get_one_delete_task(const ObIArray<FilterResult>& results, FilterResult& result)
{
  int ret = OB_SUCCESS;
  // Directly select one to delete
  // if the replica is the leader,
  // the outer logic is responsible for processing the switch leader operation first

  // the remove member task is generated first
  // to reduce the time consumption of downgrading disaster recovery
  for (int64_t i = 0; i < results.count(); i++) {
    if (ObRebalanceTaskType::MEMBER_CHANGE == results.at(i).get_cmd_type()) {
      result = results.at(i);
      break;
    } else if (ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA == results.at(i).get_cmd_type()) {
      result = results.at(i);
    }
  }
  return ret;
}

///////////////////////
bool FilterResult::is_valid() const
{
  bool bret = true;
  if (cmd_type_ == ObRebalanceTaskType::MAX_TYPE) {
    // LOG_WARN("invalid cmd type", K(cmd_type_));
    bret = false;
  }
  if (bret) {
    if (ObRebalanceTaskType::TYPE_TRANSFORM == cmd_type_) {
      bret = ObReplicaTypeCheck::is_replica_type_valid(dest_type_) && dest_memstore_percent_ <= 100 &&
             dest_memstore_percent_ >= 0;
    } else if (ObRebalanceTaskType::ADD_REPLICA == cmd_type_) {
      bret = ((!zone_.is_empty() || !region_.is_empty()) && ObReplicaTypeCheck::is_replica_type_valid(dest_type_) &&
              dest_memstore_percent_ <= 100 && dest_memstore_percent_ >= 0);
    }
  }
  return bret;
}

void FilterResult::build_type_transform_task(
    const Replica& replica, const ObReplicaType& dest_type, const int64_t memstore_percent)
{
  cmd_type_ = ObRebalanceTaskType::TYPE_TRANSFORM;
  replica_ = replica;
  dest_type_ = dest_type;
  dest_memstore_percent_ = memstore_percent;
}

void FilterResult::build_add_task(
    const ObRegion& region, const ObZone& zone, const ObReplicaType& dest_type, const int64_t dest_memstore_percent)
{
  cmd_type_ = ObRebalanceTaskType::ADD_REPLICA;
  region_ = region;
  zone_ = zone;
  dest_type_ = dest_type;
  dest_memstore_percent_ = dest_memstore_percent;
}

void FilterResult::build_delete_task(const uint64_t tenant_id, const uint64_t table_id, bool is_standby_cluster,
    const Replica& replica, bool invalid_unit)
{
  UNUSED(tenant_id);
  UNUSED(table_id);
  UNUSED(is_standby_cluster);
  if (ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)) {
    cmd_type_ = ObRebalanceTaskType::MEMBER_CHANGE;
  } else {
    cmd_type_ = ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA;
  }
  replica_ = replica;
  invalid_unit_ = invalid_unit;
}

int FilterResult::build_task(const Partition& partition, ObReplicaTask& task) const
{
  int ret = OB_SUCCESS;
  task.reset();
  if (!partition.get_key().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition", KR(ret), K(partition));
  } else if (ObRebalanceTaskType::TYPE_TRANSFORM != cmd_type_ &&
             ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA != cmd_type_ &&
             ObRebalanceTaskType::MEMBER_CHANGE != cmd_type_ && ObRebalanceTaskType::ADD_REPLICA != cmd_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter result", KR(ret), K_(cmd_type));
  } else if (ObRebalanceTaskType::ADD_REPLICA != cmd_type_ && OB_ISNULL(replica_.server_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server stat is null", KR(ret), K_(replica));
  } else {
    task.tenant_id_ = extract_tenant_id(partition.table_id_);
    task.table_id_ = partition.table_id_;
    task.partition_id_ = partition.partition_id_;
    task.cmd_type_ = cmd_type_;
    if (ObRebalanceTaskType::TYPE_TRANSFORM == cmd_type_) {
      task.src_ = replica_.server_->server_;
      task.replica_type_ = replica_.replica_type_;
      task.memstore_percent_ = replica_.get_memstore_percent();
      task.zone_ = replica_.zone_;
      task.region_ = replica_.region_;
      task.dst_replica_type_ = dest_type_;
      task.dst_memstore_percent_ = dest_memstore_percent_;
      task.comment_ = balancer::LOCALITY_TYPE_TRANSFORM;
    } else if (ObRebalanceTaskType::ADD_REPLICA == cmd_type_) {
      task.zone_ = zone_;
      task.region_ = region_;
      task.dst_replica_type_ = dest_type_;
      task.dst_memstore_percent_ = dest_memstore_percent_;
      task.comment_ = balancer::REPLICATE_ENOUGH_REPLICA;
    } else if (ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA == cmd_type_) {
      task.src_ = replica_.server_->server_;
      task.replica_type_ = replica_.replica_type_;
      task.zone_ = replica_.zone_;
      task.region_ = replica_.region_;
      task.comment_ = balancer::LOCALITY_REMOVE_REDUNDANT_REPLICA;
    } else if (ObRebalanceTaskType::MEMBER_CHANGE == cmd_type_) {
      task.src_ = replica_.server_->server_;
      task.replica_type_ = replica_.replica_type_;
      task.zone_ = replica_.zone_;
      task.region_ = replica_.region_;
      task.comment_ = balancer::LOCALITY_REMOVE_REDUNDANT_MEMBER;
    }
  }
  return ret;
}
///////////////////////
ObFilterLocalityUtility::ObFilterLocalityUtility(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
    const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list)
    : ObLocalityUtility(zone_mgr, zloc, zone_list),
      ts_(&ts),
      table_id_(common::OB_INVALID_INDEX_INT64),
      partition_id_(common::OB_INVALID_INDEX_INT64),
      replicas_(),
      results_(),
      unit_mgr_(NULL),
      tenant_id_(OB_INVALID_ID),
      zone_mgr_(&zone_mgr),
      has_balance_info_(true)
{}

ObFilterLocalityUtility::ObFilterLocalityUtility(const ObZoneManager& zone_mgr,
    const share::schema::ZoneLocalityIArray& zloc, const ObUnitManager& unit_mgr, const uint64_t tenant_id,
    const common::ObIArray<common::ObZone>& zone_list)
    : ObLocalityUtility(zone_mgr, zloc, zone_list),
      ts_(NULL),
      table_id_(common::OB_INVALID_INDEX_INT64),
      partition_id_(common::OB_INVALID_INDEX_INT64),
      replicas_(),
      results_(),
      unit_mgr_(&unit_mgr),
      tenant_id_(tenant_id),
      zone_mgr_(&zone_mgr),
      has_balance_info_(true)
{}

int ObFilterLocalityUtility::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLocalityUtility::init(true, true))) {
    LOG_WARN("fail to init", KR(ret));
  }
  return ret;
}

// Compare partition with locality:
// Filter out the replicas that match the locality,
// and leave the replicas that do not match the locality in the results.
//
// At the same time, the difference between these replicas and locality is recorded,
// and the user obtains the result through get_filter_result()
// According to locality, handle readonly@all and non-readonly@all separately
// Prioritize non-readonly@all locality
//
// The whole process is divided into four steps:
// Step 0: For the replica on l_unit, directly generate the corresponding task according to the replica;
// Step 1: Compare the locality of non-readonly@all,
//         filter out the replica location and type in the partition that match the locality;
// Step 2: Compare the locality of non-readonly@all,
//         Filter out the replicas whose location is satisfied but the replica type is not satisfied;
//         And generate type_transform task for these replica and store in result
// Step 3: Handling the locality of readonly@all,
//         If the replica type is not READONLY, add the type_transform task,
//         If the readonly replica is not enough, add the add_replica task
//         If teh readonly replica is too much, add remove_replica task.
// Step 4: Handle the remaining replicas and locality,
//         The remaining locality means that the corresponding replica is missing,
//         and add the add_replica task to the result
//         The remaining replicas indicate that they are redundant replicas,
//         and add the remove_replica task to the result;
int ObFilterLocalityUtility::filter_locality(const Partition& partition)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(partition))) {
    LOG_WARN("fail to init partition info", KR(ret), K(table_id_), K(partition_id_), K(partition));
  } else if (OB_FAIL(do_filter_locality())) {
    LOG_WARN("fail to do filter locality", KR(ret), K(table_id_), K(partition_id_), K(partition));
  } else {
  }  // no more to do
  return ret;
}

int ObFilterLocalityUtility::filter_locality(const ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(partition_info))) {
    LOG_WARN("fail to init partition info", KR(ret), K(partition_info));
  } else if (OB_FAIL(do_filter_locality())) {
    LOG_WARN("fail to do filter locality", KR(ret), K(table_id_), K(partition_id_), K(partition_info));
  } else {
  }  // no more to do
  return ret;
}

// The remaining replicas in the zone in the current replicas_ belong to readonly@all
// As long as there is a unit, the replica seists reasonably,
// so delete the replica which is not in unit
int ObFilterLocalityUtility::delete_redundant_replica(const ObZone& zone, const int64_t delete_cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < delete_cnt; i++) {
    if (OB_FAIL(choose_one_replica_to_delete(zone))) {
      LOG_WARN("fail to choose one replica to delete", KR(ret));
    }
  }
  return ret;
}

int ObFilterLocalityUtility::choose_one_replica_to_delete(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  bool find = false;
  // First find the replica without unit
  for (int64_t i = 0; OB_SUCC(ret) && i < replicas_.count() && !find; i++) {
    Replica& replica = replicas_.at(i);
    if (!replica.is_in_service() || replica.zone_ != zone) {
      // nothing todo
    } else if (NULL == replica.unit_ || !replica.unit_->in_pool_) {
      if (OB_FAIL(add_remove_task(replica, true))) {
        LOG_WARN("fail to add remove task", KR(ret), K(replica));
      } else if (OB_FAIL(replicas_.remove(i))) {
        LOG_WARN("fail to remove replica", KR(ret), K(i), K(replicas_));
      } else {
        find = true;
      }
      if (NULL != replica.unit_) {
        LOG_INFO("replica unit has been removed, add remove task",
            K(replica),
            "unit_in_pool",
            replica.unit_->in_pool_,
            "unit",
            replica.unit_->info_.unit_,
            K(table_id_),
            K(partition_id_));
      } else {
        LOG_INFO("replica unit null, add remove task for alter locality checker",
            K(replica),
            K(table_id_),
            K(partition_id_));
      }
    }
  }
  // Find replica of the same unit
  for (int64_t i = 0; OB_SUCC(ret) && i < replicas_.count() && !find; i++) {
    Replica& replica = replicas_.at(i);
    if (!replica.is_in_service() || replica.zone_ != zone) {
      // nothing todo
    } else if (!replica.is_in_unit()) {
      // Find out if there is already a replica on the unit
      for (int64_t j = 0; j < bak_replicas_.count() && OB_SUCC(ret); j++) {
        if (bak_replicas_.at(j).is_in_service() && bak_replicas_.at(j).zone_ == zone &&
            bak_replicas_.at(j).get_unit_id() == replica.get_unit_id() && bak_replicas_.at(j).is_in_unit()) {
          LOG_INFO("replica not in unit, and there is already one replica in unit",
              K(replica),
              "exist_replica",
              bak_replicas_.at(j),
              "unit_id",
              replica.get_unit_id(),
              "unit_addr",
              replica.get_unit_addr(),
              K_(table_id),
              K_(partition_id));
          if (OB_FAIL(add_remove_task(replica))) {
            LOG_WARN("fail to add remove task", KR(ret), K(replica));
          } else if (OB_FAIL(replicas_.remove(i))) {
            LOG_WARN("fail to remove replica", KR(ret), K(i), K(replicas_));
          } else {
            find = true;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !find) {
    LOG_WARN("fail to find replica to remove", KR(ret), K(zone), K_(table_id), K_(partition_id), K_(bak_replicas));
  }
  return ret;
}

int ObFilterLocalityUtility::do_filter_locality()
{
  int ret = OB_SUCCESS;
  // Step 0: except the replica that the unit = -1
  for (int64_t j = 0; OB_SUCC(ret) && j < replicas_.count(); j++) {
    Replica& replica = replicas_.at(j);
    if (has_balance_info_ && replica.is_in_service() && OB_ISNULL(replica.unit_) && replica.server_->online_) {
      LOG_INFO("replica has no unit info", K(replica));
      if (OB_FAIL(add_remove_task(replica, true))) {
        LOG_WARN("fail to add remove task", KR(ret), K(replica));
      } else {
        replica.replica_status_ = REPLICA_STATUS_OFFLINE;
        // Avoid modifying the array in the loop; directly set the replica to invalid
      }
    }
  }
  // Step 1: Compare the locality of non-readonly@all,
  // filter out the replica location, type, and memstore_percent in the partition that match the locality
  int64_t remove_idx = OB_INVALID_INDEX;
  int64_t candidate_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < shadow_zone_tasks_.count(); i++) {
    ZoneTask& task = shadow_zone_tasks_.at(i);
    remove_idx = OB_INVALID_INDEX;
    candidate_idx = OB_INVALID_INDEX;
    for (int64_t j = 0; OB_SUCC(ret) && j < replicas_.count(); j++) {
      Replica& replica = replicas_.at(j);
      if (!replica.is_in_service() || replica.zone_ != task.name_ || replica.replica_type_ != task.replica_type_ ||
          replica.memstore_percent_ != task.memstore_percent_ ||
          (has_balance_info_ && replica.server_->permanent_offline_)) {
        // nothing todo
      } else if (replica.is_in_unit()) {
        // Preferred replica of in_unit
        remove_idx = j;
        break;
      } else if (OB_INVALID_INDEX == candidate_idx) {
        candidate_idx = j;
      }
      LOG_DEBUG("process replica", K(i), K(replica), K(remove_idx), K(candidate_idx));
    }
    remove_idx = (remove_idx == OB_INVALID_INDEX) ? candidate_idx : remove_idx;
    if (OB_SUCC(ret) && OB_INVALID_INDEX != remove_idx) {
      if (OB_FAIL(replicas_.remove(remove_idx))) {
        LOG_WARN("fail to remove item", KR(ret), K(remove_idx), K(replicas_));
      } else if (OB_FAIL(shadow_zone_tasks_.remove(i))) {
        LOG_WARN("fail to remove item", KR(ret), K(i), K(shadow_zone_tasks_));
      } else {
        i--;
      }
    }
  }  // end for
  // Step 1.1: Compare the locality of non-readonly@all,
  //           filter out the replicas whose location is satisfied,
  //           the replica type is satisfied,
  //           but the memstore_percent is not satisfied
  if (OB_SUCC(ret)) {
    int64_t remove_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < shadow_zone_tasks_.count(); i++) {
      ZoneTask& task = shadow_zone_tasks_.at(i);
      remove_idx = OB_INVALID_INDEX;
      candidate_idx = OB_INVALID_INDEX;
      for (int64_t j = 0; OB_SUCC(ret) && j < replicas_.count(); j++) {
        Replica& replica = replicas_.at(j);
        if (!replica.is_in_service() || replica.zone_ != task.name_ || replica.replica_type_ != task.replica_type_ ||
            (has_balance_info_ && replica.server_->permanent_offline_)) {
          // nothing todo
        } else if (ObMemstorePercentCheck::is_memstore_percent_valid(task.memstore_percent_) &&
                   ObMemstorePercentCheck::is_memstore_percent_valid(replica.memstore_percent_)) {
          remove_idx = j;
          break;
        } else if (replica.is_in_unit()) {
          candidate_idx = j;
        } else if (OB_INVALID_INDEX == candidate_idx) {
          candidate_idx = j;
        }
      }
      remove_idx = (remove_idx == OB_INVALID_INDEX) ? candidate_idx : remove_idx;
      if (OB_SUCC(ret) && OB_INVALID_INDEX != remove_idx) {
        if (OB_FAIL(add_type_transform_task(replicas_.at(remove_idx), task.replica_type_, task.memstore_percent_))) {
          LOG_WARN("fail to add type transform task",
              KR(ret),
              "replica",
              replicas_.at(remove_idx),
              K(task),
              K(table_id_),
              K(partition_id_));
        } else if (OB_FAIL(replicas_.remove(remove_idx))) {
          LOG_WARN("fail to remove item", KR(ret), K(remove_idx), K(replicas_));
        } else if (OB_FAIL(shadow_zone_tasks_.remove(i))) {
          LOG_WARN("fail to remove item", KR(ret), K(i), K(shadow_zone_tasks_));
        } else {
          i--;
        }
      }
    }  // end for
  }

  // Step 2: Compare the locality of non-readonly@all,
  //         filter out the replicas
  //         whose location is satisfied but the replica type is not satisfied.
  //         And add the type_transform task for these replicas to the result.
  //
  //         When doing the type transform,
  //         the replica of the similar type is preferred;
  //         for example, the priority is to transform L to F
  if (OB_SUCC(ret)) {
    int64_t remove_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < shadow_zone_tasks_.count(); i++) {
      ZoneTask& task = shadow_zone_tasks_.at(i);
      remove_idx = OB_INVALID_INDEX;
      candidate_idx = OB_INVALID_INDEX;
      for (int64_t j = 0; OB_SUCC(ret) && j < replicas_.count(); j++) {
        Replica& replica = replicas_.at(j);
        if (!replica.is_in_service() || replica.zone_ != task.name_ ||
            (has_balance_info_ && replica.server_->permanent_offline_)) {
          // nothing todo
        } else if (ObReplicaTypeCheck::is_paxos_replica_V2(task.replica_type_) &&
                   ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)) {
          remove_idx = j;
          break;
        } else if (replica.is_in_unit()) {
          candidate_idx = j;
        } else if (OB_INVALID_INDEX == candidate_idx) {
          candidate_idx = j;
        }
      }
      remove_idx = (remove_idx == OB_INVALID_INDEX) ? candidate_idx : remove_idx;
      if (OB_SUCC(ret) && OB_INVALID_INDEX != remove_idx) {
        if (OB_FAIL(add_type_transform_task(replicas_.at(remove_idx), task.replica_type_, task.memstore_percent_))) {
          LOG_WARN("fail to add type transform task",
              KR(ret),
              "replica",
              replicas_.at(remove_idx),
              K(task),
              K(table_id_),
              K(partition_id_));
        } else if (OB_FAIL(replicas_.remove(remove_idx))) {
          LOG_WARN("fail to remove item", KR(ret), K(remove_idx), K(replicas_));
        } else if (OB_FAIL(shadow_zone_tasks_.remove(i))) {
          LOG_WARN("fail to remove item", KR(ret), K(i), K(shadow_zone_tasks_));
        } else {
          i--;
        }
      }
    }  // end for
  }

  // Step 3: Handling the locality of readonly@all
  if (OB_SUCC(ret)) {
    if (OB_FAIL(filter_readonly_at_all())) {
      LOG_WARN("fail to filter readonly at all", KR(ret), K(table_id_), K(partition_id_));
    } else if (OB_FAIL(process_remain_info())) {
      // Step 4: Handle the remaining replicas and locality
      LOG_WARN("fail to process remain info", KR(ret), K(table_id_), K(partition_id_));
    }
  }
  return ret;
}

// Handle the locality of readonly@all separately
int ObFilterLocalityUtility::filter_readonly_at_all()
{
  int ret = OB_SUCCESS;
  int64_t expect_readonly_count = 0;
  FOREACH_CNT_X(zone, all_server_zones_, OB_SUCC(ret))
  {
    if (OB_ISNULL(zone)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid locality", KR(ret));
    } else if (OB_FAIL(get_readonly_replica_count(*zone, expect_readonly_count))) {
      LOG_WARN("fail to get readonly replica count", KR(ret), K(table_id_), K(partition_id_));
    } else {
      int64_t readonly_count = 0;
      for (int64_t i = 0; i < replicas_.count() && OB_SUCC(ret); i++) {
        Replica& replica = replicas_.at(i);
        if (replica.is_in_service() && replica.zone_ == *zone) {
          // all the remaining replicas should be READONLY
          readonly_count++;
        }
      }  // end for
      if (readonly_count > expect_readonly_count) {
        int64_t need_delete_count = readonly_count - expect_readonly_count;
        if (OB_FAIL(delete_redundant_replica(*zone, need_delete_count))) {
          LOG_WARN("fail to delete redundant replica", KR(ret), K(*zone), K(need_delete_count));
        } else {
          LOG_WARN("filter readonly@all localicy, more replica",
              K(*zone),
              K(readonly_count),
              K(expect_readonly_count),
              K(table_id_),
              K(partition_id_));
        }
      } else if (readonly_count < expect_readonly_count) {
        ObRegion null_region;
        const int64_t memstore_percent = 100;
        if (OB_FAIL(add_rereplicate_task(null_region, *zone, REPLICA_TYPE_READONLY, memstore_percent))) {
          LOG_WARN("fail to add rereplicate task", KR(ret), K(table_id_), K(partition_id_));
        } else {
          LOG_WARN("filter readonly@all locality, lost replica",
              K(*zone),
              K(readonly_count),
              K(expect_readonly_count),
              K(REPLICA_TYPE_READONLY),
              K(table_id_),
              K(partition_id_));
        }
      }
      for (int64_t i = 0; i < replicas_.count() && OB_SUCC(ret); i++) {
        const int64_t memstore_percent = 100;
        Replica& replica = replicas_.at(i);
        if (replica.is_in_service() && replica.zone_ == *zone) {
          if (REPLICA_TYPE_READONLY == replica.replica_type_) {
            // nothing todo
          } else if (OB_FAIL(add_type_transform_task(replica, REPLICA_TYPE_READONLY, memstore_percent))) {
            LOG_WARN("fail to add type transform task", KR(ret), K(replica), K(table_id_), K(partition_id_));
          } else {
            LOG_WARN("filter readonly at all locality, replica type mismatch locality",
                K(replica),
                K(table_id_),
                K(partition_id_));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(replicas_.remove(i))) {
              LOG_WARN("fail to remove replica", KR(ret), K(i));
            } else {
              i--;
            }
          }
        }  // end if (replica.is_in_service() && replica.zone_ == *zone) {
      }    // end for
    }
  }
  return ret;
}

int ObFilterLocalityUtility::process_remain_info()
{
  int ret = OB_SUCCESS;
  ObRegion null_region;
  ObZone null_zone;
  for (int64_t i = 0; i < shadow_zone_tasks_.count(); i++ && OB_SUCC(ret)) {
    ZoneTask& task = shadow_zone_tasks_.at(i);
    if (OB_FAIL(add_rereplicate_task(null_region, task.name_, task.replica_type_, task.memstore_percent_))) {
      LOG_WARN("fail to add rereplicate task", KR(ret), K(task), K(table_id_), K(partition_id_));
    } else {
      LOG_DEBUG("filter locality, lost replica", K(task), K(table_id_), K(partition_id_));
    }
  }
  for (int64_t i = 0; i < replicas_.count(); i++ && OB_SUCC(ret)) {
    if (!replicas_.at(i).is_in_service() || (has_balance_info_ && replicas_.at(i).server_->permanent_offline_)) {
    } else if (common::REPLICA_TYPE_READONLY == replicas_.at(i).replica_type_ &&
               ObTimeUtility::current_time() - replicas_.at(i).modify_time_us_ < SAFE_REMOVE_REDUNDANCY_REPLICA_TIME) {
      // Delay the deletion of newly created R-type replicas to avoid the case where the source
      // and destination replicas are normal when the R-type replica is migrated
      //
      // The destination replica was deleted due to locality detection
      LOG_WARN("skip to remove new replica", "replica", replicas_.at(i), K(table_id_), K(partition_id_));
    } else if (OB_FAIL(add_remove_task(replicas_.at(i)))) {
      LOG_WARN("fail to add remove task", KR(ret), "replica", replicas_.at(i), K(table_id_), K(partition_id_));
    } else {
      LOG_WARN(
          "has more replica than locality, need delete", "replica", replicas_.at(i), K(table_id_), K(partition_id_));
    }
  }
  return ret;
}

int ObFilterLocalityUtility::get_readonly_replica_count(const common::ObZone& zone, int64_t& readonly_count)
{
  int ret = OB_SUCCESS;
  int64_t unit_count = 0;
  int64_t non_readonly_replica_count = 0;
  readonly_count = 0;
  if (OB_FAIL(get_unit_count(zone, unit_count))) {
    LOG_WARN("fail to get unit count", KR(ret), K(zone));
  } else if (OB_FAIL(get_non_readonly_replica_count(zone, non_readonly_replica_count))) {
    LOG_WARN("fail to get non readonly replica", KR(ret), K(zone));
  } else {
    // The calculated readonly_count may be wrong:
    // If there are not enough replicas of locality@region,
    // nothing will be done during the entire type conversion process.
    // If the replica of locality@region is enough,
    // the calculation result of this function is correct.
    readonly_count = unit_count - non_readonly_replica_count;
  }
  return ret;
}

int ObFilterLocalityUtility::get_non_readonly_replica_count(
    const common::ObZone& zone, int64_t& non_readonly_replica_count)
{
  int ret = OB_SUCCESS;
  non_readonly_replica_count = 0;
  bool find = false;
  for (int64_t i = 0; i < zone_replica_info_.count(); i++) {
    if (zone_replica_info_.at(i).zone_ == zone) {
      non_readonly_replica_count = zone_replica_info_.at(i).non_readonly_replica_count_;
      find = true;
      break;
    }
  }
  if (false == find) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find readonly info", KR(ret), K(zone), K(zone_replica_info_));
  }
  return ret;
}

int ObFilterLocalityUtility::get_unit_count(const common::ObZone& zone, int64_t& unit_count)
{
  int ret = OB_SUCCESS;
  unit_count = 0;
  if (NULL != ts_) {
    FOREACH_CNT(zone_unit, ts_->all_zone_unit_)
    {
      if (OB_ISNULL(zone_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid zone unit", KR(ret));
      } else if (zone_unit->zone_ == zone) {
        unit_count = zone_unit->all_unit_.count();
        break;
      }
    }
  } else {
    common::ObArray<share::ObUnitInfo> unit_infos;
    if (NULL == unit_mgr_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_mgr_ is null", KR(ret), KP(unit_mgr_));
    } else if (OB_FAIL(unit_mgr_->get_zone_active_unit_infos_by_tenant(tenant_id_, zone, unit_infos))) {
      LOG_WARN("fail to get zone unit infos by tenant", KR(ret), K(tenant_id_), K(zone));
    } else {
      unit_count = unit_infos.count();
    }
  }
  return ret;
}

int ObFilterLocalityUtility::inner_init(const Partition& partition)
{
  int ret = OB_SUCCESS;
  results_.reset();
  replicas_.reset();
  shadow_zone_tasks_.reset();
  table_id_ = partition.table_id_;
  partition_id_ = partition.partition_id_;
  has_balance_info_ = true;
  if (OB_FAIL(shadow_zone_tasks_.assign(zone_tasks_))) {
    LOG_WARN("fail to assign zone task", KR(ret));
  } else if (OB_UNLIKELY(NULL == ts_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ts_ is null", KR(ret), KP(ts_));
  } else {
    FOR_BEGIN_END_E(replica, partition, ts_->all_replica_, OB_SUCC(ret))
    {
      if (OB_ISNULL(replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", KR(ret), K(partition));
      } else if (OB_FAIL(replicas_.push_back(*replica))) {
        LOG_WARN("fail to push back replica", KR(ret), K(partition), K(*replica));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_readonly_info())) {
      LOG_WARN("fail to build readonly info", KR(ret));
    } else if (OB_FAIL(bak_replicas_.assign(replicas_))) {
      LOG_WARN("fail to assign array", KR(ret), K_(table_id), K_(partition_id));
    }
  }
  return ret;
}

// Count the number of non-readonly replicas in readonly@all zone
int ObFilterLocalityUtility::build_readonly_info()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(zone, all_server_zones_, OB_SUCC(ret))
  {
    int64_t non_readonly_replica_count = 0;
    FOREACH_CNT_X(task, zone_tasks_, OB_SUCC(ret))
    {
      if (task->name_ == *zone && task->replica_type_ != REPLICA_TYPE_READONLY) {
        non_readonly_replica_count++;
      }
    }
    if (OB_SUCC(ret)) {
      ZoneReplicaInfo info;
      info.zone_ = *zone;
      info.non_readonly_replica_count_ = non_readonly_replica_count;
      if (OB_FAIL(zone_replica_info_.push_back(info))) {
        LOG_WARN("fail to push back", KR(ret), K(info));
      }
    }
  }
  return ret;
}

int ObFilterLocalityUtility::inner_init(const ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  results_.reset();
  replicas_.reset();
  shadow_zone_tasks_.reset();
  table_id_ = partition_info.get_table_id();
  partition_id_ = partition_info.get_partition_id();

  has_balance_info_ = false;
  if (OB_FAIL(shadow_zone_tasks_.assign(zone_tasks_))) {
    LOG_WARN("fail to assign zone task", KR(ret));
  } else if (OB_UNLIKELY(NULL == zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_mgr_ is null", KR(ret), KP(zone_mgr_));
  } else {
    HEAP_VAR(share::ObZoneInfo, zone_info)
    {
      const ObPartitionInfo::ReplicaArray& replica_array = partition_info.get_replicas_v2();
      for (int64_t i = 0; i < replica_array.count() && OB_SUCC(ret); ++i) {
        const ObPartitionReplica& partition_replica = replica_array.at(i);
        zone_info.reset();
        zone_info.zone_ = partition_replica.zone_;
        Replica replica;
        if (OB_FAIL(zone_mgr_->get_zone(zone_info))) {
          LOG_WARN("fail to get zone", KR(ret));
        } else if (OB_FAIL(replica.region_.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", KR(ret));
        } else {
          // replica.unit_ do not be set;
          // replica.server_ do not be set;
          replica.zone_ = partition_replica.zone_;
          replica.member_time_us_ = partition_replica.member_time_us_;
          replica.role_ = partition_replica.role_;
          replica.set_in_member_list(partition_replica.in_member_list_);
          replica.replica_status_ = partition_replica.replica_status_;
          replica.rebuild_ = partition_replica.rebuild_;
          replica.data_version_ = partition_replica.data_version_;
          replica.replica_type_ = partition_replica.replica_type_;
          replica.memstore_percent_ = partition_replica.get_memstore_percent();
          // replica.readonly_at_all_ do not be set;
          if (OB_FAIL(replicas_.push_back(replica))) {
            LOG_WARN("fail to push back", KR(ret));
          } else {
          }  // no more to do
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_readonly_info())) {
      LOG_WARN("fail to build readonly info", KR(ret));
    } else if (OB_FAIL(bak_replicas_.assign(replicas_))) {
      LOG_WARN("fail to assign array", KR(ret), K_(table_id), K_(partition_id));
    }
  }
  return ret;
}

int ObFilterLocalityUtility::add_type_transform_task(
    const Replica& replica, const common::ObReplicaType& type, const int64_t memstore_percent)
{
  int ret = OB_SUCCESS;
  FilterResult result;
  result.build_type_transform_task(replica, type, memstore_percent);
  if (OB_FAIL(results_.push_back(result))) {
    LOG_WARN("fail to push back", KR(ret), K(result));
  } else {
    LOG_INFO("add type transform task", K(result), K(replica), K(type), K(table_id_), K(partition_id_));
  }
  return ret;
}

int ObFilterLocalityUtility::add_rereplicate_task(
    const ObRegion& region, const ObZone& zone, const common::ObReplicaType& type, const int64_t memstore_percent)
{
  int ret = OB_SUCCESS;
  FilterResult result;
  result.build_add_task(region, zone, type, memstore_percent);
  if (OB_FAIL(results_.push_back(result))) {
    LOG_WARN("fail to push back", KR(ret), K(result));
  } else {
    LOG_INFO("add rereplicate task", K(region), K(zone), K(type), K(table_id_), K(partition_id_));
  }
  return ret;
}

int ObFilterLocalityUtility::add_remove_task(const Replica& replica, bool invalid_unit)
{
  int ret = OB_SUCCESS;
  FilterResult result;
  const uint64_t tenant_id = (nullptr != ts_ ? ts_->tenant_id_ : tenant_id_);
  result.build_delete_task(tenant_id, table_id_, GCTX.is_standby_cluster(), replica, invalid_unit);
  if (OB_FAIL(results_.push_back(result))) {
    LOG_WARN("fail to push back", KR(ret), K(result));
  } else {
    LOG_INFO("add remove task", K(replica), K(invalid_unit), K(table_id_), K(partition_id_));
  }
  return ret;
}

bool ObFilterLocalityUtility::has_add_task(ObIArray<FilterResult>& results)
{
  bool bret = false;
  for (int64_t i = 0; i < results.count(); i++) {
    if (ObRebalanceTaskType::ADD_REPLICA == results.at(i).get_cmd_type()) {
      bret = true;
      break;
    }
  }
  return bret;
}

bool ObFilterLocalityUtility::has_type_transform_task(ObIArray<FilterResult>& results)
{
  bool bret = false;
  for (int64_t i = 0; i < results.count(); i++) {
    if (ObRebalanceTaskType::TYPE_TRANSFORM == results.at(i).get_cmd_type()) {
      bret = true;
      break;
    }
  }
  return bret;
}

////////////////////////
ObReplicaTypeTransformUtility::ObReplicaTypeTransformUtility(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
    const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list,
    const ObServerManager& server_mgr)
    : ObFilterLocalityUtility(zone_mgr, ts, zloc, zone_list), server_mgr_(&server_mgr)
{}

int ObReplicaTypeTransformUtility::init()
{
  return ObFilterLocalityUtility::init();
}

void ObReplicaTypeTransformUtility::reset()
{
  ObFilterLocalityUtility::reset();
}

// Count the count of units that are not permanently offline
int ObReplicaTypeTransformUtility::get_unit_count(
    const ObZone& zone, int64_t& active_unit_count, int64_t& inactive_unit_count)
{
  int ret = OB_SUCCESS;
  active_unit_count = 0;
  inactive_unit_count = 0;
  if (OB_UNLIKELY(NULL == ts_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ts is null", KR(ret), KP(ts_));
  } else {
    FOREACH_CNT_X(zone_unit, ts_->all_zone_unit_, OB_SUCC(ret))
    {
      if (OB_ISNULL(zone_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid zone unit", KR(ret));
      } else if (zone_unit->zone_ == zone) {
        FOREACH_CNT_X(unit, zone_unit->all_unit_, OB_SUCC(ret))
        {
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid unit", KR(ret));
          } else if (!(*unit)->server_->is_permanent_offline()) {
            active_unit_count++;
          } else {
            inactive_unit_count++;
          }
        }
        break;
      }
    }
  }
  return ret;
}

int ObReplicaTypeTransformUtility::get_replica_count(
    const Partition& partition, const ObZone& zone, int64_t& replica_count)
{
  int ret = OB_SUCCESS;
  replica_count = 0;
  if (OB_UNLIKELY(NULL == ts_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ts is null", KR(ret), KP(ts_));
  } else {
    FOR_BEGIN_END_E(replica, partition, ts_->all_replica_, OB_SUCC(ret))
    {
      if (OB_ISNULL(replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", KR(ret), K(partition));
      } else if (replica->is_in_service() && replica->zone_ == zone) {
        replica_count++;
      }
    }
  }
  return ret;
}

bool ObReplicaTypeTransformUtility::all_unit_have_replica(const Partition& partition, const ObZone& zone)
{
  bool bret = false;
  int64_t active_unit_count = 0;
  int64_t inactive_unit_count = 0;
  int64_t replica_count = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_unit_count(zone, active_unit_count, inactive_unit_count))) {
    LOG_WARN("fail to get active unit count", KR(ret), K(zone), K(partition));
  } else if (OB_FAIL(get_replica_count(partition, zone, replica_count))) {
    LOG_WARN("fail to get replica count", KR(ret), K(partition), K(zone));
  } else if (replica_count > 0 && replica_count == active_unit_count) {
    LOG_INFO("all unit in zone filled with replica", K(partition), K(zone), K(active_unit_count), K(replica_count));
    bret = true;
  }
  return bret;
}

// Deal with the situation where the paxos replica needs to be supplemented by type_transform
int ObReplicaTypeTransformUtility::check_paxos_replica(
    const Partition& partition, const ObIArray<FilterResult>& results, FilterResult& result)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < results.count(); i++ && OB_SUCC(ret)) {
    if (ObRebalanceTaskType::ADD_REPLICA == results.at(i).get_cmd_type() &&
        ObReplicaTypeCheck::is_paxos_replica_V2(results.at(i).get_dest_type())) {
      // Missing replica of paxos type
      // If it is a simple few replicas, use rerepliate to supplement the copy, then do not do this here
      // Here will distinguish whether the current location is not found to supplement the replica of paoxos;
      // if so, the supplementary replica is reached through type_transform
      if (all_unit_have_replica(partition, results.at(i).get_zone()) && !can_migrate_unit(results.at(i).get_zone())) {
        LOG_WARN("lost paxos replica, but no place for it", K(partition), "result", results.at(i));
        Replica replica;
        if (OB_FAIL(choose_replica_for_type_transform(
                partition, results.at(i).get_zone(), results.at(i).get_dest_type(), replica))) {
          LOG_WARN("fail to choose replica", KR(ret), K(partition));
          ret = OB_SUCCESS;
          break;
        } else {
          result.build_type_transform_task(
              replica, results.at(i).get_dest_type(), results.at(i).get_dest_memstore_percent());
          LOG_WARN("choose replica for type transform to get paxos replica",
              K(replica),
              "dest_type",
              results.at(i).get_dest_type());
          break;
        }
      } else {
        LOG_DEBUG("lost replica, but not rereplicate here", K(partition), K(result));
      }
    }
  }
  return ret;
}

// If there are replicas on all units, but less paxos replica,
// There will be a badcase if you directly select a replica for type conversion.
// If there is a permanently offline unit at this time,
// it is best to supplement the replica by migrating the unit.
// Rather than do the type conversion first, and then do the non-paxos replica.
// Therefore, check logic is added here,
// if the unit can be migrated, the type will not be changed
bool ObReplicaTypeTransformUtility::can_migrate_unit(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  int64_t active_unit_count = 0;
  int64_t inactive_unit_count = 0;
  int64_t active_server_count = 0;
  ObArray<share::ObServerStatus> server_stat;
  if (OB_FAIL(get_unit_count(zone, active_unit_count, inactive_unit_count))) {
    LOG_WARN("fail to get active unit count", KR(ret), K(zone));
  } else if (OB_FAIL(server_mgr_->get_server_statuses(zone, server_stat))) {
    LOG_WARN("fail to get server stat", KR(ret), K(zone));
  } else {
    for (int64_t i = 0; i < server_stat.count() && OB_SUCC(ret); i++) {
      if (server_stat.at(i).can_migrate_in()) {
        active_server_count++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (inactive_unit_count > 0 && active_server_count > active_unit_count) {
      bret = true;
    }
  }
  return bret;
}

// Choose a replica as the source to transform to a paxos replica;
// 1.If it is a few L replicas, the first choice is to transform the replica on L_UNIT
// 2.Cannot choose the replica on L_UNIT to be converted to non-LOGONLY replica;
// 3.Cannot convert F to L; unless unit=l_unit
// 4.Directly select the non-paxos replica with the largest version number
// 5.Implementation restrictions, now only supports F->L, R->F, F->R
int ObReplicaTypeTransformUtility::choose_replica_for_type_transform(
    const Partition& partition, const ObZone& zone, const ObReplicaType& dest_replica_type, Replica& replica)
{
  int ret = OB_SUCCESS;
  Replica* tmp_replica = NULL;
  if (OB_UNLIKELY(NULL == ts_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ts is null", KR(ret), KP(ts_));
  } else {
    FOR_BEGIN_END_E(r, partition, ts_->all_replica_, OB_SUCC(ret))
    {
      if (r->zone_ == zone && r->is_in_service() && r->server_->can_migrate_in() &&
          !r->is_in_blacklist(ObRebalanceTaskType::TYPE_TRANSFORM, r->server_->server_, ts_)) {
        if (REPLICA_TYPE_FULL == dest_replica_type && REPLICA_TYPE_LOGONLY == r->unit_->info_.unit_.replica_type_) {
          // nothing todo
          // If the replica of F is missing, it cannot be achieved by transing the replica on l_unit
        } else if (REPLICA_TYPE_FULL == r->replica_type_ && REPLICA_TYPE_FULL == r->unit_->info_.unit_.replica_type_) {
          // nothing todo
          // Cannot select the normal replica of F as the source for transform
        } else if (REPLICA_TYPE_LOGONLY == dest_replica_type && REPLICA_TYPE_FULL != r->replica_type_) {
          // nothing todo;
          // only supports F to L;
        } else if (REPLICA_TYPE_LOGONLY == r->replica_type_) {
          // nothing todo
          // L replica does not support transform
        } else if (OB_ISNULL(tmp_replica)) {
          tmp_replica = r;
        } else if (NULL != r->unit_ && r->unit_->in_pool_) {
          if (NULL == tmp_replica->unit_ || !tmp_replica->unit_->in_pool_) {
            tmp_replica = r;
          } else if (tmp_replica->data_version_ < r->data_version_) {
            tmp_replica = r;
          } else {
          }  // do nothing when tmp_replica in pool with a bigger version
        } else {
          // r->unit_ null or not in pool
          if (NULL != tmp_replica->unit_ && tmp_replica->unit_->in_pool_) {
            // do nothing when tmp_replica in pool
          } else if (tmp_replica->data_version_ < r->data_version_) {
            tmp_replica = r;
          } else {
          }  // do nothing when tmp_replica data version is smaller
        }
        if (NULL != r->unit_ && r->unit_->in_pool_ && REPLICA_TYPE_LOGONLY == r->unit_->info_.unit_.replica_type_ &&
            REPLICA_TYPE_LOGONLY == dest_replica_type) {
          // Find the replica on lunit and convert it to l
          tmp_replica = r;
          break;
        }
      }  // end if
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(tmp_replica)) {
        // Maybe the current machine is offline and the type transform cannot be performed.
        // Try again later
        ret = OB_EAGAIN;
        LOG_WARN("fail to choose replica for type transform, may in black list", KR(ret), K(partition), K(zone));
      } else {
        replica = *tmp_replica;
      }
    }
  }
  return ret;
}

// There is no rereplica operation in type transform
// But it will handle such a situation,
// if all units in the zone are filled with replicas of non-paxos type,
// but there is no replica of paxos type.
// At this time, type_transform task is needed for type transform to achieve
// the purpose of supplementing paxos replicas
int ObReplicaTypeTransformUtility::get_transform_task(const Partition& partition, bool& need_change, Replica& replica,
    ObReplicaType& dest_type, int64_t& dest_memstore_percent)
{
  int ret = OB_SUCCESS;
  need_change = false;
  if (OB_FAIL(filter_locality(partition))) {
    LOG_WARN("fail to filter locality", KR(ret));
  } else {
    FilterResult result;
    ObIArray<FilterResult>& results = get_filter_result();
    if (0 == results.count()) {
      // nothing todo
    } else if (OB_FAIL(check_paxos_replica(partition, results, result))) {
      LOG_WARN("fail to check paxos replica", KR(ret), K(results));
    } else if (result.is_valid()) {
      // get one task
    } else if (has_add_task(results)) {
      // nothing todo, wait for rereplicate
    } else if (OB_FAIL(get_one_type_transform_task(results, result))) {
      LOG_WARN("fail to get one type transform task", KR(ret), K(partition), K(result));
    } else if (result.is_valid()) {
      // get one task
    } else {
      // nothing todo
    }
    if (OB_SUCC(ret) && result.is_valid()) {
      // get one task
      need_change = true;
      replica = result.get_replica();
      dest_type = result.get_dest_type();
      dest_memstore_percent = result.get_dest_memstore_percent();
      LOG_INFO("get one type transform task", KR(ret), K(partition), K(replica), K(dest_type), K(results));
    }
  }
  return ret;
}

int ObReplicaTypeTransformUtility::get_one_type_transform_task(
    const ObIArray<FilterResult>& results, FilterResult& result)
{
  int ret = OB_SUCCESS;
  // transform to F has the highest priority;
  // next is transfrom to L;
  // and finally other types of transform.
  // According to our strategy, for changing from F to L/R or L to R,
  // there is no need to worry about affecting the number of paxos member groups
  const FilterResult* tmp_result = NULL;
  for (int64_t i = 0; i < results.count(); i++ && OB_SUCC(ret)) {
    if (!results.at(i).is_type_transform_task()) {
      // nothing todo
    } else if (OB_ISNULL(tmp_result)) {
      tmp_result = &results.at(i);
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(results.at(i).get_dest_type())) {
      if (REPLICA_TYPE_FULL != tmp_result->get_dest_type() && REPLICA_TYPE_FULL == results.at(i).get_dest_type()) {
        tmp_result = &results.at(i);
        break;
      } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(tmp_result->get_dest_type()) &&
                 ObReplicaTypeCheck::is_paxos_replica_V2(results.at(i).get_dest_type())) {
        tmp_result = &results.at(i);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tmp_result)) {
      // nothing todo
    } else {
      result = *tmp_result;
    }
  }
  return ret;
}

int ZoneReplicaDistTask::generate_paxos_replica_dist_task(
    const ObZoneReplicaAttrSet& zone_replica_attr_set, const common::ObIArray<rootserver::ObReplicaAddr>& exist_addr)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObZone>& zone_set = zone_replica_attr_set.zone_set_;
  common::ObSEArray<common::ObZone, 7> this_zone_set;
  common::ObSEArray<common::ObZone, 7> to_be_deleted_zone_set;
  share::ObReplicaAttrSet replica_task_set;
  if (ReplicaNature::PAXOS != replica_nature_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica nature unexpected", KR(ret), K(replica_nature_));
  } else if (zone_set.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_replica_attr_set));
  } else if (OB_FAIL(replica_task_set.set_paxos_replica_attr_array(
                 zone_replica_attr_set.replica_attr_set_.get_full_replica_attr_array(),
                 zone_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
    LOG_WARN("fail to set paxos replica attr", KR(ret));
  } else if (FALSE_IT(multi_zone_dist_ = (zone_set.count() > 1))) {
    // never be here
  } else if (zone_replica_attr_set.get_paxos_replica_num() <= 0) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exist_addr.count(); ++i) {
      const rootserver::ObReplicaAddr& replica_addr = exist_addr.at(i);
      bool target_replica = true;
      if (!ObReplicaTypeCheck::is_paxos_replica_V2(replica_addr.replica_type_)) {
        target_replica = false;  // not a paxos replica, pass
      } else if (!has_exist_in_array(zone_set, replica_addr.zone_)) {
        target_replica = false;  // The assigned copy does not belong to this zone
      } else if (REPLICA_TYPE_LOGONLY == replica_addr.replica_type_ && replica_task_set.get_logonly_replica_num() > 0) {
        if (OB_FAIL(replica_task_set.sub_logonly_replica_num(ReplicaAttr(1, 100)))) {
          LOG_WARN("fail to sub logonly replica", KR(ret));
        }
      } else if (REPLICA_TYPE_FULL == replica_addr.replica_type_ && replica_task_set.get_full_replica_num() > 0) {
        if (OB_FAIL(replica_task_set.sub_full_replica_num(ReplicaAttr(1, replica_addr.get_memstore_percent())))) {
          LOG_WARN("fail to sub full replica", KR(ret));
        }
      } else {
        target_replica = false;
      }

      if (OB_FAIL(ret)) {
        // failed
      } else if (!target_replica) {
        //
      } else if (!multi_zone_dist_ && replica_task_set.get_paxos_replica_num() > 0) {
        // In the case of single zone deployment, keep the zone,
        // because there may be multiple copies of paxos in the zone.
        //
        // In the case of multi-zone mixing, the tasks on the zone are directly deleted,
        // because the number of zones is equal to the number of paxos,
      } else if (OB_FAIL(to_be_deleted_zone_set.push_back(replica_addr.zone_))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_set.count(); ++i) {
        const common::ObZone& zone = zone_set.at(i);
        if (has_exist_in_array(to_be_deleted_zone_set, zone)) {
          // pass
        } else if (OB_FAIL(this_zone_set.push_back(zone))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(zone_set_.assign(this_zone_set))) {
        LOG_WARN("fail to assign zone set", KR(ret));
      } else if (OB_FAIL(replica_task_set_.assign(replica_task_set))) {
        LOG_WARN("fail to assign replica task set", KR(ret));
      }
    }
  }
  return ret;
}

int ZoneReplicaDistTask::generate_non_paxos_replica_dist_task(
    const ObZoneReplicaAttrSet& zone_replica_attr_set, const common::ObIArray<rootserver::ObReplicaAddr>& exist_addr)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObZone>& zone_set = zone_replica_attr_set.zone_set_;
  common::ObSEArray<common::ObZone, 7> this_zone_set;
  common::ObSEArray<common::ObZone, 7> to_be_deleted_zone_set;
  share::ObReplicaAttrSet replica_task_set;
  if (ReplicaNature::NON_PAXOS != replica_nature_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica nature unexpected", KR(ret), K(replica_nature_));
  } else if (zone_set.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_replica_attr_set));
  } else if (zone_set.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ZoneReplicaDistTask do not support multi zone readonly dist", KR(ret));
  } else if (OB_FAIL(replica_task_set.set_readonly_replica_attr_array(
                 zone_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
    LOG_WARN("fail to set readonly replica attr", KR(ret));
  } else if (FALSE_IT(multi_zone_dist_ = (zone_set.count() > 1))) {
    // never be here
  } else if (zone_replica_attr_set.get_readonly_replica_num() <= 0) {
    // There is no replica of paxos, just end
  } else if (zone_replica_attr_set.is_allserver_readonly_replica()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone replica dist task only provide service for specific readonly", KR(ret), K(zone_replica_attr_set));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exist_addr.count(); ++i) {
      const rootserver::ObReplicaAddr& replica_addr = exist_addr.at(i);
      bool target_replica = true;
      if (!ObReplicaTypeCheck::is_readonly_replica(replica_addr.replica_type_)) {
        target_replica = false;  // not a paxos replica, pass
      } else if (!has_exist_in_array(zone_set, replica_addr.zone_)) {
        target_replica = false;  // The assigned replica does not belong to this zone
      } else if (REPLICA_TYPE_READONLY == replica_addr.replica_type_ &&
                 replica_task_set.get_readonly_replica_num() > 0) {
        if (OB_FAIL(replica_task_set.sub_readonly_replica_num(ReplicaAttr(1, 100)))) {
          LOG_WARN("fail to sub logonly replica", KR(ret));
        }
      } else {
        target_replica = false;
      }

      if (OB_FAIL(ret)) {
        // failed
      } else if (!target_replica) {
        //
      } else if (replica_task_set.get_readonly_replica_num() > 0) {
      } else if (OB_FAIL(to_be_deleted_zone_set.push_back(replica_addr.zone_))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_set.count(); ++i) {
        const common::ObZone& zone = zone_set.at(i);
        if (has_exist_in_array(to_be_deleted_zone_set, zone)) {
          // pass
        } else if (OB_FAIL(this_zone_set.push_back(zone))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(zone_set_.assign(this_zone_set))) {
        LOG_WARN("fail to assign zone set", KR(ret));
      } else if (OB_FAIL(replica_task_set_.assign(replica_task_set))) {
        LOG_WARN("fail to assign replica task set", KR(ret));
      }
    }
  }
  return ret;
}

/*
 * The locality of the multi-zone mixed department,
 * the number of paxos must be the same as the length of the zone set
 * The locality deployed in a single zone allows up to two replicas of paxos to appear in a zone
 */
int ZoneReplicaDistTask::generate(const ReplicaNature replica_nature, const ObZoneReplicaAttrSet& zone_replica_attr_set,
    const common::ObIArray<rootserver::ObReplicaAddr>& exist_addr)
{
  int ret = OB_SUCCESS;
  if (replica_nature < ReplicaNature::PAXOS || replica_nature >= ReplicaNature::INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_nature));
  } else {
    replica_nature_ = replica_nature;
    if (replica_nature_ == ReplicaNature::PAXOS) {
      if (OB_FAIL(generate_paxos_replica_dist_task(zone_replica_attr_set, exist_addr))) {
        LOG_WARN("fail to generate", KR(ret));
      }
    } else if (replica_nature_ == ReplicaNature::NON_PAXOS) {
      if (OB_FAIL(generate_non_paxos_replica_dist_task(zone_replica_attr_set, exist_addr))) {
        LOG_WARN("fail to genrate", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica nature unexpected", KR(ret), K(replica_nature));
    }
  }
  return ret;
}

int ZoneReplicaDistTask::check_valid(bool& is_valid)
{
  int ret = OB_SUCCESS;
  if (ReplicaNature::NON_PAXOS == replica_nature_) {
    is_valid = true;
  } else if (multi_zone_dist_) {
    is_valid = (zone_set_.count() == replica_task_set_.get_paxos_replica_num());
  } else {
    is_valid = (zone_set_.count() <= replica_task_set_.get_paxos_replica_num());
  }
  return ret;
}

int ZoneReplicaDistTask::check_has_task(
    const common::ObZone& zone, const ObReplicaType replica_type, const int64_t memstore_percent, bool& has_this_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type) || memstore_percent < 0 ||
                  memstore_percent > 100)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone), K(replica_type), K(memstore_percent));
  } else if (!has_exist_in_array(zone_set_, zone)) {
    has_this_task = false;
  } else {
    has_this_task = false;
    if (REPLICA_TYPE_FULL == replica_type) {
      for (int64_t i = 0; !has_this_task && i < replica_task_set_.get_full_replica_attr_array().count(); ++i) {
        const ReplicaAttr& replica_attr = replica_task_set_.get_full_replica_attr_array().at(i);
        if (replica_attr.num_ > 0 && replica_attr.memstore_percent_ == memstore_percent) {
          has_this_task = true;
        }
      }
    } else if (REPLICA_TYPE_LOGONLY == replica_type) {
      // ignore memstore percent for logonly
      for (int64_t i = 0; !has_this_task && i < replica_task_set_.get_logonly_replica_attr_array().count(); ++i) {
        const ReplicaAttr& replica_attr = replica_task_set_.get_logonly_replica_attr_array().at(i);
        if (replica_attr.num_ > 0) {
          has_this_task = true;
        }
      }
    } else if (REPLICA_TYPE_READONLY == replica_type) {
      for (int64_t i = 0; !has_this_task && i < replica_task_set_.get_readonly_replica_attr_array().count(); ++i) {
        const ReplicaAttr& replica_attr = replica_task_set_.get_readonly_replica_attr_array().at(i);
        if (replica_attr.num_ > 0 && replica_attr.memstore_percent_ == memstore_percent) {
          has_this_task = true;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type unexpected", KR(ret), K(replica_type));
    }
  }
  return ret;
}

int ZoneReplicaDistTask::erase_task(
    const common::ObZone& zone, const int64_t memstore_percent, const ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone), K(replica_type));
  } else {
    bool has_this_task = false;
    if (OB_FAIL(check_has_task(zone, replica_type, memstore_percent, has_this_task))) {
      LOG_WARN("fail to check has task", KR(ret));
    } else if (!has_this_task) {
      // by pass
    } else if (REPLICA_TYPE_FULL == replica_type) {
      if (OB_FAIL(replica_task_set_.sub_full_replica_num(ReplicaAttr(1, memstore_percent)))) {
        LOG_WARN("fail to sub full replica", KR(ret));
      }
    } else if (REPLICA_TYPE_LOGONLY == replica_type) {
      if (OB_FAIL(replica_task_set_.sub_logonly_replica_num(ReplicaAttr(1, memstore_percent)))) {
        LOG_WARN("fail to sub logonly replica", KR(ret));
      }
    } else if (REPLICA_TYPE_READONLY == replica_type) {
      if (OB_FAIL(replica_task_set_.sub_readonly_replica_num(ReplicaAttr(1, memstore_percent)))) {
        LOG_WARN("fail to sub readonly replica", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type unexpected", KR(ret), K(replica_type));
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (ReplicaNature::NON_PAXOS == replica_nature_) {
    } else if (!multi_zone_dist_ && replica_task_set_.get_paxos_replica_num() > 0) {
      // In a non-mixed situation, there are replicas on the zone
    } else {  // delete zone
      common::ObArray<common::ObZone> new_zone_set;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_set_.count(); ++i) {
        const common::ObZone& this_zone = zone_set_.at(i);
        if (this_zone == zone) {
          // bypass, this zone will be deleted
        } else if (OB_FAIL(new_zone_set.push_back(this_zone))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(zone_set_.assign(new_zone_set))) {
        LOG_WARN("fail to assign zone set", KR(ret));
      }
    }
  }
  return ret;
}

int ZoneReplicaDistTask::check_empty(bool& is_empty)
{
  int ret = OB_SUCCESS;
  if (ReplicaNature::PAXOS == replica_nature_) {
    is_empty = replica_task_set_.get_paxos_replica_num() <= 0;
  } else if (ReplicaNature::NON_PAXOS == replica_nature_) {
    is_empty = replica_task_set_.get_readonly_replica_num() <= 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica nature unexpected", KR(ret));
  }
  return ret;
}

int ZoneReplicaDistTask::assign(const ZoneReplicaDistTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_set_.assign(that.zone_set_))) {
    LOG_WARN("fail to assign", KR(ret));
  } else if (OB_FAIL(replica_task_set_.assign(that.replica_task_set_))) {
    LOG_WARN("fail to assign", KR(ret));
  } else {
    replica_nature_ = that.replica_nature_;
    multi_zone_dist_ = that.multi_zone_dist_;
  }
  return ret;
}

int ZoneReplicaDistTask::tmp_compatible_generate(const ReplicaNature replica_nature, const common::ObZone& zone,
    const ObReplicaType replica_type, const int64_t memstore_percent)
{
  int ret = OB_SUCCESS;
  if (replica_nature < ReplicaNature::PAXOS || replica_nature >= ReplicaNature::INVALID || memstore_percent < 0 ||
      memstore_percent > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_nature), K(memstore_percent));
  } else {
    replica_nature_ = replica_nature;
    multi_zone_dist_ = false;
    if (OB_FAIL(zone_set_.push_back(zone))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (REPLICA_TYPE_FULL == replica_type) {
      if (replica_task_set_.add_full_replica_num(ReplicaAttr(1, memstore_percent))) {
        LOG_WARN("fail to add full replica num", KR(ret));
      }
    } else if (REPLICA_TYPE_LOGONLY == replica_type) {
      if (replica_task_set_.add_logonly_replica_num(ReplicaAttr(1, memstore_percent))) {
        LOG_WARN("fail to add full replica num", KR(ret));
      }
    } else if (REPLICA_TYPE_READONLY == replica_type) {
      if (replica_task_set_.add_readonly_replica_num(ReplicaAttr(1, memstore_percent))) {
        LOG_WARN("fail to add full replica num", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type unexpected", KR(ret), K(replica_type));
    }
  }
  return ret;
}
