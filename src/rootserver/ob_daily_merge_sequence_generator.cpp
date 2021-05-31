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
#include "ob_daily_merge_sequence_generator.h"
#include "ob_zone_manager.h"
#include "ob_server_manager.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/ob_define.h"
namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace share;
using namespace share::schema;

#define RESET_ENTRY_NOT_EXIST                                        \
  do {                                                               \
    if (OB_ENTRY_NOT_EXIST == ret) {                                 \
      ret = OB_SUCCESS;                                              \
      LOG_WARN("entry not exists, may be delete. treat as success"); \
    }                                                                \
  } while (0)

void ObDailyMergeSequenceGenerator::ObMergeUnitGenerator::init(
    ObZoneManager& zone_manager, ObServerManager& server_manager)
{
  zone_mgr_ = &zone_manager;
  server_manager_ = &server_manager;
  inited_ = true;
}

int ObDailyMergeSequenceGenerator::ObMergeUnitGenerator::build_merge_unit(ObMergeUnitArray& merge_units)
{
  return build_merge_unit_by_zone(merge_units);
}

int ObDailyMergeSequenceGenerator::ObMergeUnitGenerator::build_merge_unit_by_zone(ObMergeUnitArray& merge_units)
{
  int ret = OB_SUCCESS;
  merge_units.reset();
  ObSEArray<ObZone, DEFAULT_MERGE_UNIT_COUNT> zones;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("mrege unit generator not init", K(ret), K(inited_));
  } else if (OB_ISNULL(zone_mgr_) || OB_ISNULL(server_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid zone_mgr", K(ret), K(zone_mgr_), K(server_manager_));
  } else if (OB_FAIL(zone_mgr_->get_zone(zones))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    std::sort(zones.begin(), zones.end());
    ObMergeUnit merge_unit;
    HEAP_VAR(ObZoneInfo, zone_info)
    {
      FOREACH_CNT_X(zone, zones, OB_SUCC(ret))
      {
        if (OB_ISNULL(zone)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalie zone", K(ret), K(zone));
        } else if (FALSE_IT(zone_info.zone_ = *zone)) {
          // nothing to do
        } else if (OB_FAIL(zone_mgr_->get_zone(zone_info))) {
          RESET_ENTRY_NOT_EXIST;
          LOG_WARN("fail to get zone", K(ret), K(*zone));
        } else if (OB_FAIL(server_manager_->get_servers_of_zone(*zone, merge_unit.servers_))) {
          LOG_WARN("fail to get alive server", K(ret), K(*zone));
        } else if (OB_FAIL(zone_info.get_region(merge_unit.region_))) {
          LOG_WARN("fail to get region", K(ret));
        } else {
          merge_unit.type_ = static_cast<ObZoneType>(zone_info.zone_type_.value_);
          merge_unit.zone_ = *zone;
          merge_unit.leader_cnt_ = 0;
          if (OB_FAIL(merge_units.push_back(merge_unit))) {
            LOG_WARN("fail to push back merge unit", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
///////////////////////////////
///////////////////////////////
ObDailyMergeSequenceGenerator::ObDailyMergeSequenceGenerator()
    : zone_mgr_(NULL),
      server_manager_(NULL),
      pt_(NULL),
      schema_service_(NULL),
      full_partitions_distribution_(),
      merge_units_(),
      conflict_pairs_(),
      concurrency_count_(-1)
{}
ObDailyMergeSequenceGenerator::~ObDailyMergeSequenceGenerator()
{}

void ObDailyMergeSequenceGenerator::init(ObZoneManager& zone_manager, ObServerManager& server_manager,
    ObPartitionTableOperator& pt, ObMultiVersionSchemaService& schema_service)
{
  zone_mgr_ = &zone_manager;
  server_manager_ = &server_manager;
  pt_ = &pt;
  schema_service_ = &schema_service;
}

void ObDailyMergeSequenceGenerator::reset()
{
  full_partitions_distribution_.reset();
  readonly_partitions_distribution_.reset();
  merge_units_.reset();
  conflict_pairs_.reset();
}

int ObDailyMergeSequenceGenerator::get_merge_unit_index(const ObAddr& server, int64_t& index)
{
  int ret = OB_SUCCESS;
  index = -1;
  for (int64_t i = 0; i < merge_units_.count() && OB_SUCC(ret) && index == -1; i++) {
    FOREACH_CNT_X(addr, merge_units_.at(i).servers_, OB_SUCC(ret))
    {
      if (OB_ISNULL(addr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid addr", K(ret), K(addr));
      } else if (server == *addr) {
        index = i;
        break;
      }
    }
  }
  if (OB_UNLIKELY(index == -1)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find server in merge unit", K(ret), K(server), K(index));
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_merge_unit_index(const ObZone& zone, int64_t& index)
{
  int ret = OB_SUCCESS;
  index = -1;
  for (int64_t i = 0; i < merge_units_.count() && OB_SUCC(ret); i++) {
    if (merge_units_.at(i).zone_ == zone) {
      index = i;
      break;
    }
  }
  if (OB_UNLIKELY(index == -1)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find zone in merge unit", K(ret), K(zone), K(index));
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_next_zone(
    bool merge_by_turn, const int64_t concurrency_count, ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  to_merge.reset();
  concurrency_count_ = concurrency_count;
  ObSEArray<ObZone, DEFAULT_MERGE_UNIT_COUNT> all_zones;
  if (merge_by_turn) {
    if (OB_FAIL(get_next_zone_by_turn(to_merge))) {
      LOG_WARN("fail to get next zone", K(ret));
    }
  } else {
    if (OB_FAIL(get_next_zone_no_turn(to_merge))) {
      LOG_WARN("fail to get next zone", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(filter_merging_zone(to_merge))) {
      LOG_WARN("fail to filter in merging zone", K(ret));
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_next_zone_no_turn(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  to_merge.reset();
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (OB_FAIL(zone_mgr_->get_zone(to_merge))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    LOG_INFO("schedule daily merge no turn", K(to_merge));
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_next_zone_by_turn(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  to_merge.reset();
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (OB_FAIL(rebuild())) {
    LOG_WARN("fail to rebuild execute order", K(ret));
  }
  // start to merge zone which is alread in merging state
  // get next zone by conflict
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_in_merging_zone(to_merge))) {
      LOG_WARN("fail to add in merging zone", K(ret));
    } else if (OB_FAIL(get_next_zone_by_conflict_pair(to_merge))) {
      LOG_WARN("fail to get next zone by conflict pair", K(ret));
    }
  }
  if (OB_SUCC(ret) && to_merge.count() < concurrency_count_ && can_merge_concurrently_in_region()) {
    if (OB_FAIL(get_next_zone_by_concurrency_count(to_merge))) {
      LOG_WARN("fail to get next zone by priority", K(ret));
    } else {
      LOG_INFO("can merge concurrency in region", K(to_merge));
    }
  }
  return ret;
}

// 3 zone and one region, concurrency is 2
bool ObDailyMergeSequenceGenerator::can_merge_concurrently_in_region()
{
  bool bret = true;
  int ret = OB_SUCCESS;
  ObArray<ObZoneInfo> infos;
  ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH> region;
  ObZoneStatus::Status inactive_status = ObZoneStatus::INACTIVE;
  if (concurrency_count_ != 2) {
    bret = false;
  } else if (OB_ISNULL(zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(zone_mgr_));
  } else if (OB_FAIL(zone_mgr_->get_zone(infos))) {
    LOG_WARN("fail to get zone", K(ret));
  } else if (infos.count() != 3) {
    bret = false;
  } else {
    bool have = false;
    for (int64_t i = 0; i < infos.count() && OB_SUCC(ret); i++) {
      if (0 == i) {
        region = infos.at(i).region_.info_;
      } else if (region != infos.at(i).region_.info_) {
        bret = false;
        break;
      } else if (infos.at(i).status_ == inactive_status) {  // Be Simple, return false when zone stopped
        bret = false;
        break;
      } else if (OB_FAIL(server_manager_->have_server_stopped(infos.at(i).zone_, have))) {
        LOG_WARN("fail to check server stopped", K(ret), "zone", infos.at(i).zone_);
      } else if (have) {
        bret = false;
        break;
      }
    }  // end for
  }    // end else
  if (OB_FAIL(ret)) {
    bret = false;
  }
  // just for debug, will remove later.
  LOG_INFO("check can merge concurrency", K(bret), K_(concurrency_count), K(infos));
  return bret;
}

int ObDailyMergeSequenceGenerator::get_next_zone_by_conflict_pair(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  bool can_start = false;
  bool can_switch = false;
  ObMergePriorityCmp compare(zone_mgr_);
  std::sort(conflict_pairs_.begin(), conflict_pairs_.end(), compare);
  ObZone last_zone;
  for (int64_t i = 0; i < conflict_pairs_.count() && OB_SUCC(ret); i++) {
    can_start = false;
    can_switch = false;
    int64_t merge_unit_index = OB_INVALID_INDEX;
    if (to_merge.count() >= concurrency_count_ && concurrency_count_ != 0) {
      break;
    } else if (last_zone == conflict_pairs_.at(i).first) {
      // nothing todo
    } else if (has_exist_in_array(to_merge, conflict_pairs_.at(i).first) ||
               !is_need_merge(conflict_pairs_.at(i).first)) {
      // nothing todo
    } else if (OB_FAIL(can_start_merge(to_merge, conflict_pairs_.at(i).first, can_start))) {
      LOG_WARN("fail to check zone can start merge", K(ret), "zone index", conflict_pairs_.at(i).first);
    } else if (!can_start) {
      // nothing todo
    } else if (OB_FAIL(get_merge_unit_index(conflict_pairs_.at(i).first, merge_unit_index))) {
      LOG_WARN("fail to get merge unit index", K(ret));
    } else {
      last_zone = conflict_pairs_.at(i).first;
      bool can_start_merge = true;
      for (int64_t j = i; j < conflict_pairs_.count() && OB_SUCC(ret); j++) {
        if (conflict_pairs_.at(j).first != last_zone) {
          // nothing todo
          break;
        } else if (OB_FAIL(can_switch_to_leader(to_merge, conflict_pairs_.at(j).second, can_switch))) {
          LOG_WARN("fail to check zone can swtich to be leader", K(ret), "zone index", conflict_pairs_.at(j).second);
        } else if (!can_switch) {
          can_start_merge = false;
          // break;
        } else if (merge_units_.at(merge_unit_index).replica_cnt_ == conflict_pairs_.at(j).same_partition_cnt_) {
          can_start_merge = true;  // ex. 3 zone in 1 region, we can switch to next zone if on zone stopped
          break;
        }
      }  // end for
      if (OB_SUCC(ret) && can_start_merge) {
        if (OB_FAIL(to_merge.push_back(conflict_pairs_.at(i).first))) {
          LOG_WARN("fail to push bask zone", K(ret), "zone", conflict_pairs_.at(i).first);
        } else {
          LOG_INFO("zone is ready to merge by conflict pairs",
              "zone",
              conflict_pairs_.at(i).first,
              K(concurrency_count_),
              "to_merge_count",
              to_merge.count());
        }
      }
    }
  }  // end for
  return ret;
}

int ObDailyMergeSequenceGenerator::add_in_merging_zone(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  ObArray<ObZoneInfo> all_zone;
  int64_t global_broadcast_version = 0;
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (OB_FAIL(zone_mgr_->get_zone(all_zone))) {
    LOG_WARN("fail to get zone", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_zone.count(); ++i) {
    if (all_zone.at(i).is_in_merge()) {
      if (OB_FAIL(to_merge.push_back(all_zone.at(i).zone_))) {
        LOG_WARN("fail to push back", K(ret), "zone", all_zone.at(i).zone_);
      } else {
        LOG_INFO("zone is already in merging", "zone", all_zone.at(i).zone_, "count", to_merge.count());
      }
    } else {
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_next_zone_by_concurrency_count(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < conflict_pairs_.count() && OB_SUCC(ret); i++) {
    if (to_merge.count() >= concurrency_count_) {
      break;
    } else if (has_exist_in_array(to_merge, conflict_pairs_.at(i).first) ||
               !is_need_merge(conflict_pairs_.at(i).first)) {
      // nothing todo
    } else if (OB_FAIL(to_merge.push_back(conflict_pairs_.at(i).first))) {
      LOG_WARN("fail to push back", K(ret), "zone", conflict_pairs_.at(i).first);
    } else {
      LOG_INFO("zone is ready to merge, by concurrency count",
          "zone",
          conflict_pairs_.at(i).first,
          K(concurrency_count_),
          "to_merge_count",
          to_merge.count());
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_next_zone_by_priority(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  ObConflictPairPriorityCmp cmp(*this);
  std::sort(conflict_pairs_.begin(), conflict_pairs_.end(), cmp);
  for (int64_t i = 0; i < conflict_pairs_.count() && OB_SUCC(ret); i++) {
    if (to_merge.count() >= concurrency_count_) {
      break;
    } else if (has_exist_in_array(to_merge, conflict_pairs_.at(i).first) ||
               !is_need_merge(conflict_pairs_.at(i).first)) {
      // nothing todo
    } else if (OB_FAIL(to_merge.push_back(conflict_pairs_.at(i).first))) {
      LOG_WARN("fail to push back", K(ret), "zone", conflict_pairs_.at(i).first);
    } else {
      LOG_INFO("zone is ready to merge, by leader count",
          "zone",
          conflict_pairs_.at(i).first,
          K(concurrency_count_),
          "to_merge_count",
          to_merge.count());
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::can_switch_to_leader(
    const ObIArray<ObZone>& to_merge, const ObZone& zone, bool& can_switch)
{
  int ret = OB_SUCCESS;
  can_switch = true;
  HEAP_VAR(ObZoneInfo, info)
  {
    info.zone_ = zone;
    ObZone empty_zone;
    if (OB_ISNULL(zone_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
    } else if (zone == empty_zone) {
      can_switch = true;
    } else if (OB_FAIL(zone_mgr_->get_zone(info))) {
      LOG_WARN("fail to get zone", K(ret), K(zone));
      RESET_ENTRY_NOT_EXIST;
      can_switch = false;
    } else if (!info.can_switch_to_leader_while_daily_merge()) {
      can_switch = false;
    } else {
      can_switch = true;
      for (int64_t i = 0; i < to_merge.count(); i++) {
        if (zone == to_merge.at(i)) {
          can_switch = false;
        }
      }
    }
    LOG_DEBUG("check can swich to leader", K(ret), K(zone), K(can_switch));
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::can_start_merge(
    const ObIArray<ObZone>& to_merge, const ObZone& zone, bool& can_start)
{
  int ret = OB_SUCCESS;
  can_start = true;
  ObZone empty_zone;
  HEAP_VAR(ObZoneInfo, info)
  {
    info.zone_ = zone;
    if (OB_ISNULL(zone_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_zone(info))) {
      LOG_WARN("fail to get zone", K(ret), K(zone));
      can_start = false;
      RESET_ENTRY_NOT_EXIST;
    } else {
      for (int64_t i = 0; i < conflict_pairs_.count() && OB_SUCC(ret) && can_start; i++) {
        if (zone == conflict_pairs_.at(i).second && empty_zone != conflict_pairs_.at(i).second) {
          info.zone_ = conflict_pairs_.at(i).first;
          if (has_exist_in_array(
                  to_merge, info.zone_)) {  // all zone in merging state or to be in merging are in to_merge
            can_start = false;
          }
        }
      }
    }
    LOG_DEBUG("check can start merge", K(ret), K(zone), K(can_start));
  }
  return ret;
}

bool ObDailyMergeSequenceGenerator::is_need_merge(const ObZone& zone)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, info)
  {
    info.zone_ = zone;
    int64_t global_broadcast_version = 0;
    if (OB_ISNULL(zone_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid zone mgr", K(ret), K(zone_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_zone(info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find zone failed", K(ret), K(zone));
      } else {
        ret = OB_SUCCESS;
        bret = false;
        LOG_DEBUG("ignore not exist zones");
      }
    } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
      LOG_WARN("get_global_broadcast_version failed", K(ret));
    } else {
      bret = info.need_merge(global_broadcast_version);
    }
    LOG_DEBUG("check is need merge", K(bret), K(zone));
  }
  return bret;
}

int ObDailyMergeSequenceGenerator::rebuild()
{
  int ret = OB_SUCCESS;
  reset();
  ObMergeUnitGenerator generator;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(zone_mgr_) || OB_ISNULL(server_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid parameters", K(ret), K(zone_mgr_), K(server_manager_));
  } else if (FALSE_IT(generator.init(*zone_mgr_, *server_manager_))) {
    LOG_WARN("fail to init merge unit generator", K(ret), K(zone_mgr_), K_(server_manager));
  } else if (OB_FAIL(generator.build_merge_unit(merge_units_))) {
    LOG_WARN("fail to build merge unit", K(ret));
  } else if (OB_FAIL(generate_conflict_pairs())) {
    LOG_WARN("fail to get conflict pair", K(ret));
  }
  int64_t process_time = ObTimeUtility::current_time() - start_time;
  LOG_INFO("daily merge sequence generator rebuild finish", K(process_time));
  return ret;
}

int ObDailyMergeSequenceGenerator::generate_conflict_pairs()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_distributions())) {
    LOG_WARN("fail to get replica distribute", K(ret));
  }
  for (int64_t i = 0; i < merge_units_.count() && OB_SUCC(ret); i++) {
    if (ZONE_TYPE_READWRITE == merge_units_.at(i).type_) {
      if (OB_FAIL(generate_conflict_pair(i, full_partitions_distribution_))) {
        LOG_WARN("fail to generate conflict pair", K(ret), "merge unit", merge_units_.at(i).zone_);
      }
    } else if (ZONE_TYPE_READONLY == merge_units_.at(i).type_) {
      if (OB_FAIL(generate_conflict_pair(i, readonly_partitions_distribution_))) {
        LOG_WARN("fail to generate conflict pair", K(ret), "merge unit", merge_units_.at(i).zone_);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone type", K(ret), "type", merge_units_.at(i).type_);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ensure_all_zone_exist())) {
      LOG_WARN("fail to arrange conflict pairs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < conflict_pairs_.count() && OB_SUCC(ret); i++) {
      LOG_INFO("add conflict pair",
          "first",
          conflict_pairs_.at(i).first,
          "second",
          conflict_pairs_.at(i).second,
          "same_count",
          conflict_pairs_.at(i).same_partition_cnt_);
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::ensure_all_zone_exist()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObZone, DEFAULT_MERGE_UNIT_COUNT> all_zones;
  ObZone empty_zone;
  if (OB_FAIL(zone_mgr_->get_zone(all_zones))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    for (int64_t i = 0; i < all_zones.count() && OB_SUCC(ret); i++) {
      if (exist_in_conflict_pair(all_zones.at(i))) {
        // nothing todo
      } else if (OB_FAIL(add_conflict_pair(all_zones.at(i), empty_zone))) {
        LOG_WARN("fail to add conflict pair", K(ret), K(all_zones.at(i)));
      }
    }
  }

  return ret;
}

bool ObDailyMergeSequenceGenerator::exist_in_conflict_pair(const ObZone& zone)
{
  bool bret = false;
  for (int64_t i = 0; i < conflict_pairs_.count(); i++) {
    if (conflict_pairs_.at(i).first == zone) {
      bret = true;
    }
  }
  return bret;
}

int ObDailyMergeSequenceGenerator::calc_distributions()
{
  int ret = OB_SUCCESS;
  full_partitions_distribution_.reset();
  readonly_partitions_distribution_.reset();
  ObPartitionTableIterator iter;
  bool ignore_row_checksum = true;
  if (OB_ISNULL(pt_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid member", K(ret), K(pt_), K(schema_service_));
  } else if (OB_FAIL(iter.init(*pt_, *schema_service_, ignore_row_checksum))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_filter_permanent_offline(*server_manager_))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_only_user_table())) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("set filter failed", K(ret));
  } else {
    ObPartitionInfo partition;
    ObPartitionDistribution full_distribution;
    ObPartitionDistribution readonly_distribution;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterator partition table failed", K(ret));
        }
        break;
      } else if (OB_FAIL(calc_partition_distribution(
                     partition, ZONE_TYPE_READWRITE, REPLICA_TYPE_FULL, full_distribution))) {
        LOG_WARN("fail to calc partition distribution", K(ret), K(partition));
      } else if (OB_FAIL(full_partitions_distribution_.push_back(full_distribution))) {
        LOG_WARN("fail to push back", K(ret), K(full_distribution));
      } else if (OB_FAIL(calc_partition_distribution(
                     partition, ZONE_TYPE_READONLY, REPLICA_TYPE_READONLY, readonly_distribution))) {
        LOG_WARN("fail to calc partition distribution", K(ret), K(partition));
      } else if (OB_FAIL(readonly_partitions_distribution_.push_back(readonly_distribution))) {
        LOG_WARN("fail to push back", K(ret), K(readonly_distribution));
      }
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::calc_partition_distribution(const ObPartitionInfo& partition_info,
    const ObZoneType& zone_type, const ObReplicaType& type, ObPartitionDistribution& distribution)
{
  int ret = OB_SUCCESS;
  distribution.reset();
  if (OB_UNLIKELY(!partition_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid partition", K(ret), K(partition_info));
  } else {
    int64_t index = -1;
    FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), OB_SUCC(ret))
    {
      index = -1;
      if (OB_ISNULL(replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", K(ret), K(replica));
      } else if (type != replica->replica_type_) {
        // nothing todo
      } else if (OB_FAIL(get_merge_unit_index(replica->server_, index))) {
        LOG_WARN("fail to add address", K(ret), K(*replica));
        RESET_ENTRY_NOT_EXIST;
      } else if (merge_units_.at(index).type_ != zone_type) {
        // nothing todo
      } else {
        if (OB_FAIL(distribution.add_member(index))) {
          LOG_WARN("fail to add memeber", K(ret), K(index));
        } else if (replica->is_leader_by_election()) {
          merge_units_.at(index).leader_cnt_++;
        }
        if (OB_SUCC(ret) && type == REPLICA_TYPE_FULL) {
          merge_units_.at(index).replica_cnt_++;
        }
      }
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::generate_conflict_pair(
    const int64_t merge_unit_index, const ObPartitionsDistribution& partition_distribution)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_unit_index < 0 || merge_unit_index > merge_units_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(merge_unit_index), "array size", merge_units_.count());
  } else {
    for (int64_t i = 0; i < partition_distribution.count() && OB_SUCC(ret); i++) {
      if (!partition_distribution.at(i).has_member(merge_unit_index)) {
        // nothing to do
      } else {
        for (int64_t j = 0; j < merge_units_.count() && OB_SUCC(ret); j++) {
          if (j == merge_unit_index) {
            // nothing todo
          } else if (!partition_distribution.at(i).has_member(j)) {
            // nothing todo
          } else if (merge_units_.at(merge_unit_index).region_ != merge_units_.at(j).region_) {
            // nothing todo
          } else if (merge_units_.at(merge_unit_index).type_ != merge_units_.at(j).type_) {
            // nothing todo
          } else if (OB_FAIL(add_conflict_pair(merge_units_.at(merge_unit_index).zone_, merge_units_.at(j).zone_))) {
            LOG_WARN("fail to add conflict pair", K(ret), K(merge_unit_index), K(j));
          }
        }
      }
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::filter_merging_zone(ObIArray<ObZone>& to_merge)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, info)
  {
    int64_t global_broadcast_version = 0;
    if (OB_ISNULL(zone_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
      LOG_WARN("get_global_broadcast_version failed", K(ret));
    }
    for (int64_t i = 0; i < to_merge.count() && OB_SUCC(ret); i++) {
      info.zone_ = to_merge.at(i);
      if (OB_FAIL(zone_mgr_->get_zone(info))) {
        LOG_WARN("fail to get zone", K(ret), "zone", to_merge.at(i));
        RESET_ENTRY_NOT_EXIST;
      } else if (info.broadcast_version_ == global_broadcast_version) {
        if (OB_FAIL(to_merge.remove(i))) {
          LOG_WARN("fail to remove to merge zone", K(ret), "zone", to_merge.at(i));
        } else {
          i--;
        }
      } else {
      }
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::add_conflict_pair(const ObZone& first, const ObZone& second)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  for (int64_t i = 0; i < conflict_pairs_.count() && OB_SUCC(ret); i++) {
    if (conflict_pairs_.at(i).first == first && conflict_pairs_.at(i).second == second) {
      conflict_pairs_.at(i).same_partition_cnt_++;
      exist = true;
      break;
    }
  }
  if (OB_SUCC(ret) && !exist) {
    ObConflictPair pair;
    ObZone empty;
    pair.first = first;
    pair.second = second;
    if (pair.second == empty) {
      pair.same_partition_cnt_ = 0;
    } else {
      pair.same_partition_cnt_ = 1;
    }
    if (OB_FAIL(conflict_pairs_.push_back(pair))) {
      LOG_WARN("fail to push back pair", K(ret), K(first), K(second));
    } else {
      LOG_DEBUG("add conflict pair", K(ret), K(first), K(second), "same_count", pair.same_partition_cnt_);
    }
  }
  return ret;
}

int ObDailyMergeSequenceGenerator::get_leader_count(const ObZone& zone, int64_t& leader_count)
{
  int ret = OB_SUCCESS;
  leader_count = 0;
  for (int64_t i = 0; i < merge_units_.count(); i++) {
    if (merge_units_.at(i).zone_ == zone) {
      leader_count = merge_units_.at(i).leader_cnt_;
      break;
    }
  }
  return ret;
}

bool ObDailyMergeSequenceGenerator::ObConflictPairPriorityCmp::operator()(
    const ObConflictPair& pair, const ObConflictPair& pair_other)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  int64_t leader_count = 0;
  int64_t other_leader_count = 0;
  if (OB_FAIL(generator_.get_leader_count(pair.first, leader_count))) {
    LOG_WARN("fail to get leader count", K(ret), K(pair));
  } else if (OB_FAIL(generator_.get_leader_count(pair_other.first, other_leader_count))) {
    LOG_WARN("fail to get leader count", K(ret), K(pair_other));
  } else if (leader_count < other_leader_count) {
    bret = true;
  } else if (leader_count > other_leader_count) {
    bret = false;
  } else {
    bret = pair.first < pair_other.first;
  }
  return bret;
}

bool ObDailyMergeSequenceGenerator::ObMergePriorityCmp::operator()(
    const ObConflictPair& first, const ObConflictPair& second)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, first_zone)
  {
    HEAP_VAR(ObZoneInfo, second_zone)
    {
      first_zone.zone_ = first.first;
      second_zone.zone_ = second.first;
      if (OB_ISNULL(zone_mgr_)) {
        LOG_WARN("get invalid zone_mgr", K(zone_mgr_));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(zone_mgr_->get_zone(first_zone))) {
        RESET_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(zone_mgr_->get_zone(second_zone))) {
        RESET_ENTRY_NOT_EXIST;
      }
      if (OB_SUCC(ret)) {
        int64_t first_merge_time = first_zone.last_merged_time_ - first_zone.merge_start_time_;
        int64_t second_merge_time = second_zone.last_merged_time_ - second_zone.merge_start_time_;
        if (first_zone.start_merge_fail_times_ > second_zone.start_merge_fail_times_) {
          bret = false;
        } else if (first_zone.start_merge_fail_times_ < second_zone.start_merge_fail_times_) {
          bret = true;
        } else if (first_merge_time > second_merge_time) {
          bret = true;
        } else if (first_merge_time < second_merge_time) {
          bret = false;
        } else {
          bret = (first.first < second.first);
        }
      }
    }
  }
  return bret;
}
}  // namespace rootserver
}  // namespace oceanbase
