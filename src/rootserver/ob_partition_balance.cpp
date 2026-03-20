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

#define USING_LOG_PREFIX BALANCE
#include "share/tablet/ob_tablet_table_iterator.h"
#include "rootserver/ob_partition_balance.h"
#include "rootserver/ob_tenant_balance_service.h" // ObTenantBalanceService
#include "rootserver/ob_balance_ls_primary_zone.h" // ObBalanceLSPrimaryZone
#include "observer/omt/ob_tenant_config_mgr.h" // ObTenantConfigGuard

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::share;
using namespace oceanbase::storage;

#define CHECK_NOT_NULL(ptr)                              \
do {                                                     \
  if (OB_SUCC(ret) && OB_ISNULL(ptr)) {                  \
    ret = OB_ERR_UNEXPECTED;                             \
    LOG_WARN("unexpeced null ptr", KR(ret), KP(ptr));    \
  }                                                      \
} while (0)

const int64_t PART_GROUP_SIZE_SEGMENT[] = {
  10 * 1024L * 1024L * 1024L, // 10G
  5 * 1024L * 1024L * 1024L,  // 5G
  2 * 1024L * 1024L * 1024L,  // 2G
  1 * 1024L * 1024L * 1024L,  // 1G
  500 * 1024L * 1024L,        // 500M
  200 * 1024L * 1024L,        // 200M
  100 * 1024L * 1024L         // 100M
};

int ObPartitionBalance::ZoneBGItem::init(
    const ObBalanceGroup &bg,
    const int64_t data_disk,
    const int64_t weight)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bg.is_valid() || data_disk < 0 || weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg), K(data_disk), K(weight));
  } else {
    bg_ = bg;
    data_disk_ = data_disk;
    weight_ = weight;
  }
  return ret;
}

template <>
int64_t ObPartitionBalance::ZoneBGItem::get_metric<ObPartitionBalance::ZoneBGItem::WEIGHT>() const
{
  return weight_;
}

template <>
int64_t ObPartitionBalance::ZoneBGItem::get_metric<ObPartitionBalance::ZoneBGItem::DISK>() const
{
  return data_disk_;
}

int ObPartitionBalance::ZoneBGStat::init(const ObZone &zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone));
  } else {
    zone_ = zone;
  }
  return ret;
}

int ObPartitionBalance::ZoneBGStat::add_bg_item(const ZoneBGItem &bg_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bg_item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_item));
  } else if (OB_FAIL(bg_items_.push_back(bg_item))) {
    LOG_WARN("push back bg item failed", KR(ret), K(bg_item));
  }
  return ret;
}

int ObPartitionBalance::ZoneBGStat::remove_last_bg_item(ZoneBGItem &last_bg_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bg_items_.count() <= 0)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("unexpected empty bg_items_", KR(ret), KPC(this));
  } else {
    last_bg_item = bg_items_.at(bg_items_.count() - 1);
    bg_items_.pop_back();
  }
  return ret;
}

template <ObPartitionBalance::ZoneBGItem::MetricType metric_type>
int64_t ObPartitionBalance::ZoneBGStat::get_total() const
{
  int64_t total = 0;
  ARRAY_FOREACH_NORET(bg_items_, i) {
    total += bg_items_.at(i).get_metric<metric_type>();
  }
  return total;
}

int ObPartitionBalance::init(
    uint64_t tenant_id,
    schema::ObMultiVersionSchemaService *schema_service,
    common::ObMySQLProxy *sql_proxy,
    TaskMode task_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionBalance has inited", KR(ret), K(this));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)
      || (task_mode != GEN_BG_STAT && task_mode != GEN_TRANSFER_TASK)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObPartitionBalance run", KR(ret), K(tenant_id), K(schema_service), K(sql_proxy), K(task_mode));
  } else if (OB_FAIL(bg_builder_.init(tenant_id, "PB_BGBuilder", *this, *sql_proxy, *schema_service))) {
    LOG_WARN("init all balance group builder fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(bg_map_.create(40960, lib::ObLabel("PB_BGMap"), lib::ObLabel("PB_BGMapNode"), tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(weighted_bg_map_.create(128, lib::ObLabel("PB_WBGMap"), lib::ObLabel("PB_WBGMapNode"), tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(scope_zone_bg_stat_map_.create(
      1024, lib::ObLabel("PB_SZBGStat"), lib::ObLabel("PB_SZBGStNd"), tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(ls_desc_map_.create(10, lib::ObLabel("PB_LSDescMap"), lib::ObLabel("PB_LSDMapNode"), tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(bg_ls_stat_operator_.init(sql_proxy))) {
    LOG_WARN("init balance group ls stat operator fail", KR(ret));
  } else if (OB_FAIL(job_generator_.init(tenant_id, sql_proxy))) {
    LOG_WARN("init job generator failed", KR(ret), K(tenant_id), KP(sql_proxy));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    allocator_.reset();
    ls_desc_array_.reset();
    task_mode_ = task_mode;
    inited_ = true;
  }
  return ret;
}

void ObPartitionBalance::destroy()
{
  FOREACH(iter, ls_desc_map_) {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObLSDesc();
    }
  }
  FOREACH(iter, weighted_bg_map_) {
    for (int64_t i = 0; i < iter->second.count(); i++) {
      if (OB_NOT_NULL(iter->second.at(i))) {
        iter->second.at(i)->~ObBalanceGroupInfo();
      }
    }
    iter->second.destroy();
  }
  FOREACH(iter, bg_map_) {
    for (int64_t i = 0; i < iter->second.count(); i++) {
      if (OB_NOT_NULL(iter->second.at(i))) {
        iter->second.at(i)->~ObBalanceGroupInfo();
      }
    }
    iter->second.destroy();
  }
  //reset
  job_generator_.reset();
  bg_builder_.destroy();
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.destroy();
  scope_zone_bg_stat_map_.destroy();
  weighted_bg_map_.destroy();
  bg_map_.destroy();
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dup_ls_id_.reset();
  sql_proxy_ = NULL;
  task_mode_ = GEN_BG_STAT;
}

int ObPartitionBalance::process(const ObBalanceJobID &job_id, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_PARTITION);
  ObBalanceStrategy balance_strategy;
  bool is_ls_primary_zone_balanced = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_FAIL(ObBalanceLSPrimaryZone::check_user_ls_primary_zone_balanced(
      tenant_id_, is_ls_primary_zone_balanced))) {
    LOG_WARN("failed to check user ls primary zone balanced", KR(ret), K(tenant_id_));
  } else if (!is_ls_primary_zone_balanced) {
    // Partition balance is only performed after user LS primary zone balancing is complete.
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    FLOG_INFO("[PART_BALANCE] user ls primary zone not balanced, can not do partition balance now, "
      "need to wait for primary zone balance", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(prepare_balance_group_())) {
    LOG_WARN("prepare_balance_group fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(save_balance_group_stat_())) {
    LOG_WARN("save balance group stat fail", KR(ret), K(tenant_id_));
  } else if (GEN_BG_STAT == task_mode_) {
    // finish
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_ATTR_ALIGN;
  } else if (OB_FAIL(process_balance_zone_tablegroup_weight_())) {
    LOG_WARN("process balance zone tablegroup weight failed", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTER_ZONE_WEIGHT;
  } else if (OB_FAIL(process_balance_zone_tablegroup_count_())) {
    LOG_WARN("process balance zone tablegroup count failed", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTER_ZONE;
  } else if (OB_FAIL(process_balance_zone_tablegroup_disk_())) {
    LOG_WARN("process balance zone tablegroup disk failed", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTER_ZONE_DISK;
  } else if (OB_FAIL(process_weight_balance_intragroup_())) {
    LOG_WARN("process weight balance intragroup failed", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTRA_GROUP_WEIGHT;
  } else if (bg_map_.empty()) {
    LOG_INFO("PART_BALANCE balance group is empty do nothing", K(tenant_id_));
  } else if (OB_FAIL(process_balance_partition_inner_())) {
    LOG_WARN("process_balance_partition_inner fail", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTRA_GROUP;
  } else if (OB_FAIL(process_balance_partition_extend_())) {
    LOG_WARN("process_balance_partition_extend fail", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTER_GROUP;
  } else if (OB_FAIL(process_balance_partition_disk_())) {
    LOG_WARN("process_balance_partition_disk fail", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_PART_DISK;
  }

  if (OB_FAIL(ret) || !balance_strategy.is_valid()) {
    // skip
  } else if (OB_UNLIKELY(!balance_strategy.is_partition_balance_strategy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected balance strategy", KR(ret), K(balance_strategy));
  } else {
    // compatible scenario: observer is new but data_version has not been pushed up
    bool is_supported = false;
    if (OB_FAIL(ObBalanceStrategy::check_compat_version(tenant_id_, is_supported))) {
      LOG_WARN("check compat version failed", KR(ret), K(tenant_id_), K(is_supported));
    } else if (!is_supported) {
      balance_strategy = ObBalanceStrategy::PB_COMPAT_OLD;
    }
    if (FAILEDx(job_generator_.gen_balance_job_and_tasks(job_type, balance_strategy, job_id, timeout))) {
      LOG_WARN("gen balance job and tasks failed", KR(ret),
          K(tenant_id_), K(job_type), K(balance_strategy), K(job_id), K(timeout));
    }
  }
  DEBUG_SYNC(BEFORE_PARTITION_BALANCE_END);
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("PART_BALANCE process", KR(ret), K(tenant_id_), K(task_mode_), K(job_id), K(timeout), K(balance_strategy),
      "cost", end_time - start_time, "need balance", job_generator_.need_gen_job(), K(is_ls_primary_zone_balanced));
  return ret;
}

int ObPartitionBalance::prepare_ls_()
{
  int ret = OB_SUCCESS;
  ObLSStatusInfoArray ls_stat_array;
  if (!inited_ || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(inited_), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantBalanceService::gather_ls_status_stat(tenant_id_, ls_stat_array, true))) {
    LOG_WARN("failed to gather ls status", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(job_generator_.prepare_ls(ls_stat_array))) {
    LOG_WARN("job_generator_ prepare ls failed", KR(ret), K(tenant_id_), K(ls_stat_array));
  } else {
    ARRAY_FOREACH(ls_stat_array, idx) {
      const ObLSStatusInfo &ls_stat = ls_stat_array.at(idx);
      ObLSDesc *ls_desc = nullptr;
      if (ls_stat.get_ls_id().is_sys_ls()) {
        // only skip sys ls
      } else if (!ls_stat.is_normal()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("ls is not in normal status", KR(ret), K(ls_stat));
      } else if (OB_ISNULL(ls_desc = reinterpret_cast<ObLSDesc*>(allocator_.alloc(sizeof(ObLSDesc))))) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("alloc mem fail", KR(ret));
      } else if (FALSE_IT(new(ls_desc) ObLSDesc(ls_stat.get_ls_id(), ls_stat.get_ls_group_id(), ls_stat.primary_zone_))) {
      } else if (OB_FAIL(ls_desc_map_.set_refactored(ls_desc->get_ls_id(), ls_desc))) { // need dup ls desc to gen job
        LOG_WARN("init ls_desc to map fail", KR(ret), K(ls_desc->get_ls_id()));
      } else if (ls_stat.is_duplicate_ls()) {
        if (OB_UNLIKELY(dup_ls_id_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("there should be only one dup ls when doing partition balance",
              KR(ret), K(dup_ls_id_), K(ls_stat), K(idx));
        } else {
          dup_ls_id_ = ls_stat.get_ls_id();
        }
        // ls_desc_array_ is used to balance partition groups on all user ls.
        // Paritions on dup ls can not be balanced together.
      } else if (OB_FAIL(ls_desc_array_.push_back(ls_desc))) {
        LOG_WARN("push_back ls_desc to array fail", KR(ret), K(ls_desc->get_ls_id()));
      }
    }
    if (OB_SUCC(ret) && ls_desc_array_.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no ls can assign", KR(ret), K(tenant_id_));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[PART_BALANCE] prepare_ls", K(tenant_id_), K(dup_ls_id_),
        K(ls_desc_array_), "ls_desc_map_ size", ls_desc_map_.size(), K(ls_stat_array));
  }
  return ret;
}

int ObPartitionBalance::prepare_balance_group_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  }
  // Here must prepare balance group data first, to get schema info for this build
  else if (OB_FAIL(bg_builder_.prepare(GEN_TRANSFER_TASK == task_mode_))) {
    LOG_WARN("balance group builder prepare fail", KR(ret));
  }
  // Then prepare LS info after schema info prepared
  else if (OB_FAIL(prepare_ls_())) {
    LOG_WARN("prepare_ls fail", KR(ret), K(tenant_id_));
  }
  // At last, build balance group info
  // During this build, on_new_partition() will be called
  else if (OB_FAIL(bg_builder_.build())) {
    LOG_WARN("balance group build fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(split_out_weighted_bg_map_())) {
    LOG_WARN("split out weighted bg map failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(prepare_ls_desc_())) {
    LOG_WARN("prepare ls desc failed", KR(ret), K(tenant_id_));
  }
  return ret;
}

// Prepare LS descriptor information for balance groups by scope.
// - If `scope` is MAX, stat all balance groups.
// - If not, only stat the balance groups with the matching scope.
int ObPartitionBalance::prepare_ls_desc_(const ObBalanceGroup::Scope scope)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bg_map_.empty())) {
    // do nothing
  } else {
    // reset all ls desc first
    ARRAY_FOREACH(ls_desc_array_, idx) {
      ObLSDesc *ls_desc = ls_desc_array_.at(idx);
      CHECK_NOT_NULL(ls_desc);
      if (OB_SUCC(ret)) {
        ls_desc->reset_stat();
      }
    }
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      if (scope != ObBalanceGroup::BG_SCOPE_MAX && iter->first.scope() != scope) {
        continue;
      }
      ARRAY_FOREACH(iter->second, i) {
        ObBalanceGroupInfo *bg_ls_info = iter->second.at(i);
        CHECK_NOT_NULL(bg_ls_info);
        if (FAILEDx(update_ls_desc_(
            bg_ls_info->get_ls_id(),
            bg_ls_info->get_part_groups_count(),
            bg_ls_info->get_part_groups_data_size(),
            bg_ls_info->get_part_groups_weight()))) {
          LOG_WARN("update ls desc failed", KR(ret), K(bg_ls_info));
        }
      } // end ARRAY_FOREACH iter->second
    }
    FOREACH_X(iter, weighted_bg_map_, OB_SUCC(ret)) {
      if (scope != ObBalanceGroup::BG_SCOPE_MAX && iter->first.scope() != scope) {
        continue;
      }
      ARRAY_FOREACH(iter->second, i) {
        ObBalanceGroupInfo *weighted_bg_ls_info = iter->second.at(i);
        CHECK_NOT_NULL(weighted_bg_ls_info);
        if (FAILEDx(update_ls_desc_(
            weighted_bg_ls_info->get_ls_id(),
            weighted_bg_ls_info->get_part_groups_count(),
            weighted_bg_ls_info->get_part_groups_data_size(),
            weighted_bg_ls_info->get_part_groups_weight()))) {
          LOG_WARN("update ls desc failed", KR(ret), K(weighted_bg_ls_info));
        }
      } // end ARRAY_FOREACH iter->second
    }
    LOG_INFO("[PART_BALANCE] prepare ls desc", KR(ret), K(tenant_id_), "bg_map size", bg_map_.size(),
        "weighted_bg_map size", weighted_bg_map_.size(), K(ls_desc_array_));
  }
  return ret;
}

int ObPartitionBalance::split_out_weighted_bg_map_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bg_map_.empty())) {
    // do nothing
  } else {
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      if (!is_bg_with_balance_weight_(iter->second)) {
        continue;
      }
      ObBalanceGroup bg = iter->first; // copy
      ARRAY_FOREACH(iter->second, i) {
        ObBalanceGroupInfo *src_bg_info = iter->second.at(i);
        CHECK_NOT_NULL(src_bg_info);
        if (OB_SUCC(ret)) {
          const ObLSID &ls_id = src_bg_info->get_ls_id();
          ObBalanceGroupInfo *weighted_bg_info = nullptr;
          if (OB_FAIL(get_bg_info_by_ls_id_(weighted_bg_map_, bg, ls_id, weighted_bg_info))) {
            LOG_WARN("get bg info by ls id failed", KR(ret), K(bg), K(ls_id), KPC(src_bg_info));
          } else if (OB_ISNULL(weighted_bg_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null bg info", KR(ret), K(bg), K(ls_id));
          } else if (OB_FAIL(src_bg_info->split_out_weighted_bg_info(*weighted_bg_info))) {
            LOG_WARN("append failed", KR(ret), K(bg), K(ls_id), KPC(src_bg_info));
          }
        }
      } // end ARRAY_FOREACH iter->second
    }
    LOG_INFO("[PART_BALANCE] split out weighted_bg_map", KR(ret), K(tenant_id_),
        "bg_map size", bg_map_.size(), "weighted_bg_map size", weighted_bg_map_.size());
  }
  return ret;
}

int ObPartitionBalance::get_bg_info_by_ls_id_(
    ObBalanceGroupMap &bg_map,
    const ObBalanceGroup &bg,
    const ObLSID &ls_id,
    ObBalanceGroupInfo *&bg_info)
{
  int ret = OB_SUCCESS;
  bg_info = nullptr;
  ObArray<ObBalanceGroupInfo *> *bg_ls_array = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(bg), K(ls_id));
  } else if (OB_FAIL(get_or_create_bg_ls_array_(bg_map, bg, bg_ls_array))) {
    LOG_WARN("get or create bg ls array failed", KR(ret), K(bg));
  } else if (OB_ISNULL(bg_ls_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null bg_ls_array", KR(ret), K(bg));
  } else {
    ARRAY_FOREACH_X(*bg_ls_array, idx, cnt, OB_SUCC(ret) && nullptr == bg_info) {
      ObBalanceGroupInfo *tmp_bg_info = bg_ls_array->at(idx);
      CHECK_NOT_NULL(tmp_bg_info);
      if (OB_FAIL(ret)) {
      } else if (tmp_bg_info->get_ls_id() == ls_id) {
        bg_info = tmp_bg_info;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(bg_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no LS found", KR(ret), K(ls_id), K(bg));
    }
  }
  return ret;
}

int ObPartitionBalance::on_new_partition(
    const ObBalanceGroup &bg_in,
    const ObSimpleTableSchemaV2 &table_schema,
    const ObObjectID part_object_id,
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const int64_t tablet_size,
    const uint64_t part_group_uid,
    const int64_t bg_balance_weight,
    const int64_t part_balance_weight)
{
  int ret = OB_SUCCESS;
  ObBalanceGroup bg = bg_in; // get a copy
  ObTransferPartInfo part_info;
  const ObObjectID &table_id = table_schema.get_table_id();
  const bool is_dup_ls_related_part = dup_ls_id_.is_valid() && (src_ls_id == dup_ls_id_ || dest_ls_id == dup_ls_id_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not inited", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg_in.is_valid()
      || !table_schema.is_valid()
      || !is_valid_id(part_object_id)
      || !is_valid_id(part_group_uid)
      || !src_ls_id.is_valid_with_tenant(tenant_id_)
      || !dest_ls_id.is_valid_with_tenant(tenant_id_)
      || tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_in), K(table_schema), K(part_object_id),
        K(src_ls_id), K(dest_ls_id), K(tablet_size), K(part_group_uid));
  } else if (OB_FAIL(part_info.init(table_id, part_object_id))) {
    LOG_WARN("part_info init fail", KR(ret), K(table_id), K(part_object_id));
  } else if (dest_ls_id != src_ls_id) { // need transfer
    // transfer caused by table duplicate_scope change or tablegroup change
    if (OB_FAIL(job_generator_.add_need_transfer_part(src_ls_id, dest_ls_id, part_info))) {
      LOG_WARN("add need transfer part failed",
          KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info), K(dup_ls_id_));
    }
  } else if (is_dup_ls_related_part) {
    // do not record dup ls related part_info
  } else if (OB_FAIL(add_new_part_to_update_maps_(
      src_ls_id,
      bg,
      table_schema,
      part_group_uid,
      part_info,
      tablet_size,
      bg_balance_weight,
      part_balance_weight))) {
    LOG_WARN("add new partition group to balance group failed", KR(ret), K(src_ls_id), K(bg),
        K(table_schema), K(part_group_uid), K(part_info), K(tablet_size));
  }
  return ret;
}

// this function is used to add new part to balance group and update scope_zone_bg_stat_map_ if bg is a ZONE scope bg
int ObPartitionBalance::add_new_part_to_update_maps_(
    const ObLSID &ls_id,
    ObBalanceGroup &bg,
    const ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    const ObTransferPartInfo &part_info,
    const int64_t tablet_size,
    const int64_t bg_balance_weight,
    const int64_t part_balance_weight)
{
  int ret = OB_SUCCESS;
  // STEP 1: update scope_zone_bg_stat_map_ when bg is a zone scope bg,
  // according to the primary zone of src_ls
  bool is_zone_mismatch = false;
  if (bg.is_scope_zone()) {
    ObLSDesc *ls_desc = nullptr;
    if (OB_FAIL(ls_desc_map_.get_refactored(ls_id, ls_desc))) {
      LOG_WARN("get ls desc failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ls desc", KR(ret), K(ls_id));
    } else {
      const ObZone &part_ls_primary_zone = ls_desc->get_primary_zone();
      ObScopeZoneBGStatInfo *stat_info = scope_zone_bg_stat_map_.get(bg);
      if (OB_ISNULL(stat_info)) {
        // use first part's balance weight as tablegroup weight
        ObScopeZoneBGStatInfo new_stat(tablet_size, bg_balance_weight, part_ls_primary_zone);
        if (OB_FAIL(scope_zone_bg_stat_map_.set_refactored(bg, new_stat))) {
          LOG_WARN("set scope zone bg stat failed", KR(ret), K(bg), K(new_stat));
        } else {
          LOG_TRACE("init scope zone bg stat", K(bg), K(new_stat));
        }
      } else if (!stat_info->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid scope zone bg stat info", KR(ret), KPC(stat_info));
      } else {
        stat_info->add_bg_data_size(tablet_size);
        if (stat_info->get_zone() != part_ls_primary_zone) {
          is_zone_mismatch = true;
          if (OB_FAIL(try_transfer_part_to_primary_zone_ls_in_group_(part_info,
              ls_desc, stat_info->get_zone()))) {
            LOG_WARN("try transfer part to bg primary zone ls in group failed", KR(ret),
                K(part_info), KPC(ls_desc), K(stat_info->get_zone()), K(bg));
          }
        }
      }
    }
  }
  // STEP 2: update bg_map_ when new part is added to bg
  // For zone scope bg, if partition is on LS with different zone from bg primary zone, skip recording it in bg_map_
  ObBalanceGroupInfo *bg_info = nullptr;
  if (OB_FAIL(ret) || (bg.is_scope_zone() && is_zone_mismatch)) {
    // skip
  } else if (OB_FAIL(get_bg_info_by_ls_id_(bg_map_, bg, ls_id, bg_info))) {
    LOG_WARN("get bg info by ls id failed", KR(ret), K(bg), K(ls_id));
  } else if (OB_ISNULL(bg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null bg info", KR(ret), K(bg), K(ls_id));
  } else if (OB_FAIL(bg_info->append_part(
      table_schema,
      part_info,
      tablet_size,
      part_group_uid,
      part_balance_weight))) {
    LOG_WARN("append_part failed", KR(ret), K(part_info), K(tablet_size),
        K(part_group_uid), K(part_balance_weight), K(table_schema));
  }
  return ret;
}

int ObPartitionBalance::try_transfer_part_to_primary_zone_ls_in_group_(
    const ObTransferPartInfo &part_info,
    const ObLSDesc *src_ls_desc,
    const ObZone &target_primary_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!part_info.is_valid() || OB_ISNULL(src_ls_desc) || target_primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_info), KP(src_ls_desc), K(target_primary_zone));
  } else if (src_ls_desc->get_primary_zone() == target_primary_zone) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls primary zone is the same as target primary zone", KR(ret), K(part_info), KP(src_ls_desc), K(target_primary_zone));
  } else {
    ObLSDesc *dest_ls_desc = nullptr;
    if (OB_FAIL(get_ls_desc_in_same_ls_group_on_target_zone_(src_ls_desc, target_primary_zone, dest_ls_desc))) {
      LOG_WARN("get ls desc in same ls group with zone failed", KR(ret), KPC(src_ls_desc), K(target_primary_zone));
    } else if (OB_ISNULL(dest_ls_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null dest ls desc", KR(ret), K(part_info), KPC(src_ls_desc), K(target_primary_zone));
    } else if (OB_FAIL(job_generator_.add_need_transfer_part(src_ls_desc->get_ls_id(), dest_ls_desc->get_ls_id(), part_info))) {
      LOG_WARN("add need transfer part failed", KR(ret), KPC(src_ls_desc), KPC(dest_ls_desc), K(part_info));
    }
  }
  return ret;
}

int ObPartitionBalance::get_or_create_bg_ls_array_(
    ObBalanceGroupMap &bg_map,
    const ObBalanceGroup &bg,
    ObArray<ObBalanceGroupInfo *> *&bg_ls_array)
{
  int ret = OB_SUCCESS;
  bg_ls_array = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(bg_ls_array = bg_map.get(bg))) {
    ObArray<ObBalanceGroupInfo *> bg_ls_array_obj;
    if (OB_FAIL(bg_map.set_refactored(bg, bg_ls_array_obj))) {
      LOG_WARN("init part to balance group fail", KR(ret), K(bg));
    } else {
      bg_ls_array = bg_map.get(bg);
      if (OB_ISNULL(bg_ls_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get part from balance group fail", KR(ret), K(bg));
      } else if (FALSE_IT(bg_ls_array->set_block_size(sizeof(ObBalanceGroupInfo *) * ls_desc_array_.count()))) {
      } else if (FALSE_IT(bg_ls_array->set_block_allocator(ModulePageAllocator(allocator_, "PB_BGLSArr")))) {
      } else if (OB_FAIL(bg_ls_array->reserve(ls_desc_array_.count()))) {
        LOG_WARN("fail to reserve", KR(ret), K(bg_ls_array));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_desc_array_.count(); i++) {
          ObBalanceGroupInfo *ls_part_desc = nullptr;
          ObBalanceGroupInfo *bg_info = nullptr;
          if (OB_ISNULL(bg_info = reinterpret_cast<ObBalanceGroupInfo *>(
              allocator_.alloc(sizeof(ObBalanceGroupInfo))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_ISNULL(bg_info = new(bg_info) ObBalanceGroupInfo(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_FAIL(bg_info->init(
              bg.id(),
              ls_desc_array_.at(i)->get_ls_id(),
              ls_desc_array_.count()))) {
            LOG_WARN("failed to init bg_info", KR(ret), K(bg_info), K(ls_desc_array_), K(i));
          } else if (OB_FAIL(bg_ls_array->push_back(bg_info))) {
            LOG_WARN("push_back fail", KR(ret), K(bg));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(bg_ls_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null bg_ls_array", KR(ret), K(bg), KP(bg_ls_array));
  }
  return ret;
}

int ObPartitionBalance::get_ls_desc_in_same_ls_group_on_target_zone_(
    const ObLSDesc *src_ls_desc,
    const ObZone &target_zone,
    ObLSDesc *&dest_ls_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(NULL == src_ls_desc || target_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(src_ls_desc), K(target_zone));
  } else if (OB_UNLIKELY(src_ls_desc->get_primary_zone() == target_zone)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls zone is the same as target zone", KR(ret), KPC(src_ls_desc), K(target_zone));
  } else {
    bool found = false;
    ARRAY_FOREACH_X(ls_desc_array_, idx, cnt, OB_SUCC(ret) && !found) {
      ObLSDesc *ls_desc = ls_desc_array_.at(idx);
      CHECK_NOT_NULL(ls_desc);
      if (OB_SUCC(ret)
          && ls_desc->get_primary_zone() == target_zone
          && ls_desc->get_ls_group_id() == src_ls_desc->get_ls_group_id()) {
        dest_ls_desc = ls_desc;
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_REBALANCE_TASK_CANT_EXEC;
      FLOG_INFO("no ls in target zone in the same ls group, user ls primary zone is not balanced",
          KR(ret), KPC(src_ls_desc), K(target_zone));
    }
  }
  return ret;
}

int ObPartitionBalance::try_transfer_zone_scope_bg_to_zone_(
    const ObBalanceGroup &bg,
    const ObZone &target_zone)
{
  int ret = OB_SUCCESS;
  ObArray<ObBalanceGroupInfo *> *bg_ls_array = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg.is_valid()
      || !bg.is_scope_zone()
      || target_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg), K(target_zone));
  } else if (FALSE_IT(bg_ls_array = bg_map_.get(bg))) {
  } else if (OB_ISNULL(bg_ls_array) || bg_ls_array->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone scope bg not found in bg_map", KR(ret), K(bg), K(target_zone), KP(bg_ls_array));
  } else {
    ARRAY_FOREACH_X(*bg_ls_array, idx, cnt, OB_SUCC(ret)) {
      ObBalanceGroupInfo *src_bg_info = bg_ls_array->at(idx);
      ObLSDesc *src_ls_desc = nullptr;
      ObLSDesc *dest_ls_desc = nullptr;
      if (OB_ISNULL(src_bg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null bg info", KR(ret), K(bg), K(idx));
      } else if (OB_FAIL(ls_desc_map_.get_refactored(src_bg_info->get_ls_id(), src_ls_desc))) {
        LOG_WARN("get ls desc failed", KR(ret), K(bg), "src_ls_id", src_bg_info->get_ls_id());
      } else if (OB_ISNULL(src_ls_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ls desc", KR(ret), K(bg), "src_ls_id", src_bg_info->get_ls_id());
      } else if (target_zone == src_ls_desc->get_primary_zone()) {
        // skip bg ls info in target zone
      } else if (OB_FAIL(get_ls_desc_in_same_ls_group_on_target_zone_(src_ls_desc, target_zone, dest_ls_desc))) {
        LOG_WARN("get dest ls desc failed", KR(ret), K(bg), K(target_zone), KPC(src_ls_desc));
      } else if (OB_ISNULL(dest_ls_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null dest ls desc", KR(ret), K(bg), K(target_zone), KPC(src_ls_desc));
      } else {
        const ObLSID &src_ls_id = src_ls_desc->get_ls_id();
        const ObLSID &dest_ls_id = dest_ls_desc->get_ls_id();
        ObBalanceGroupInfo *dest_bg_info = nullptr;
        if (OB_FAIL(get_bg_info_by_ls_id_(bg_map_, bg, dest_ls_id, dest_bg_info))) {
          LOG_WARN("get bg info by ls id failed", KR(ret), K(bg), K(dest_ls_id));
        }
        CHECK_NOT_NULL(dest_bg_info);
        while (OB_SUCC(ret) && src_bg_info->get_part_groups_count() > 0) {
          ObPartGroupInfo *part_group = nullptr;
          if (OB_FAIL(src_bg_info->transfer_out_by_round_robin(*dest_bg_info, part_group))) {
            LOG_WARN("transfer out part group failed", KR(ret), K(bg), K(src_ls_id), K(dest_ls_id));
          } else if (OB_ISNULL(part_group) || OB_UNLIKELY(!part_group->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid part group", KR(ret), K(bg), KP(part_group), K(src_ls_id), K(dest_ls_id));
          } else if (OB_FAIL(add_transfer_task_(src_ls_id, dest_ls_id, part_group))) {
            LOG_WARN("add transfer task failed", KR(ret), K(bg), K(src_ls_id), K(dest_ls_id), KPC(part_group));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionBalance::get_ls_primary_zone_array_(
    ObIArray<ObZone> &primary_zone_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else {
    primary_zone_array.reset();
    ARRAY_FOREACH_X(ls_desc_array_, idx, cnt, OB_SUCC(ret)) {
      const ObLSDesc *ls_desc = ls_desc_array_.at(idx);
      if (OB_ISNULL(ls_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ls desc", KR(ret), K(idx));
      } else if (OB_FAIL(add_var_to_array_no_dup(primary_zone_array, ls_desc->get_primary_zone()))) {
        LOG_WARN("push back failed", KR(ret), K(ls_desc->get_primary_zone()));
      }
    }
  }
  return ret;
}

int ObPartitionBalance::build_zone_bg_stat_array_(
    ObIArray<ZoneBGStat> &unweighted_zone_stats,
    ObIArray<ZoneBGStat> &weighted_zone_stats)
{
  int ret = OB_SUCCESS;
  unweighted_zone_stats.reset();
  weighted_zone_stats.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else {
    ObSEArray<ObZone, DEFAULT_ZONE_COUNT> primary_zone_array;
    if (OB_FAIL(get_ls_primary_zone_array_(primary_zone_array))) {
      LOG_WARN("failed to get ls primary zone array", KR(ret), K(tenant_id_));
    } else {
      ARRAY_FOREACH(primary_zone_array, idx) {
        ZoneBGStat stat;
        if (OB_FAIL(stat.init(primary_zone_array.at(idx)))) {
          LOG_WARN("init zone bg stat failed", KR(ret), K(primary_zone_array.at(idx)));
        } else if (OB_FAIL(unweighted_zone_stats.push_back(stat))) {
          LOG_WARN("push back failed", KR(ret), K(primary_zone_array.at(idx)));
        } else if (OB_FAIL(weighted_zone_stats.push_back(stat))) {
          LOG_WARN("push back failed", KR(ret), K(primary_zone_array.at(idx)));
        }
      }
    }
    // build zone -> bg list, split by whether bg has weight
    FOREACH_X(iter, scope_zone_bg_stat_map_, OB_SUCC(ret)) {
      const ObBalanceGroup &bg = iter->first;
      const ObScopeZoneBGStatInfo &stat_info = iter->second;
      if (OB_UNLIKELY(!stat_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid scope zone bg stat info", K(bg), K(stat_info));
      } else {
        ObIArray<ZoneBGStat> &zone_stats = (0 == stat_info.get_bg_weight() ? unweighted_zone_stats : weighted_zone_stats);
        bool found = false;
        ARRAY_FOREACH_X(zone_stats, z_idx, cnt, OB_SUCC(ret) && !found) {
          if (zone_stats.at(z_idx).get_zone() == stat_info.get_zone()) {
            found = true;
            ZoneBGItem bg_item;
            if (OB_FAIL(bg_item.init(bg, stat_info.get_bg_data_size(), stat_info.get_bg_weight()))) {
              LOG_WARN("init bg item failed", KR(ret), K(bg), K(stat_info));
            } else if (OB_FAIL(zone_stats.at(z_idx).add_bg_item(bg_item))) {
              LOG_WARN("push back failed", KR(ret), K(bg_item));
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone of scope zone bg not found in primary_zones unexpected", KR(ret), K(bg), K(stat_info),
              K(primary_zone_array));
        }
      }
    }
  }
  return ret;
}

int ObPartitionBalance::process_balance_zone_tablegroup_count_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> unweighted_zone_stats;
  ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> weighted_zone_stats;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (scope_zone_bg_stat_map_.empty()) {
    // do nothing
  } else if (OB_FAIL(build_zone_bg_stat_array_(unweighted_zone_stats, weighted_zone_stats))) {
    LOG_WARN("build zone bg stat array failed", KR(ret), K(tenant_id_));
  } else {
    // only unweighted zone stats are considered for now
    ObIArray<ZoneBGStat> &zone_stats = unweighted_zone_stats;
    // balance zone bg count by transferring whole bg
    while (OB_SUCC(ret)) {
      // Find the zone with the most bg_items_ and the one with the least
      ZoneBGStat *src_zone_stat = nullptr;
      ZoneBGStat *dest_zone_stat = nullptr;
      ARRAY_FOREACH(zone_stats, z_idx) {
        ZoneBGStat *curr_stat = &zone_stats.at(z_idx);
        if (nullptr == src_zone_stat || curr_stat->get_bg_cnt() > src_zone_stat->get_bg_cnt()) {
          src_zone_stat = curr_stat;
        }
        if (nullptr == dest_zone_stat || curr_stat->get_bg_cnt() < dest_zone_stat->get_bg_cnt()) {
          dest_zone_stat = curr_stat;
        }
      }
      if (OB_ISNULL(src_zone_stat) || OB_ISNULL(dest_zone_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null zone stat", KR(ret), KP(src_zone_stat), KP(dest_zone_stat));
      } else if (src_zone_stat->get_bg_cnt() - dest_zone_stat->get_bg_cnt() <= 1) {
        // already balanced
        break;
      } else {
        // always choose the last bg item of src_zone_stat to move to dest_zone_stat
        ZoneBGItem moved_item;
        if (OB_FAIL(src_zone_stat->remove_last_bg_item(moved_item))) {
          LOG_WARN("remove last bg item failed", KR(ret), KPC(src_zone_stat));
        } else if (OB_FAIL(try_transfer_zone_scope_bg_to_zone_(moved_item.get_bg(), dest_zone_stat->get_zone()))) {
          LOG_WARN("try transfer zone scope bg to zone failed", KR(ret), K(moved_item), "target_zone", dest_zone_stat->get_zone());
        } else if (OB_FAIL(dest_zone_stat->add_bg_item(moved_item))) {
          LOG_WARN("push back moved bg failed", KR(ret), K(moved_item));
        } else {
          LOG_INFO("[PART_BALANCE] move zone scope bg for zone tg count balance",
              K(moved_item), "from_zone", src_zone_stat->get_zone(), "to_zone", dest_zone_stat->get_zone(),
              "from_cnt", src_zone_stat->get_bg_cnt() + 1, "to_cnt", dest_zone_stat->get_bg_cnt() - 1);
        }
      }
    } // end while

    LOG_INFO("[PART_BALANCE] process_balance_zone_tablegroup_count_ end", KR(ret), K(tenant_id_), K(zone_stats));
  }
  return ret;
}

template <ObPartitionBalance::ZoneBGItem::MetricType metric_type>
int ObPartitionBalance::get_zone_metric_avg_(
    const ObIArray<ZoneBGStat> &zone_stats,
    const int64_t begin_idx,
    const int64_t end_idx,
    double &avg) const
{
  int ret = OB_SUCCESS;
  avg = 0;
  if (OB_UNLIKELY(begin_idx < 0
      || end_idx < 0
      || begin_idx >= zone_stats.count()
      || end_idx >= zone_stats.count()
      || begin_idx > end_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(begin_idx), K(end_idx), K(zone_stats));
  } else {
    double sum = 0;
    for (int64_t i = begin_idx; i <= end_idx; ++i) {
      sum += zone_stats.at(i).get_total<metric_type>();
    }
    avg = sum / static_cast<double>(end_idx - begin_idx + 1);
  }
  return ret;
}

int ObPartitionBalance::move_zone_bg_item_(
    ZoneBGStat &src_zone_stat,
    ZoneBGStat &dest_zone_stat,
    const int64_t src_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ZoneBGItem> &src_items = src_zone_stat.get_bg_items();
  if (OB_UNLIKELY(src_idx < 0 || src_idx >= src_items.count()
      || src_zone_stat.get_zone() == dest_zone_stat.get_zone())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(src_idx), K(src_zone_stat), K(dest_zone_stat));
  } else {
    const ZoneBGItem src_item = src_items.at(src_idx);
    if (OB_FAIL(try_transfer_zone_scope_bg_to_zone_(src_item.get_bg(), dest_zone_stat.get_zone()))) {
      LOG_WARN("try transfer zone scope bg to zone failed", KR(ret), K(src_item), K(dest_zone_stat));
    } else if (OB_FAIL(dest_zone_stat.add_bg_item(src_item))) {
      LOG_WARN("push back moved bg failed", KR(ret), K(src_item));
    } else if (OB_FAIL(src_items.remove(src_idx))) {
      LOG_WARN("remove moved bg failed", KR(ret), K(src_idx), K(src_zone_stat));
    }
  }
  return ret;
}

int ObPartitionBalance::swap_zone_bg_items_(
    ZoneBGStat &left_zone_stat,
    ZoneBGStat &right_zone_stat,
    const int64_t left_idx,
    const int64_t right_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ZoneBGItem> &left_items = left_zone_stat.get_bg_items();
  ObArray<ZoneBGItem> &right_items = right_zone_stat.get_bg_items();
  if (OB_UNLIKELY(left_idx < 0 || left_idx >= left_items.count()
      || right_idx < 0 || right_idx >= right_items.count()
      || left_zone_stat.get_zone() == right_zone_stat.get_zone())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(left_idx), K(right_idx), K(left_zone_stat), K(right_zone_stat));
  } else {
    const ZoneBGItem left_item = left_items.at(left_idx);
    const ZoneBGItem right_item = right_items.at(right_idx);
    // TODO: (cangming.zl) bg的跨zone transfer可以去重, 减少跨zone transfer的次数
    if (OB_FAIL(try_transfer_zone_scope_bg_to_zone_(left_item.get_bg(), right_zone_stat.get_zone()))) {
      LOG_WARN("try transfer zone scope bg to zone failed", KR(ret), K(left_item), K(right_zone_stat));
    } else if (OB_FAIL(try_transfer_zone_scope_bg_to_zone_(right_item.get_bg(), left_zone_stat.get_zone()))) {
      LOG_WARN("try transfer zone scope bg to zone failed", KR(ret), K(right_item), K(left_zone_stat));
    } else {
      left_items.at(left_idx) = right_item;
      right_items.at(right_idx) = left_item;
    }
  }
  return ret;
}

int ObPartitionBalance::try_move_zone_bg_by_weight_(
    ZoneBGStat &src_zone_stat,
    ZoneBGStat &dest_zone_stat,
    const double avg,
    int64_t &transfer_bg_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ZoneBGItem> &src_items = src_zone_stat.get_bg_items();
  if (!src_items.empty()) {
    lib::ob_sort(src_items.begin(), src_items.end(), ZoneBGItem::less<ZoneBGItem::WEIGHT>);
  }
  // iterate from back to front (largest weight to the smallest)
  for (int64_t idx = src_items.count() - 1; idx >= 0 && OB_SUCC(ret); --idx) {
    const int64_t curr_weight = src_items.at(idx).get_metric<ZoneBGItem::WEIGHT>();
    const int64_t src_total = src_zone_stat.get_total<ZoneBGItem::WEIGHT>();
    const int64_t dest_total = dest_zone_stat.get_total<ZoneBGItem::WEIGHT>();
    const double diff_before = fabs(src_total - avg) + fabs(dest_total - avg);
    const double diff_after = fabs(src_total - curr_weight - avg)
        + fabs(dest_total + curr_weight - avg);
    LOG_TRACE("compare diff for try move zone bg between zones for weight balance",
        K(diff_before), K(diff_after), K(avg), K(curr_weight), K(src_zone_stat), K(dest_zone_stat));
    if (diff_before - diff_after > OB_DOUBLE_EPSINON) {
      if (OB_FAIL(move_zone_bg_item_(src_zone_stat, dest_zone_stat, idx))) {
        LOG_WARN("move zone bg item failed", KR(ret), K(idx), K(src_zone_stat), K(dest_zone_stat));
      } else {
        ++transfer_bg_cnt;
        LOG_INFO("[PART_BALANCE] move zone scope bg for zone tg weight balance",
            "bg_weight", curr_weight,
            "from_zone", src_zone_stat.get_zone(),
            "to_zone", dest_zone_stat.get_zone(),
            K(diff_before), K(diff_after), K(avg));
      }
    } else if (REACH_COUNT_INTERVAL(100)) {
      LOG_INFO("[PART_BALANCE] no need to move zone bg for weight balance", K(diff_before), K(diff_after),
          K(transfer_bg_cnt), K(avg), K(curr_weight), K(src_zone_stat), K(dest_zone_stat));
    }
  }
  LOG_INFO("[PART_BALANCE] try move zone bg to dest zone finished",
      KR(ret), K(transfer_bg_cnt), K(avg), "src_zone", src_zone_stat.get_zone(), "dest_zone", dest_zone_stat.get_zone());
  return ret;
}

template <ObPartitionBalance::ZoneBGItem::MetricType metric_type>
int ObPartitionBalance::try_swap_zone_bg_by_metric_(
    ZoneBGStat &src_zone_stat,
    ZoneBGStat &dest_zone_stat,
    const double avg,
    int64_t &transfer_bg_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ZoneBGItem> &src_items = src_zone_stat.get_bg_items();
  ObArray<ZoneBGItem> &dest_items = dest_zone_stat.get_bg_items();
  if (src_items.empty() || dest_items.empty()) {
    // do nothing
  } else {
    // iterate src zone bg from largest to smallest
    lib::ob_sort(src_items.begin(), src_items.end(), ZoneBGItem::less<metric_type>);
    for (int64_t i = src_items.count() - 1; i >= 0 && OB_SUCC(ret) && 0 == transfer_bg_cnt; --i) {
      const int64_t src_total = src_zone_stat.get_total<metric_type>();
      const int64_t dest_total = dest_zone_stat.get_total<metric_type>();
      const ZoneBGItem &left_item = src_items.at(i);
      // iterate dest zone bg from smallest to the largest
      lib::ob_sort(dest_items.begin(), dest_items.end(), ZoneBGItem::less<metric_type>);
      for (int64_t j = 0; j < dest_items.count() && OB_SUCC(ret) && 0 == transfer_bg_cnt; ++j) {
        const ZoneBGItem &right_item = dest_items.at(j);
        if (!ZoneBGItem::less<metric_type>(right_item, left_item)) { // left_item <= right_item
          break; // swap only the large weight/disk on the left and the small weight/disk on the right
        } else if (ZoneBGItem::DISK == metric_type
            && left_item.get_metric<ZoneBGItem::WEIGHT>() != right_item.get_metric<ZoneBGItem::WEIGHT>()) {
          continue; // if swap by disk, only swap when weight is the same
        } else {
          const double diff_before = fabs(src_total - avg) + fabs(dest_total - avg);
          const double delta = left_item.get_metric<metric_type>() - right_item.get_metric<metric_type>();
          const double diff_after = fabs(src_total - delta - avg) + fabs(dest_total + delta - avg);
          LOG_TRACE("compare diff for trying swap zone bg between zones for weight/disk balance",
              K(diff_before), K(diff_after), K(avg), K(left_item), K(right_item), K(src_zone_stat), K(dest_zone_stat));
          if (diff_before - diff_after > OB_DOUBLE_EPSINON) {
            if (OB_FAIL(swap_zone_bg_items_(src_zone_stat, dest_zone_stat, i, j))) {
              LOG_WARN("swap zone bg items failed", KR(ret), K(i), K(j), K(src_zone_stat), K(dest_zone_stat));
            } else {
              ++transfer_bg_cnt;
              LOG_INFO("[PART_BALANCE] swap zone scope bg for zone tg weight/disk balance",
                  K(left_item), K(right_item),
                  "src_zone", src_zone_stat.get_zone(),
                  "dest_zone", dest_zone_stat.get_zone(),
                  K(diff_before), K(diff_after), K(avg));
              break; // dest_zone_stat should be reordered after transfer
            }
          } else if (REACH_COUNT_INTERVAL(100)) {
            LOG_INFO("[PART_BALANCE] no need to swap zone bg for weight/disk balance", K(diff_before), K(diff_after),
                K(transfer_bg_cnt), K(avg), K(left_item), K(right_item), K(src_zone_stat), K(dest_zone_stat));
          }
        }
      } // end for j
    } // end for i
    LOG_INFO("[PART_BALANCE] try swap zone bg between zones finished",
        KR(ret), K(transfer_bg_cnt), K(avg), "src_zone", src_zone_stat.get_zone(), "dest_zone", dest_zone_stat.get_zone(), K(metric_type));
  }
  return ret;
}

int ObPartitionBalance::process_balance_zone_tablegroup_weight_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> unweighted_zone_stats;
  ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> weighted_zone_stats;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (scope_zone_bg_stat_map_.empty()) {
    // do nothing
  } else if (OB_FAIL(build_zone_bg_stat_array_(unweighted_zone_stats, weighted_zone_stats))) {
    LOG_WARN("build zone bg stat array failed", KR(ret), K(tenant_id_));
  } else if (weighted_zone_stats.count() >= 2) {
    int64_t transfer_bg_cnt = 0;
    int64_t total_transfer_bg_cnt = 0;
    do {
      transfer_bg_cnt = 0;
      lib::ob_sort(weighted_zone_stats.begin(), weighted_zone_stats.end(), ZoneBGStat::less<ZoneBGItem::WEIGHT>);
      ZoneBGStat &min_zone_stat = weighted_zone_stats.at(0);
      // STEP 1: try move (from max to second max zone -> min zone)
      for (int64_t idx = weighted_zone_stats.count() - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_bg_cnt; --idx) {
        double avg = 0;
        if (FAILEDx(get_zone_metric_avg_<ZoneBGItem::WEIGHT>(weighted_zone_stats, 0/*begin_idx*/, idx, avg))) {
          LOG_WARN("get zone weight avg failed", KR(ret), K(idx), K(weighted_zone_stats));
        } else if (OB_FAIL(try_move_zone_bg_by_weight_(weighted_zone_stats.at(idx), min_zone_stat, avg, transfer_bg_cnt))) {
          LOG_WARN("try move zone bg by weight failed", KR(ret), K(idx), K(avg), K(weighted_zone_stats));
        }
      }
      // STEP 2: try swap (from max to second max zone <-> min zone)
      for (int64_t idx = weighted_zone_stats.count() - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_bg_cnt; --idx) {
        double avg = 0;
        if (FAILEDx(get_zone_metric_avg_<ZoneBGItem::WEIGHT>(weighted_zone_stats, 0/*begin_idx*/, idx, avg))) {
          LOG_WARN("get zone weight avg failed", KR(ret), K(idx), K(weighted_zone_stats));
        } else if (OB_FAIL(try_swap_zone_bg_by_metric_<ZoneBGItem::WEIGHT>(
            weighted_zone_stats.at(idx), min_zone_stat, avg, transfer_bg_cnt))) {
          LOG_WARN("try swap zone bg by weight failed", KR(ret), K(idx), K(avg), K(weighted_zone_stats));
        }
      }
      if (OB_SUCC(ret) && transfer_bg_cnt > 0) {
        total_transfer_bg_cnt += transfer_bg_cnt; // only for logging
      }
    } while (OB_SUCC(ret) && transfer_bg_cnt > 0);
    LOG_INFO("[PART_BALANCE] process_balance_zone_tablegroup_weight_ end",
        KR(ret), K(tenant_id_), K(weighted_zone_stats), K(total_transfer_bg_cnt));
  }
  return ret;
}

bool ObPartitionBalance::check_need_zone_disk_balance_(
    const ZoneBGStat &max_zone_stat,
    const ZoneBGStat &min_zone_stat) const
{
  bool need = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    SHARE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tenant config is invalid", K(tenant_id_));
  } else {
    const int64_t tolerance_percent = tenant_config->zone_disk_balance_tolerance_percentage;
    const int64_t disk_threshold = tenant_config->_partition_balance_disk_threshold;
    const int64_t max_zone_disk = max_zone_stat.get_total<ZoneBGItem::DISK>();
    const int64_t min_zone_disk = min_zone_stat.get_total<ZoneBGItem::DISK>();
    const int64_t diff = max_zone_disk - min_zone_disk;
    int64_t ls_cnt_in_max_zone = 0;
    ARRAY_FOREACH_NORET(ls_desc_array_, idx) {
      const ObLSDesc *ls_desc = ls_desc_array_.at(idx);
      if (OB_ISNULL(ls_desc)) {
        SHARE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "ls desc is null", K(idx), K(ls_desc_array_));
      } else if (ls_desc->get_primary_zone() == max_zone_stat.get_zone()) {
        ++ls_cnt_in_max_zone;
      }
    }
    if (tolerance_percent >= 100) {
      // within tolerance
    } else if (max_zone_disk * tolerance_percent / 100 >= diff) {
      // within tolerance
    } else if (ls_cnt_in_max_zone <= 0) {
      // defensive
    } else if (max_zone_disk < ls_cnt_in_max_zone * disk_threshold) {
      // disk usage per LS is expected below threshold
    } else {
      need = true;
    }
  }
  return need;
}

int ObPartitionBalance::balance_zone_tablegroup_disk_(
    ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> &zone_stats)
{
  int ret = OB_SUCCESS;
  if (zone_stats.count() <= 1) {
    // do nothing
  } else {
    int64_t transfer_bg_cnt = 0;
    int64_t total_transfer_bg_cnt = 0;
    do {
      transfer_bg_cnt = 0;
      lib::ob_sort(zone_stats.begin(), zone_stats.end(), ZoneBGStat::less<ZoneBGItem::DISK>);
      ZoneBGStat &min_zone_stat = zone_stats.at(0);
      ZoneBGStat &max_zone_stat = zone_stats.at(zone_stats.count() - 1);
      if (!check_need_zone_disk_balance_(max_zone_stat, min_zone_stat)) {
        break;
      }
      // try swap once per round (from max to second max zone <-> min zone)
      for (int64_t idx = zone_stats.count() - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_bg_cnt; --idx) {
        double avg = 0;
        if (FAILEDx(get_zone_metric_avg_<ZoneBGItem::DISK>(zone_stats, 0/*begin_idx*/, idx, avg))) {
          LOG_WARN("get zone disk avg failed", KR(ret), K(idx), K(zone_stats));
        } else if (OB_FAIL(try_swap_zone_bg_by_metric_<ZoneBGItem::DISK>(
            zone_stats.at(idx), min_zone_stat, avg, transfer_bg_cnt))) {
          LOG_WARN("try swap zone bg by disk failed", KR(ret), K(idx), K(avg), K(zone_stats));
        }
      }
      if (OB_SUCC(ret) && transfer_bg_cnt > 0) {
        total_transfer_bg_cnt += transfer_bg_cnt; // only for logging
      }
    } while (OB_SUCC(ret) && transfer_bg_cnt > 0);
    LOG_INFO("[PART_BALANCE] balance zone tablegroup disk end", KR(ret), K(tenant_id_), K(zone_stats),
        K(total_transfer_bg_cnt));
  }
  return ret;
}

int ObPartitionBalance::process_balance_zone_tablegroup_disk_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> unweighted_zone_stats;
  ObSEArray<ZoneBGStat, DEFAULT_ZONE_COUNT> weighted_zone_stats;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (scope_zone_bg_stat_map_.empty()) {
    // do nothing
  } else if (OB_FAIL(build_zone_bg_stat_array_(unweighted_zone_stats, weighted_zone_stats))) {
    LOG_WARN("build zone bg stat array failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(balance_zone_tablegroup_disk_(unweighted_zone_stats))) {
    LOG_WARN("balance unweighted zone tablegroup disk failed", KR(ret), K(unweighted_zone_stats));
  } else if (OB_FAIL(balance_zone_tablegroup_disk_(weighted_zone_stats))) {
    LOG_WARN("balance weighted zone tablegroup disk failed", KR(ret), K(weighted_zone_stats));
  }
  LOG_INFO("[PART_BALANCE] process_balance_zone_tablegroup_disk_ end", KR(ret), K(tenant_id_),
      K(unweighted_zone_stats), K(weighted_zone_stats));
  return ret;
}

int ObPartitionBalance::process_weight_balance_intragroup_()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (weighted_bg_map_.empty()) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id_), K(data_version));
  } else if (data_version < MOCK_DATA_VERSION_4_2_5_2
    || (data_version >= DATA_VERSION_4_3_0_0
        && data_version < DATA_VERSION_4_4_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support version", KR(ret), K(tenant_id_), K(data_version));
  } else {
    FOREACH_X(iter, weighted_bg_map_, OB_SUCC(ret)) {
      ObArray<ObBalanceGroupInfo *> &ls_pg_desc_arr = iter->second;
      int64_t transfer_cnt = 0;
      if (OB_UNLIKELY(!is_bg_with_balance_weight_(ls_pg_desc_arr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("weighted bg has no weight", KR(ret), "bg", iter->first, K(ls_pg_desc_arr));
      } else {
        do {
          transfer_cnt = 0;
          // from smallest weight to the largest
          lib::ob_sort(ls_pg_desc_arr.begin(), ls_pg_desc_arr.end(), ObBalanceGroupInfo::weight_cmp);
          if (OB_FAIL(try_move_weighted_part_group_(ls_pg_desc_arr, transfer_cnt))) {
            LOG_WARN("try move weighted part group failed", KR(ret), K(ls_pg_desc_arr), K(transfer_cnt));
          } else if (transfer_cnt > 0) {
            continue;
          } else if (OB_FAIL(try_swap_weighted_part_group_(ls_pg_desc_arr, transfer_cnt))) {
            LOG_WARN("try swap weighted part group failed", KR(ret), K(ls_pg_desc_arr), K(transfer_cnt));
          }
        } while (OB_SUCC(ret) && transfer_cnt > 0);
      }
    }
    LOG_INFO("[PART_BALANCE] process_weight_balance_intragroup_ end",
        KR(ret), K(tenant_id_), K(ls_desc_array_));
  }
  return ret;
}

int ObPartitionBalance::try_move_weighted_part_group_(
    ObArray<ObBalanceGroupInfo *> &sorted_ls_pg_desc_arr,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  transfer_cnt = 0;
  double avg = 0;
  const int64_t ls_cnt = sorted_ls_pg_desc_arr.count();
  if (OB_UNLIKELY(sorted_ls_pg_desc_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sorted_ls_pg_desc_arr", KR(ret), K(sorted_ls_pg_desc_arr));
  } else {
    ObBalanceGroupInfo *min_ls = sorted_ls_pg_desc_arr.at(0);
    CHECK_NOT_NULL(min_ls);
    // iterate from the largest ls to the second-smallest ls;
    // need reordered after transfer
    for (int64_t idx = ls_cnt - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_cnt; --idx) {
      ObBalanceGroupInfo *cur_ls = sorted_ls_pg_desc_arr.at(idx);
      CHECK_NOT_NULL(cur_ls);
      if (FAILEDx(get_ls_balance_weight_avg_(sorted_ls_pg_desc_arr, 0/*begin_idx*/, idx, avg))) {
        LOG_WARN("get ls balance weight avg failed", KR(ret), K(idx), K(sorted_ls_pg_desc_arr));
      } else if (OB_FAIL(try_move_weighted_pg_to_dest_ls_(avg, cur_ls, min_ls, transfer_cnt))) {
        LOG_WARN("try move weighted pg to dest ls failed", KR(ret), K(avg), KPC(cur_ls), KPC(min_ls));
      }
    }
  }
  return ret;
}

int ObPartitionBalance::try_move_weighted_pg_to_dest_ls_(
    const double avg,
    ObBalanceGroupInfo *src_ls,
    ObBalanceGroupInfo *dest_ls,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> weight_arr;
  if (OB_ISNULL(src_ls) || OB_ISNULL(dest_ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ptr", KR(ret), KP(src_ls), KP(dest_ls), K(avg));
  } else if (OB_FAIL(src_ls->get_balance_weight_array(weight_arr))) {
    LOG_WARN("get balance weight array failed", KR(ret),
        K(avg), KPC(src_ls), KPC(dest_ls), K(transfer_cnt));
  } else {
    // from smallest weight to the largest
    lib::ob_sort(weight_arr.begin(), weight_arr.end());
    // iterate from back to front (largest weight to the smallest)
    for (int64_t idx = weight_arr.count() - 1; idx >= 0 && OB_SUCC(ret); --idx) {
      const int64_t curr_weight = weight_arr.at(idx);
      ObPartGroupInfo *curr_pg = nullptr;
      const double diff_before = fabs(src_ls->get_part_groups_weight() - avg) + fabs(dest_ls->get_part_groups_weight() - avg);
      const double diff_after = fabs(src_ls->get_part_groups_weight() - curr_weight - avg)
          + fabs(dest_ls->get_part_groups_weight() + curr_weight - avg);
      LOG_TRACE("compare diff for try move pg between ls for weight balance",
          K(diff_before), K(diff_after), K(avg), K(curr_weight), KPC(src_ls), KPC(dest_ls));
      if (diff_before - diff_after > OB_DOUBLE_EPSINON) {
        if (OB_FAIL(src_ls->transfer_out_by_balance_weight(curr_weight, *dest_ls, curr_pg))) {
          LOG_WARN("transfer out faield", KR(ret), K(avg), KPC(src_ls), KPC(dest_ls), K(transfer_cnt));
        } else if (OB_FAIL(add_transfer_task_(src_ls->get_ls_id(), dest_ls->get_ls_id(), curr_pg))) {
          LOG_WARN("add transfer task failed", KR(ret), K(avg), KPC(src_ls), KPC(dest_ls));
        } else {
          ++transfer_cnt;
          LOG_INFO("[PART_BALANCE] move part group for weight balance", K(diff_before), K(diff_after), K(curr_weight),
              K(transfer_cnt), K(avg), KPC(curr_pg), "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
        }
      } else if (REACH_COUNT_INTERVAL(100)) {
        LOG_INFO("[PART_BALANCE] no need to move part group for weight balance", K(diff_before), K(diff_after),
            K(transfer_cnt), K(avg), K(curr_weight), K(weight_arr), KPC(src_ls), KPC(dest_ls));
      }
    }
    LOG_INFO("[PART_BALANCE] try move weighted part group to dest ls finished",
        KR(ret), K(transfer_cnt), K(avg), "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
  }
  return ret;
}

int ObPartitionBalance::try_swap_weighted_part_group_(
    ObArray<ObBalanceGroupInfo *> &sorted_ls_pg_desc_arr,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  transfer_cnt = 0;
  double avg = 0;
  const int64_t ls_cnt = sorted_ls_pg_desc_arr.count();
  if (OB_UNLIKELY(sorted_ls_pg_desc_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sorted_ls_pg_desc_arr", KR(ret), K(sorted_ls_pg_desc_arr));
  } else {
    ObBalanceGroupInfo *min_ls = sorted_ls_pg_desc_arr.at(0);
    CHECK_NOT_NULL(min_ls);
    // iterate from the largest ls to the second-smallest ls;
    // need reordered after transfer
    for (int64_t idx = ls_cnt - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_cnt; --idx) {
      ObBalanceGroupInfo *cur_ls = sorted_ls_pg_desc_arr.at(idx);
      CHECK_NOT_NULL(cur_ls);
      if (FAILEDx(get_ls_balance_weight_avg_(sorted_ls_pg_desc_arr, 0/*begin_idx*/, idx, avg))) {
        LOG_WARN("get ls balance weight avg failed", KR(ret), K(idx), K(sorted_ls_pg_desc_arr));
      } else if (OB_FAIL(try_swap_weighted_pg_between_ls_(avg, cur_ls, min_ls, transfer_cnt))) {
        LOG_WARN("try move weighted pg to dest ls failed", KR(ret), K(avg), KPC(cur_ls), KPC(min_ls));
      }
    }
  }
  return ret;
}

int ObPartitionBalance::try_swap_weighted_pg_between_ls_(
    const double avg,
    ObBalanceGroupInfo *src_ls,
    ObBalanceGroupInfo *dest_ls,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> src_weight_arr;
  ObArray<int64_t> dest_weight_arr;
  if (OB_ISNULL(src_ls) || OB_ISNULL(dest_ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ptr", KR(ret), KP(src_ls), KP(dest_ls));
  } else if (OB_FAIL(src_ls->get_balance_weight_array(src_weight_arr))) {
    LOG_WARN("get src balance weight array failed", KR(ret),
        K(avg), KPC(src_ls), KPC(dest_ls), K(transfer_cnt));
  } else if (OB_FAIL(dest_ls->get_balance_weight_array(dest_weight_arr))) {
    LOG_WARN("get dest balance weight array failed", KR(ret),
        K(avg), KPC(src_ls), KPC(dest_ls), K(transfer_cnt));
  } else {
    // from smallest weighted pg to the largest
    lib::ob_sort(src_weight_arr.begin(), src_weight_arr.end());
    // iterate from back to front (largest weight to the smallest)
    for (int64_t idx = src_weight_arr.count() - 1; idx >= 0 && OB_SUCC(ret); --idx) {
      const int64_t left_weight = src_weight_arr.at(idx);
      ObPartGroupInfo *left_pg = nullptr;
      lib::ob_sort(dest_weight_arr.begin(), dest_weight_arr.end());
      ARRAY_FOREACH(dest_weight_arr, j) { // iterate dest ls from smallest to the largest
        const int64_t right_weight = dest_weight_arr.at(j);
        ObPartGroupInfo *right_pg = nullptr;
        if (OB_FAIL(ret)) {
        } else if (left_weight <= right_weight) {
          break; // swap only the large weights on the left and the small weights on the right
        } else {
          const double diff_before = fabs(src_ls->get_part_groups_weight() - avg) + fabs(dest_ls->get_part_groups_weight() - avg);
          const double diff_after = fabs(src_ls->get_part_groups_weight() - left_weight + right_weight - avg)
              + fabs(dest_ls->get_part_groups_weight() + left_weight - right_weight - avg);
          LOG_TRACE("compare diff for trying swap pg between ls for weight balance",
              K(diff_before), K(diff_after), K(avg), K(left_weight), K(right_weight), KPC(src_ls), KPC(dest_ls));
          if (diff_before - diff_after > OB_DOUBLE_EPSINON) {
            if (OB_FAIL(src_ls->transfer_out_by_balance_weight(left_weight, *dest_ls, left_pg))) {
              LOG_WARN("transfer out failed", KR(ret), K(left_pg), K(left_weight));
            } else if (OB_FAIL(dest_ls->transfer_out_by_balance_weight(right_weight, *src_ls, right_pg))) {
              LOG_WARN("transfer out failed", KR(ret), K(right_pg), K(right_weight));
            } else if (OB_FAIL(add_transfer_task_(src_ls->get_ls_id(), dest_ls->get_ls_id(), left_pg))) {
              LOG_WARN("add transfer task failed", KR(ret), KPC(src_ls), KPC(dest_ls));
            } else if (OB_FAIL(add_transfer_task_(dest_ls->get_ls_id(), src_ls->get_ls_id(), right_pg))) {
              LOG_WARN("add transfer task failed", KR(ret), KPC(src_ls), KPC(dest_ls));
            } else {
              src_weight_arr.at(idx) = right_weight;
              dest_weight_arr.at(j) = left_weight;
              ++transfer_cnt;
              LOG_INFO("[PART_BALANCE] swap part group for weight balance", K(diff_before), K(diff_after),
                  K(transfer_cnt), K(avg), KPC(left_pg), KPC(right_pg),
                  "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
              break; // dest_ls should be reordered after transfer
            }
          } else if (REACH_COUNT_INTERVAL(100)) {
             LOG_INFO("[PART_BALANCE] no need to swap part group for weight balance", K(diff_before), K(diff_after),
                  K(transfer_cnt), K(avg), KPC(left_pg), KPC(right_pg), KPC(src_ls), KPC(dest_ls));
          }
        }
      } // end ARRAY_FOREACH dest_weight_arr
    } // end ARRAY_FOREACH src_weight_arr
    LOG_INFO("[PART_BALANCE] try swap weighted part group to dest ls finished",
        KR(ret), K(transfer_cnt), K(avg), "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
  }
  return ret;
}

int ObPartitionBalance::get_ls_balance_weight_avg_(
    const ObArray<ObBalanceGroupInfo *> &ls_pg_desc_arr,
    const int64_t begin_idx,
    const int64_t end_idx,
    double &weight_avg)
{
  int ret = OB_SUCCESS;
  weight_avg = 0;
  if (OB_UNLIKELY(begin_idx < 0
      || end_idx < 0
      || begin_idx >= ls_pg_desc_arr.count()
      || end_idx >= ls_pg_desc_arr.count()
      || begin_idx > end_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(begin_idx), K(end_idx), K(ls_pg_desc_arr));
  } else {
    double sum = 0;
    ARRAY_FOREACH(ls_pg_desc_arr, idx) {
      CHECK_NOT_NULL(ls_pg_desc_arr.at(idx));
      if (OB_SUCC(ret) && idx >= begin_idx && idx <= end_idx) {
        sum += ls_pg_desc_arr.at(idx)->get_part_groups_weight();
      }
    }
    if (OB_SUCC(ret)) {
      weight_avg = sum / (end_idx - begin_idx + 1);
    }
  }
  return ret;
}

bool ObPartitionBalance::is_bg_with_balance_weight_(
    const ObArray<ObBalanceGroupInfo *> &ls_pg_desc_arr)
{
  bool bret = false;
  ARRAY_FOREACH_NORET(ls_pg_desc_arr, i) {
    if (OB_NOT_NULL(ls_pg_desc_arr.at(i))) {
      if (ls_pg_desc_arr.at(i)->get_part_groups_weight() > 0) {
        bret = true;
        break;
      }
    }
  }
  return bret;
}

int ObPartitionBalance::filter_bg_ls_by_primary_zone_(
    const ObArray<ObBalanceGroupInfo *> &bg_ls_array,
    const ObZone &primary_zone,
    ObArray<ObBalanceGroupInfo *> &filtered_bg_ls_array)
{
  int ret = OB_SUCCESS;
  filtered_bg_ls_array.reset();
  if (OB_UNLIKELY(primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone));
  } else {
    ARRAY_FOREACH(bg_ls_array, idx) {
      ObBalanceGroupInfo *bg_info = bg_ls_array.at(idx);
      CHECK_NOT_NULL(bg_info);
      ObLSDesc *ls_desc = nullptr;
      if (FAILEDx(ls_desc_map_.get_refactored(bg_info->get_ls_id(), ls_desc))) {
        LOG_WARN("get ls desc failed", KR(ret), K(bg_info->get_ls_id()));
      } else if (OB_ISNULL(ls_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ls desc", KR(ret), K(bg_info->get_ls_id()));
      } else if (ls_desc->get_primary_zone() != primary_zone) {
        // skip bg ls in other primary zones
      } else if (OB_FAIL(filtered_bg_ls_array.push_back(bg_info))) {
        LOG_WARN("push back failed", KR(ret), K(bg_info));
      }
    }
  }
  return ret;
}

int ObPartitionBalance::process_balance_partition_inner_()
{
  int ret = OB_SUCCESS;
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    const ObBalanceGroup &bg = iter->first;
    int64_t part_group_sum = 0;
    ObArray<ObBalanceGroupInfo *> bg_ls_array;
    if (OB_UNLIKELY(is_bg_with_balance_weight_(iter->second))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("weighted pg in bg_map", KR(ret), "bg", iter->first, "bg_ls_array", iter->second);
    } else if (iter->first.is_scope_zone()) {
      ObScopeZoneBGStatInfo *stat_info = scope_zone_bg_stat_map_.get(bg);
      CHECK_NOT_NULL(stat_info);
      if (FAILEDx(filter_bg_ls_by_primary_zone_(iter->second, stat_info->get_zone(), bg_ls_array))) {
        LOG_WARN("filter bg ls by primary zone failed", KR(ret), K(bg), K(stat_info->get_zone()));
      }
    } else if (OB_FAIL(bg_ls_array.assign(iter->second))) {
      LOG_WARN("assign failed", KR(ret), K(iter->second));
    }

    int64_t ls_cnt = bg_ls_array.count();
    for (int64_t ls_idx = 0; ls_idx < ls_cnt && OB_SUCC(ret); ls_idx++) {
      part_group_sum += bg_ls_array.at(ls_idx)->get_part_groups_count();
    }

    // for bg without weight
    while (OB_SUCC(ret)) {
      lib::ob_sort(bg_ls_array.begin(), bg_ls_array.end(), ObBalanceGroupInfo::cnt_cmp);
      ObBalanceGroupInfo *ls_max = bg_ls_array.at(ls_cnt - 1);
      ObBalanceGroupInfo *ls_min = bg_ls_array.at(0);
      //If difference in number of partition groups between LS does not exceed 1, this balance group is balanced
      if (bg_ls_array.at(ls_cnt - 1)->get_part_groups_count() - bg_ls_array.at(0)->get_part_groups_count() <= 1) {
        // balance group has done
        break;
      }
      /* example1:          ls1  ls2  ls3  ls4  ls5
       * part_group_count:  3    4    4    5    5
       * we should find ls4 -> ls1
       *
       * example2:          ls1  ls2  ls3  ls4  ls5
       * part_group_count:  3    3    4    4    5
       * we should find ls5 -> ls2
       *
       * example3:          ls1  ls2  ls3  ls4  ls5
       * part_group_count:  3    3    3    3    5
       * we should find ls5 -> ls4
       */

      ObBalanceGroupInfo *ls_more = nullptr;
      int64_t ls_more_dest = 0;
      ObBalanceGroupInfo *ls_less = nullptr;
      int64_t ls_less_dest = 0;
      for (int64_t ls_idx = bg_ls_array.count() - 1; ls_idx >= 0; ls_idx--) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
            ? part_group_sum / ls_cnt
            : part_group_sum / ls_cnt + 1;
        if (bg_ls_array.at(ls_idx)->get_part_groups_count() > part_dest) {
          ls_more = bg_ls_array.at(ls_idx);
          ls_more_dest = part_dest;
          break;
        }
      }
      for (int64_t ls_idx = 0; ls_idx < bg_ls_array.count(); ls_idx++) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
            ? part_group_sum / ls_cnt
            : part_group_sum / ls_cnt + 1;
        if (bg_ls_array.at(ls_idx)->get_part_groups_count() < part_dest) {
          ls_less = bg_ls_array.at(ls_idx);
          ls_less_dest = part_dest;
          break;
        }
      }
      if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not found dest ls", KR(ret));
        break;
      }
      int64_t transfer_cnt = MIN(ls_more->get_part_groups_count() - ls_more_dest,
                                ls_less_dest - ls_less->get_part_groups_count());
      for (int64_t i = 0; OB_SUCC(ret) && i < transfer_cnt; i++) {
        ObPartGroupInfo *part_group = nullptr;
        if (OB_FAIL(ls_more->transfer_out_by_round_robin(*ls_less, part_group))) {
          LOG_WARN("failed to transfer partition from ls_more to ls_less", KR(ret),
                  KPC(ls_more), KPC(ls_less));
        } else if (OB_ISNULL(part_group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid part_group", KR(ret), KP(part_group));
        } else if (OB_FAIL(add_transfer_task_(
            ls_more->get_ls_id(),
            ls_less->get_ls_id(),
            part_group))) {
          LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()),
                  K(ls_less->get_ls_id()), KPC(part_group));
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_inner end", K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::add_transfer_task_(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    ObPartGroupInfo *part_group,
    bool modify_ls_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part group is null", KR(ret), K(src_ls_id), K(dest_ls_id), KP(part_group));
  } else {
    ARRAY_FOREACH(part_group->get_part_list(), idx) {
      const ObTransferPartInfo &part_info = part_group->get_part_list().at(idx);
      if (OB_FAIL(job_generator_.add_need_transfer_part(src_ls_id, dest_ls_id, part_info))) {
        LOG_WARN("add need transfer part failed", KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info));
      }
    }
    if (OB_FAIL(ret) || !modify_ls_desc) {
    } else if (OB_FAIL(update_ls_desc_(
        src_ls_id,
        -1,
        part_group->get_data_size() * -1,
        part_group->get_weight() * -1))) {
      LOG_WARN("update_ls_desc", KR(ret), K(src_ls_id), KPC(part_group));
    } else if (OB_FAIL(update_ls_desc_(
        dest_ls_id,
        1,
        part_group->get_data_size(),
        part_group->get_weight()))) {
      LOG_WARN("update_ls_desc", KR(ret), K(dest_ls_id), KPC(part_group));
    }
  }
  return ret;
}

int ObPartitionBalance::update_ls_desc_(
    const ObLSID &ls_id,
    const int64_t cnt,
    const int64_t size,
    const int64_t balance_weight)
{
  int ret = OB_SUCCESS;

  ObLSDesc *ls_desc = nullptr;
  if (OB_FAIL(ls_desc_map_.get_refactored(ls_id, ls_desc))) {
    LOG_WARN("get_refactored", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found ls", KR(ret), K(ls_id));
  } else {
    ls_desc->add_partgroup(cnt, size, balance_weight);
  }
  return ret;
}

// Helper: balance ZONE scope BGs within each zone separately.
// ZONE BGs only distribute within their own zone, so we group LSes by primary_zone
// and call the balance function for each zone group independently.
// Shared by process_balance_partition_{extend,disk}_.
template <ObPartitionBalance::BalanceWithinLSDescArrayFunc balance_func>
int ObPartitionBalance::balance_scope_zone_bg_within_each_zone_()
{
  int ret = OB_SUCCESS;
  constexpr const char *balance_type =
      (balance_func == &ObPartitionBalance::balance_partition_extend_within_ls_desc_array_) ? "extend" :
      (balance_func == &ObPartitionBalance::balance_partition_disk_within_ls_desc_array_)   ? "disk"   :
                                                                                              "unknown";
  if (scope_zone_bg_stat_map_.empty()) {
    // No Zone scope BGs, skip
  } else if (OB_FAIL(prepare_ls_desc_(ObBalanceGroup::BG_SCOPE_ZONE))) {
    LOG_WARN("prepare ls desc failed", KR(ret));
  } else {
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), ObLSDesc::compare_by_primary_zone);
    ObArray<ObLSDesc *> zone_ls_desc_array;
    ObZone prev_zone = ObZone();
    ARRAY_FOREACH(ls_desc_array_, idx) {
      ObLSDesc *ls_desc = ls_desc_array_.at(idx);
      CHECK_NOT_NULL(ls_desc);
      if (OB_FAIL(ret)) {
      } else if (idx != 0 && ls_desc->get_primary_zone() != prev_zone) {
        if (OB_FAIL((this->*balance_func)(ObBalanceGroup::BG_SCOPE_ZONE, zone_ls_desc_array))) {
          LOG_WARN("balance partition within ls desc array failed",
              KR(ret), "balance_type", balance_type, K(zone_ls_desc_array));
        }
        zone_ls_desc_array.reset();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(zone_ls_desc_array.push_back(ls_desc))) {
        LOG_WARN("push back failed", KR(ret));
      } else {
        prev_zone = ls_desc->get_primary_zone();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL((this->*balance_func)(ObBalanceGroup::BG_SCOPE_ZONE, zone_ls_desc_array))) {
      LOG_WARN("balance partition within ls desc array failed",
          KR(ret), "balance_type", balance_type, K(zone_ls_desc_array));
    }
  }
  return ret;
}

int ObPartitionBalance::process_balance_partition_extend_()
{
  int ret = OB_SUCCESS;
  // STEP 1: ZONE scope BGs extend balance within each zone
  if (OB_FAIL(balance_scope_zone_bg_within_each_zone_<
      &ObPartitionBalance::balance_partition_extend_within_ls_desc_array_>())) {
    LOG_WARN("balance scope zone within each zone failed", KR(ret));
  }
  // STEP 2: CLUSTER scope BGs extend balance
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_ls_desc_(ObBalanceGroup::BG_SCOPE_CLUSTER))) {
    LOG_WARN("prepare ls desc failed", KR(ret));
  } else if (OB_FAIL(balance_partition_extend_within_ls_desc_array_(
      ObBalanceGroup::BG_SCOPE_CLUSTER, ls_desc_array_))) {
    LOG_WARN("balance partition extend within ls desc array failed", KR(ret));
  }
  // STEP 3: reset ls desc array to final consistent state
  if (FAILEDx(prepare_ls_desc_())) {
    LOG_WARN("prepare ls desc failed", KR(ret));
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_extend end", KR(ret), K(tenant_id_));
  return ret;
}

// balance partition extend within ls desc array of determined scope
//     example: ls1  ls2  ls3  ls4
//     bg1      3    3    3    4
//     bg2      5    5    5    6
// we should move ls4.bg1 -> ls3 or ls4.bg2 -> ls3
//
// ATTENTION:
// - ls_desc_array must be a subset of ls_desc_array_
// - do not support intergroup balance for weighted part_group now
int ObPartitionBalance::balance_partition_extend_within_ls_desc_array_(
    const ObBalanceGroup::Scope scope,
    ObArray<ObLSDesc *> &ls_desc_array)
{
  int ret = OB_SUCCESS;
  int64_t ls_cnt = ls_desc_array.count();
  int64_t part_group_sum = 0;
  if (OB_UNLIKELY(ls_cnt == 0 || scope == ObBalanceGroup::BG_SCOPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(scope), K(ls_desc_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_cnt; i++) {
      CHECK_NOT_NULL(ls_desc_array.at(i));
      if (OB_SUCC(ret)) {
        part_group_sum += ls_desc_array.at(i)->get_unweighted_partgroup_cnt();
      }
    }
  }
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array.begin(), ls_desc_array.end(), ObLSDesc::less_unweighted_pg_cnt);
    ObLSDesc *ls_max = ls_desc_array.at(ls_cnt - 1);
    ObLSDesc *ls_min = ls_desc_array.at(0);
    if ((ls_max->get_unweighted_partgroup_cnt() - ls_min->get_unweighted_partgroup_cnt() <= 1)) {
      break;
    } else if (REACH_COUNT_INTERVAL(1000)) {
      LOG_INFO("process balance partition extend", K(ls_cnt), K(part_group_sum), KPC(ls_max), KPC(ls_min));
    }
    ObLSDesc *ls_more_desc = nullptr;
    int64_t ls_more_dest = 0;
    ObLSDesc *ls_less_desc = nullptr;
    int64_t ls_less_dest = 0;
    for (int64_t ls_idx = ls_desc_array.count() -1; ls_idx >= 0; ls_idx--) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt)) ? part_group_sum / ls_cnt : part_group_sum / ls_cnt + 1;
      if (ls_desc_array.at(ls_idx)->get_unweighted_partgroup_cnt() > part_dest) {
        ls_more_desc = ls_desc_array.at(ls_idx);
        ls_more_dest = part_dest;
        break;
      }
    }
    for (int64_t ls_idx = 0; ls_idx < ls_desc_array.count(); ls_idx++) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt)) ? part_group_sum / ls_cnt : part_group_sum / ls_cnt + 1;
      if (ls_desc_array.at(ls_idx)->get_unweighted_partgroup_cnt() < part_dest) {
        ls_less_desc = ls_desc_array.at(ls_idx);
        ls_less_dest = part_dest;
        break;
      }
    }
    if (OB_ISNULL(ls_more_desc) || OB_ISNULL(ls_less_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found dest ls", KR(ret), K(ls_more_desc), K(ls_less_desc));
      break;
    }
    int64_t transfer_cnt = MIN(ls_more_desc->get_unweighted_partgroup_cnt() - ls_more_dest, ls_less_dest - ls_less_desc->get_unweighted_partgroup_cnt());
    for (int64_t transfer_idx = 0; OB_SUCC(ret) && transfer_idx < transfer_cnt; transfer_idx++) {
      FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
        if (OB_UNLIKELY(is_bg_with_balance_weight_(iter->second))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("weighted pg in bg_map", KR(ret), "bg", iter->first);
        } else if (iter->first.scope() != scope) {
          continue;
        }

        // find ls_more/ls_less
        ObBalanceGroupInfo *ls_more = nullptr;
        ObBalanceGroupInfo *ls_less = nullptr;
        ARRAY_FOREACH_X(iter->second, idx, cnt, OB_SUCC(ret) && (nullptr == ls_more || nullptr == ls_less)) {
          ObBalanceGroupInfo *bg_info = iter->second.at(idx);
          CHECK_NOT_NULL(bg_info);
          if (OB_FAIL(ret)) {
          } else if (bg_info->get_ls_id() == ls_more_desc->get_ls_id()) {
            ls_more = bg_info;
          } else if (bg_info->get_ls_id() == ls_less_desc->get_ls_id()) {
            ls_less = bg_info;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not found ls", KR(ret));
        } else if (ls_more->get_part_groups_count() > ls_less->get_part_groups_count()) {
          ObPartGroupInfo *part_group = NULL;
          if (OB_FAIL(ls_more->transfer_out_by_round_robin(*ls_less, part_group))) {
            LOG_WARN("failed to transfer partition from ls_more to ls_less", KR(ret), KPC(ls_more), KPC(ls_less));
          } else if (OB_ISNULL(part_group)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid part_group", KR(ret), KP(part_group));
          } else if (OB_FAIL(add_transfer_task_(
              ls_more->get_ls_id(),
              ls_less->get_ls_id(),
              part_group))) {
            LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()),
                    K(ls_less->get_ls_id()), KPC(part_group));
          }
          break;
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] balance partition extend within ls desc array end", KR(ret), K(tenant_id_), K(scope), K(ls_desc_array));
  return ret;
}

// Disk balance uses a different strategy from extend balance in STEP 2:
//   extend uses prepare_ls_desc_(CLUSTER) — scope-isolated view;
//   disk uses prepare_ls_desc_() (ALL BGs) — total disk view,
//   so CLUSTER BGs can globally compensate ZONE-BG-induced disk imbalance.
int ObPartitionBalance::process_balance_partition_disk_()
{
  int ret = OB_SUCCESS;
  // STEP 1: ZONE scope BGs disk balance within each zone
  if (OB_FAIL(balance_scope_zone_bg_within_each_zone_<
      &ObPartitionBalance::balance_partition_disk_within_ls_desc_array_>())) {
    LOG_WARN("balance scope zone within each zone failed", KR(ret));
  }
  // STEP 2: CLUSTER scope BGs disk balance across all LSes.
  // prepare_ls_desc_() uses ALL BGs (no scope filter) so that max/min LSes are
  // determined by total disk usage, but only CLUSTER BGs are swapped.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_ls_desc_())) {
    LOG_WARN("prepare ls desc failed", KR(ret));
  } else if (OB_FAIL(balance_partition_disk_within_ls_desc_array_(
      ObBalanceGroup::BG_SCOPE_CLUSTER, ls_desc_array_))) {
    LOG_WARN("balance partition disk within ls desc array failed", KR(ret));
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_disk end", KR(ret), K(tenant_id_));
  return ret;
}

int ObPartitionBalance::balance_partition_disk_within_ls_desc_array_(
    const ObBalanceGroup::Scope scope,
    ObArray<ObLSDesc *> &ls_desc_array)
{
  int ret = OB_SUCCESS;
  int64_t ls_cnt = ls_desc_array.count();
  if (OB_UNLIKELY(ls_cnt == 0 || scope == ObBalanceGroup::BG_SCOPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(scope), K(ls_desc_array));
  }
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array.begin(), ls_desc_array.end(), ObLSDesc::less_data_size);
    ObLSDesc *ls_max = ls_desc_array.at(ls_cnt - 1);
    ObLSDesc *ls_min = ls_desc_array.at(0);
    CHECK_NOT_NULL(ls_max);
    CHECK_NOT_NULL(ls_min);
    if (OB_FAIL(ret)) {
    } else if (!check_ls_need_swap_(ls_max->get_data_size(), ls_min->get_data_size())) {
      // disk has balance
      break;
    } else {
      LOG_INFO("[PART_BALANCE] disk_balance", KPC(ls_max), KPC(ls_min));
    }

    /*
     * select swap part_group by size segment
     */
    int64_t swap_cnt = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < sizeof(PART_GROUP_SIZE_SEGMENT) / sizeof(PART_GROUP_SIZE_SEGMENT[0]); idx++) {
      if (OB_FAIL(try_swap_part_group_(scope, *ls_max, *ls_min, PART_GROUP_SIZE_SEGMENT[idx], swap_cnt))) {
        LOG_WARN("try_swap_part_group fail", KR(ret), KPC(ls_max), KPC(ls_min), K(PART_GROUP_SIZE_SEGMENT[idx]));
      } else if (swap_cnt > 0) {
        break;
      }
    }
    if (swap_cnt == 0) {
      // nothing to do
      break;
    }
  }
  LOG_INFO("[PART_BALANCE] balance partition disk within ls desc array end", KR(ret), K(tenant_id_), K(scope), K(ls_desc_array));
  return ret;
}

bool ObPartitionBalance::check_ls_need_swap_(int64_t ls_more_size, int64_t ls_less_size)
{
  bool need_swap = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    SHARE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tenant config is invalid", K(tenant_id_));
  } else if (ls_more_size > tenant_config->_partition_balance_disk_threshold) {
    if ((ls_more_size - ls_more_size / 100 * GCONF.balancer_tolerance_percentage) > ls_less_size) {
      need_swap = true;
    }
  }
  return need_swap;
}

int ObPartitionBalance::try_swap_part_group_(
    const ObBalanceGroup::Scope scope,
    ObLSDesc &src_ls,
    ObLSDesc &dest_ls,
    int64_t part_group_min_size,
    int64_t &swap_cnt)
{
  int ret = OB_SUCCESS;
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    if (!check_ls_need_swap_(src_ls.get_data_size(), dest_ls.get_data_size())) {
      break;
    } else if (iter->first.scope() != scope) {
      continue;
    } else if (OB_FAIL(try_swap_part_group_in_bg_(iter, src_ls, dest_ls, part_group_min_size, swap_cnt))) {
      LOG_WARN("try swap part group in bg failed", KR(ret),
          K(src_ls), K(dest_ls), K(part_group_min_size), K(swap_cnt));
    }
  }
  FOREACH_X(iter, weighted_bg_map_, OB_SUCC(ret)) {
    if (!check_ls_need_swap_(src_ls.get_data_size(), dest_ls.get_data_size())) {
      break;
    } else if (iter->first.scope() != scope) {
      continue;
    } else if (OB_FAIL(try_swap_part_group_in_bg_(iter, src_ls, dest_ls, part_group_min_size, swap_cnt))) {
      LOG_WARN("try swap part group in bg failed", KR(ret),
          K(src_ls), K(dest_ls), K(part_group_min_size), K(swap_cnt));
    }
  }
  return ret;
}

int ObPartitionBalance::try_swap_part_group_in_bg_(
    ObBalanceGroupMap::iterator &iter,
    ObLSDesc &src_ls,
    ObLSDesc &dest_ls,
    int64_t part_group_min_size,
    int64_t &swap_cnt)
{
  int ret = OB_SUCCESS;
  // find ls_more/ls_less
  ObBalanceGroupInfo *ls_more = nullptr;
  ObBalanceGroupInfo *ls_less = nullptr;
  ObPartGroupInfo *largest_pg = nullptr;
  ObPartGroupInfo *smallest_pg = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < iter->second.count(); idx++) {
    if (iter->second.at(idx)->get_ls_id() == src_ls.get_ls_id()) {
      ls_more = iter->second.at(idx);
    } else if (iter->second.at(idx)->get_ls_id() == dest_ls.get_ls_id()) {
      ls_less = iter->second.at(idx);
    }
    if (ls_more != nullptr && ls_less != nullptr) {
      break;
    }
  }
  if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found ls", KR(ret), K(ls_more), K(ls_less));
  } else if (OB_UNLIKELY(!check_ls_need_swap_(src_ls.get_data_size(), dest_ls.get_data_size()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls do not need swap", KR(ret), K(src_ls), K(dest_ls));
  } else  if (ls_more->get_part_groups_count() == 0 || ls_less->get_part_groups_count() == 0) {
    // do nothing
  } else {
    int64_t swap_size = 0;
    int64_t ls_diff_size = src_ls.get_data_size() - dest_ls.get_data_size();
    if (OB_FAIL(ls_more->get_largest_part_group(largest_pg))) {
      LOG_WARN("failed to get the largest pg", KR(ret), KPC(ls_more));
    } else if (OB_FAIL(ls_less->get_smallest_part_group(smallest_pg))) {
      LOG_WARN("failed to get the smallest pg", KR(ret), KPC(ls_less));
    } else if (OB_ISNULL(largest_pg) || OB_ISNULL(smallest_pg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr", KR(ret), KP(largest_pg), KP(smallest_pg));
    } else if (FALSE_IT(swap_size = largest_pg->get_data_size() - smallest_pg->get_data_size())) {
    } else if (swap_size >= part_group_min_size
        && ls_diff_size > swap_size
        && largest_pg->get_weight() == smallest_pg->get_weight()) {
      LOG_INFO("[PART_BALANCE] swap_partition", KPC(largest_pg), KPC(smallest_pg),
          K(src_ls), K(dest_ls), K(swap_size), K(ls_diff_size), K(part_group_min_size));
      if (OB_FAIL(add_transfer_task_(
          ls_more->get_ls_id(),
          ls_less->get_ls_id(),
          largest_pg))) {
        LOG_WARN("add transfer task failed", KR(ret), KPC(ls_more), KPC(ls_less), KPC(largest_pg));
      } else if (OB_FAIL(add_transfer_task_(
          ls_less->get_ls_id(),
          ls_more->get_ls_id(),
          smallest_pg))) {
        LOG_WARN("add transfer task failed", KR(ret), KPC(ls_more), KPC(ls_less), KPC(smallest_pg));
      } else if (OB_FAIL(ls_more->swap_largest_for_smallest_pg(*ls_less))) {
        LOG_WARN("transfer out by data size largest pg failed",
            KR(ret), KPC(largest_pg), KPC(smallest_pg), KPC(ls_more), KPC(ls_less));
      } else {
        swap_cnt++;
      }
    }
  }
  return ret;
}

int ObPartitionBalance::save_balance_group_stat_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t trans_timeout = OB_INVALID_TIMESTAMP;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (!is_user_tenant(tenant_id_)) {
    // do nothing
  } else if (FALSE_IT(trans_timeout = GCONF.internal_sql_execute_timeout + bg_map_.size() * 100_ms)) {
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, trans_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret), K(trans_timeout));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(bg_ls_stat_operator_.delete_balance_group_ls_stat(ctx.get_timeout(), trans, tenant_id_))) {
    LOG_WARN("fail to delete balance group ls stat", KR(ret), K(tenant_id_));
  } else {
    // iterator all balance group
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
      // iterator all ls
      for (int64_t i = 0; OB_SUCC(ret) && i < iter->second.count(); i++) {
        if (OB_NOT_NULL(iter->second.at(i))) {
          ObBalanceGroupLSStat bg_ls_stat;
          if (OB_FAIL(bg_ls_stat.build(tenant_id_,
                                       iter->first.id(),
                                       iter->second.at(i)->get_ls_id(),
                                       iter->second.at(i)->get_part_groups_count(),
                                       iter->first.name()))) {
            LOG_WARN("fail to build bg ls stat", KR(ret), K(iter->first), KPC(iter->second.at(i)));
          } else if (OB_FAIL(bg_ls_stat_array.push_back(bg_ls_stat))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(bg_ls_stat_operator_.insert_update_balance_group_ls_stat(ctx.get_timeout(), trans, tenant_id_,
                iter->first.id(), bg_ls_stat_array))) {
          LOG_WARN("fail to insert update balance group ls stat", KR(ret), K(tenant_id_), K(iter->first));
        }
      }
    }
    // commit/abort
    int tmp_ret = OB_SUCCESS;
    const bool is_commit = OB_SUCC(ret);
    if (OB_SUCCESS != (tmp_ret = trans.end(is_commit))) {
      LOG_WARN("trans end failed", K(tmp_ret), K(is_commit));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("[PART_BALANCE] save_balance_group_stat", K(ret), "cost", end_time - start_time);
  return ret;
}

} // end rootserver
} // end oceanbase
