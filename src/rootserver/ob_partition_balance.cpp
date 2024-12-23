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
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_struct.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_partition_balance.h"
#include "rootserver/ob_tenant_balance_service.h" // ObTenantBalanceService
#include "observer/ob_server_struct.h"//GCTX

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

int ObPartitionBalance::init(
    uint64_t tenant_id,
    schema::ObMultiVersionSchemaService *schema_service,
    common::ObMySQLProxy *sql_proxy,
    const int64_t primary_zone_num,
    const int64_t unit_group_num,
    TaskMode task_mode)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionBalance has inited", KR(ret), K(this));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)
      || (task_mode != GEN_BG_STAT && task_mode != GEN_TRANSFER_TASK)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObPartitionBalance run", KR(ret), K(tenant_id), K(schema_service), K(sql_proxy), K(task_mode));
  } else if (OB_FAIL(bg_builder_.init(tenant_id, "PART_BALANCE", *this, *sql_proxy, *schema_service))) {
    LOG_WARN("init all balance group builder fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(bg_map_.create(40960, lib::ObLabel("PART_BALANCE"), ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(weighted_bg_map_.create(128, lib::ObLabel("WeightedBG"), ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(ls_desc_map_.create(10, lib::ObLabel("PART_BALANCE"), ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(bg_ls_stat_operator_.init(sql_proxy))) {
    LOG_WARN("init balance group ls stat operator fail", KR(ret));
  } else if (OB_FAIL(job_generator_.init(tenant_id, primary_zone_num, unit_group_num, sql_proxy))) {
    LOG_WARN("init job generator failed", KR(ret), K(tenant_id),
        K(primary_zone_num), K(unit_group_num), KP(sql_proxy));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    cur_part_group_ = NULL;
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
        iter->second.at(i)->~ObLSPartGroupDesc();
      }
    }
    iter->second.destroy();
  }
  FOREACH(iter, bg_map_) {
    for (int64_t i = 0; i < iter->second.count(); i++) {
      if (OB_NOT_NULL(iter->second.at(i))) {
        iter->second.at(i)->~ObLSPartGroupDesc();
      }
    }
    iter->second.destroy();
  }
  //reset
  job_generator_.reset();
  bg_builder_.destroy();
  cur_part_group_ = NULL;
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.destroy();
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
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_FAIL(prepare_balance_group_())) {
    LOG_WARN("prepare_balance_group fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(save_balance_group_stat_())) {
    LOG_WARN("save balance group stat fail", KR(ret), K(tenant_id_));
  } else if (GEN_BG_STAT == task_mode_) {
    // finish
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_ATTR_ALIGN;
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
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id_));
    } else if (data_version < DATA_VERSION_4_2_4_0) {
      balance_strategy = ObBalanceStrategy::PB_COMPAT_OLD;
    }
    if (FAILEDx(job_generator_.gen_balance_job_and_tasks(job_type, balance_strategy, job_id, timeout))) {
      LOG_WARN("gen balance job and tasks failed", KR(ret),
          K(tenant_id_), K(job_type), K(balance_strategy), K(job_id), K(timeout));
    }
  }

  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("PART_BALANCE process", KR(ret), K(tenant_id_), K(task_mode_), K(job_id), K(timeout), K(balance_strategy),
      "cost", end_time - start_time, "need balance", job_generator_.need_gen_job());
  return ret;
}

int ObPartitionBalance::prepare_ls_()
{
  int ret = OB_SUCCESS;
  ObLSStatusInfoArray ls_stat_array;
  if (!inited_ || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(inited_), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantBalanceService::gather_ls_status_stat(tenant_id_, ls_stat_array))) {
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
      } else if (FALSE_IT(new(ls_desc) ObLSDesc(ls_stat.get_ls_id(), ls_stat.get_ls_group_id()))) {
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
  cur_part_group_ = NULL;
  if (!inited_) {
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
  } else if (FALSE_IT(cur_part_group_ = NULL)) {
  } else if (OB_FAIL(split_out_weighted_bg_map_())) {
    LOG_WARN("split out weighted bg map failed", KR(ret), K(tenant_id_));
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
        ObLSPartGroupDesc *ls_pg_desc = iter->second.at(i);
        CHECK_NOT_NULL(ls_pg_desc);
        if (OB_SUCC(ret)) {
          const ObLSID &ls_id = ls_pg_desc->get_ls_id();
          // iterate from back to avoid remove_part_group() changing the index
          for (int64_t j = ls_pg_desc->get_part_groups().count() - 1; j >= 0 && OB_SUCC(ret); --j) {
            const ObTransferPartGroup *pg = ls_pg_desc->get_part_groups().at(j);
            ObTransferPartGroup *new_pg = NULL;
            CHECK_NOT_NULL(pg);
            if (OB_FAIL(ret)) {
            } else if (0 == pg->get_weight()) {
              // not weighted pg, skip
            } else if (OB_FAIL(add_new_pg_to_bg_map_(ls_id, bg, new_pg, weighted_bg_map_))) {
              LOG_WARN("add new partition group to balance group failed",
                  KR(ret), K(ls_id), K(bg), K(new_pg));
            } else if (OB_ISNULL(new_pg)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null ptr", KR(ret), KP(new_pg), KP(pg), K(bg));
            } else if (OB_FAIL(new_pg->assign(*pg))) {
              LOG_WARN("assign failed", KR(ret), K(pg));
            } else if (OB_FAIL(ls_pg_desc->remove_part_group(j))) {
              LOG_WARN("remove part group failed", KR(ret), KPC(pg));
            }
          } // end for ls_pg_desc->get_part_groups()
        }
      } // end ARRAY_FOREACH iter->second
    }
    LOG_INFO("[PART_BALANCE] split out weighted_bg_map", KR(ret), K(tenant_id_),
        "bg_map size", bg_map_.size(), "weighted_bg_map size", weighted_bg_map_.size());
  }
  return ret;
}

int ObPartitionBalance::on_new_partition(
    const ObBalanceGroup &bg_in,
    const ObObjectID table_id,
    const ObObjectID part_object_id,
    const ObTabletID tablet_id,
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const int64_t tablet_size,
    const bool in_new_partition_group,
    const uint64_t part_group_uid,
    const int64_t balance_weight)
{
  UNUSEDx(tablet_id, part_group_uid);
  int ret = OB_SUCCESS;
  ObBalanceGroup bg = bg_in; // get a copy
  ObLSDesc *src_ls_desc = nullptr;
  ObTransferPartInfo part_info;
  const bool is_dup_ls_related_part = dup_ls_id_.is_valid() && (src_ls_id == dup_ls_id_ || dest_ls_id == dup_ls_id_);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not inited", KR(ret), K(inited_));
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
  } else if (OB_FAIL(ls_desc_map_.get_refactored(src_ls_id, src_ls_desc))) {
    LOG_WARN("get LS desc fail", KR(ret), K(src_ls_id));
  } else if (OB_ISNULL(src_ls_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found LS", KR(ret), K(src_ls_id), KPC(src_ls_desc));
  } else {
    // create partition group if in new partition group
    if (in_new_partition_group) {
      ObTransferPartGroup *new_pg = NULL;
      if (OB_FAIL(add_new_pg_to_bg_map_(src_ls_id, bg, new_pg, bg_map_))) {
        LOG_WARN("add new partition group to balance group failed", KR(ret), K(src_ls_id), K(bg), K(new_pg));
      } else if (OB_ISNULL(new_pg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new_pg is null", KR(ret), K(src_ls_id), K(bg), K(new_pg));
      } else {
        // add new partition group
        src_ls_desc->add_partgroup(1, 0, balance_weight);
        cur_part_group_ = new_pg;
      }
    }
    // if not in new partition group, current part group should be valid
    else if (OB_ISNULL(cur_part_group_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current part group is null when not in new partition group", K(cur_part_group_),
          K(in_new_partition_group), K(bg));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cur_part_group_->add_part(part_info, tablet_size, balance_weight))) {
      LOG_WARN("partition group add part fail", KR(ret), K(part_info), K(tablet_size), K(balance_weight));
    } else {
      // partition group not changed, data size need update
      src_ls_desc->add_data_size(tablet_size);
    }
  }
  return ret;
}

int ObPartitionBalance::add_new_pg_to_bg_map_(
    const ObLSID &ls_id,
    ObBalanceGroup &bg,
    ObTransferPartGroup *&part_group,
    ObBalanceGroupMap &bg_map)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSPartGroupDesc *> *ls_part_desc_arr = NULL;
  ls_part_desc_arr = bg_map.get(bg);
  if (OB_ISNULL(ls_part_desc_arr)) {
    ObArray<ObLSPartGroupDesc *> ls_part_desc_arr_obj;
    if (OB_FAIL(bg_map.set_refactored(bg, ls_part_desc_arr_obj))) {
      LOG_WARN("init part to balance group fail", KR(ret), K(ls_id), K(bg));
    } else {
      ls_part_desc_arr = bg_map.get(bg);
      if (OB_ISNULL(ls_part_desc_arr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get part from balance group fail", KR(ret), K(ls_id), K(bg));
      } else {
        ls_part_desc_arr->set_block_allocator(ModulePageAllocator(allocator_, "LSPartDescArr"));
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_desc_array_.count(); i++) {
          ObLSPartGroupDesc *ls_part_desc = nullptr;
          if (OB_ISNULL(ls_part_desc = reinterpret_cast<ObLSPartGroupDesc*>(allocator_.alloc(sizeof(ObLSPartGroupDesc))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret), K(ls_id), K(bg));
          } else if (FALSE_IT(new(ls_part_desc) ObLSPartGroupDesc(ls_desc_array_.at(i)->get_ls_id(), allocator_))) {
          } else if (OB_FAIL(ls_part_desc_arr->push_back(ls_part_desc))) {
            LOG_WARN("push_back fail", KR(ret), K(ls_id), K(bg));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && ls_part_desc_arr != nullptr) {
    bool find_ls = false;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_part_desc_arr->count(); idx++) {
      if (ls_part_desc_arr->at(idx)->get_ls_id() == ls_id) {
        if (OB_FAIL(ls_part_desc_arr->at(idx)->add_new_part_group(part_group))) {
          LOG_WARN("add_new_part_group failed", KR(ret), K(ls_id), K(idx), KPC(ls_part_desc_arr), K(part_group));
        }
        find_ls = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !find_ls) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found LS", KR(ret), K(ls_id), K(part_group), K(bg));
    }
  }

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
  } else if (data_version < DATA_VERSION_4_2_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support version", KR(ret), K(tenant_id_), K(data_version));
  } else {
    FOREACH_X(iter, weighted_bg_map_, OB_SUCC(ret)) {
      ObArray<ObLSPartGroupDesc *> &ls_pg_desc_arr = iter->second;
      ObArray<ObLSPartGroupDesc *> weighted_ls_pg_desc_arr;
      int64_t transfer_cnt = 0;
      if (OB_UNLIKELY(!is_bg_with_balance_weight_(ls_pg_desc_arr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("weighted bg has no weight", KR(ret), "bg", iter->first, K(ls_pg_desc_arr));
      } else {
        do {
          transfer_cnt = 0;
          // from smallest weight to the largest
          lib::ob_sort(ls_pg_desc_arr.begin(), ls_pg_desc_arr.end(), ObLSPartGroupDesc::weight_cmp);
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
    ObArray<ObLSPartGroupDesc *> &sorted_ls_pg_desc_arr,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  transfer_cnt = 0;
  double avg = 0;
  bool need_move = false;
  const int64_t ls_cnt = sorted_ls_pg_desc_arr.count();
  if (OB_UNLIKELY(sorted_ls_pg_desc_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sorted_ls_pg_desc_arr", KR(ret), K(sorted_ls_pg_desc_arr));
  } else {
    ObLSPartGroupDesc *min_ls = sorted_ls_pg_desc_arr.at(0);
    CHECK_NOT_NULL(min_ls);
    // iterate from the largest ls to the second-smallest ls;
    // need reordered after transfer
    for (int64_t idx = ls_cnt - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_cnt; --idx) {
      ObLSPartGroupDesc *cur_ls = sorted_ls_pg_desc_arr.at(idx);
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
    ObLSPartGroupDesc *src_ls,
    ObLSPartGroupDesc *dest_ls,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_ls) || OB_ISNULL(dest_ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ptr", KR(ret), KP(src_ls), KP(dest_ls), K(avg));
  } else {
    // from smallest weighted pg to the largest
    lib::ob_sort(src_ls->get_part_groups().begin(), src_ls->get_part_groups().end(), ObTransferPartGroup::weight_cmp);
    //To avoid remove() changing the idx, iterate from back to front
    for (int64_t idx = src_ls->get_part_groups().count() - 1; idx >= 0 && OB_SUCC(ret); --idx) {
      ObTransferPartGroup *curr_pg = src_ls->get_part_groups().at(idx);
      CHECK_NOT_NULL(curr_pg);
      if (OB_FAIL(ret)) {
      } else if (0 == curr_pg->get_weight()) {
        break;
      } else {
        const double diff_before = fabs(src_ls->get_part_groups_weight() - avg) + fabs(dest_ls->get_part_groups_weight() - avg);
        const double diff_after = fabs(src_ls->get_part_groups_weight() - curr_pg->get_weight() - avg)
            + fabs(dest_ls->get_part_groups_weight() + curr_pg->get_weight() - avg);
        LOG_TRACE("compare diff for try move pg between ls for weight balance",
            K(diff_before), K(diff_after), K(avg), KPC(curr_pg), KPC(src_ls), KPC(dest_ls));
        if (diff_before - diff_after > OB_DOUBLE_EPSINON) {
          if (OB_FAIL(add_transfer_task_(src_ls->get_ls_id(), dest_ls->get_ls_id(), curr_pg))) {
            LOG_WARN("add transfer task failed", KR(ret), KPC(src_ls), KPC(dest_ls));
          } else if (OB_FAIL(dest_ls->get_part_groups().push_back(curr_pg))) {
            LOG_WARN("push back failed", KR(ret), KPC(curr_pg), KPC(dest_ls));
          } else if (OB_FAIL(src_ls->get_part_groups().remove(idx))) {
            LOG_WARN("remove failed", KR(ret), K(idx), KPC(src_ls));
          } else {
            ++transfer_cnt;
            LOG_INFO("[PART_BALANCE] move part group for weight balance", K(diff_before), K(diff_after),
                K(transfer_cnt), K(avg), KPC(curr_pg), "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
          }
        } else if (REACH_COUNT_INTERVAL(100)) {
            LOG_INFO("[PART_BALANCE] no need to move part group for weight balance", K(diff_before), K(diff_after),
                K(transfer_cnt), K(avg), KPC(curr_pg), KPC(src_ls), KPC(dest_ls));
        }
      }
    }
    LOG_INFO("[PART_BALANCE] try move weighted part group to dest ls finished",
        KR(ret), K(transfer_cnt), K(avg), "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
  }
  return ret;
}

int ObPartitionBalance::try_swap_weighted_part_group_(
    ObArray<ObLSPartGroupDesc *> &sorted_ls_pg_desc_arr,
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
    ObLSPartGroupDesc *min_ls = sorted_ls_pg_desc_arr.at(0);
    CHECK_NOT_NULL(min_ls);
    // iterate from the largest ls to the second-smallest ls;
    // need reordered after transfer
    for (int64_t idx = ls_cnt - 1; idx > 0 && OB_SUCC(ret) && 0 == transfer_cnt; --idx) {
      ObLSPartGroupDesc *cur_ls = sorted_ls_pg_desc_arr.at(idx);
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
    ObLSPartGroupDesc *src_ls,
    ObLSPartGroupDesc *dest_ls,
    int64_t &transfer_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_ls) || OB_ISNULL(dest_ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ptr", KR(ret), KP(src_ls), KP(dest_ls));
  } else {
    // from smallest weighted pg to the largest
    lib::ob_sort(src_ls->get_part_groups().begin(), src_ls->get_part_groups().end(), ObTransferPartGroup::weight_cmp);
    //To avoid remove() changing the idx, iterate from back to front
    for (int64_t i = src_ls->get_part_groups().count() - 1; i >= 0 && OB_SUCC(ret); --i) {
      ObTransferPartGroup *left_pg = src_ls->get_part_groups().at(i);
      CHECK_NOT_NULL(left_pg);
      if (OB_FAIL(ret)) {
      } else if (0 == left_pg->get_weight()) {
        break;
      }
      lib::ob_sort(dest_ls->get_part_groups().begin(), dest_ls->get_part_groups().end(), ObTransferPartGroup::weight_cmp);
      ARRAY_FOREACH(dest_ls->get_part_groups(), j) { // iterate dest ls from smallest to the largest
        ObTransferPartGroup *right_pg = dest_ls->get_part_groups().at(j);
        CHECK_NOT_NULL(right_pg);
        if (OB_FAIL(ret)) {
        } else if (left_pg->get_weight() <= right_pg->get_weight()) {
          continue;
        } else if (0 == right_pg->get_weight()) {
          break;
        } else {
          const double diff_before = fabs(src_ls->get_part_groups_weight() - avg) + fabs(dest_ls->get_part_groups_weight() - avg);
          const double diff_after = fabs(dest_ls->get_part_groups_weight() - left_pg->get_weight() + right_pg->get_weight() - avg)
              + fabs(dest_ls->get_part_groups_weight() + left_pg->get_weight() - right_pg->get_weight() - avg);
          LOG_TRACE("compare diff for trying swap pg between ls for weight balance",
              K(diff_before), K(diff_after), K(avg), KPC(left_pg), KPC(right_pg), KPC(src_ls), KPC(dest_ls));
          if (diff_before - diff_after > OB_DOUBLE_EPSINON) {
            if (OB_FAIL(add_transfer_task_(src_ls->get_ls_id(), dest_ls->get_ls_id(), left_pg))) {
              LOG_WARN("add transfer task failed", KR(ret), KPC(src_ls), KPC(dest_ls));
            } else if (OB_FAIL(add_transfer_task_(dest_ls->get_ls_id(), src_ls->get_ls_id(), right_pg))) {
              LOG_WARN("add transfer task failed", KR(ret), KPC(src_ls), KPC(dest_ls));
            } else if (OB_FAIL(src_ls->get_part_groups().push_back(right_pg))) {
              LOG_WARN("push back failed", KR(ret), KPC(right_pg), KPC(dest_ls));
            } else if (OB_FAIL(dest_ls->get_part_groups().push_back(left_pg))) {
              LOG_WARN("push back failed", KR(ret), KPC(left_pg), KPC(dest_ls));
            } else if (OB_FAIL(src_ls->get_part_groups().remove(i))) {
              LOG_WARN("remove failed", KR(ret), K(i), KPC(src_ls));
            } else if (OB_FAIL(dest_ls->get_part_groups().remove(j))) {
              LOG_WARN("remove failed", KR(ret), K(j), KPC(dest_ls));
            } else {
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
      } // end ARRAY_FOREACH dest_ls
    } // end ARRAY_FOREACH src_ls
    LOG_INFO("[PART_BALANCE] try swap weighted part group to dest ls finished",
        KR(ret), K(transfer_cnt), K(avg), "src_ls", src_ls->get_ls_id(), "dest_ls", dest_ls->get_ls_id());
  }
  return ret;
}

int ObPartitionBalance::get_ls_balance_weight_avg_(
    const ObArray<ObLSPartGroupDesc *> &ls_pg_desc_arr,
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
    const ObArray<ObLSPartGroupDesc *> &ls_pg_desc_arr)
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

int ObPartitionBalance::process_balance_partition_inner_()
{
  int ret = OB_SUCCESS;

  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    int64_t part_group_sum = 0;
    ObArray<ObLSPartGroupDesc*> &ls_part_desc_arr = iter->second;
    if (OB_UNLIKELY(is_bg_with_balance_weight_(ls_part_desc_arr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("weighted pg in bg_map", KR(ret), "bg", iter->first, K(ls_part_desc_arr));
    }

    int64_t ls_cnt = ls_part_desc_arr.count();
    for (int64_t ls_idx = 0; ls_idx < ls_cnt && OB_SUCC(ret); ls_idx++) {
      part_group_sum += ls_part_desc_arr.at(ls_idx)->get_part_groups().count();
    }

    // for bg without weight
    while (OB_SUCC(ret)) {
      lib::ob_sort(ls_part_desc_arr.begin(), ls_part_desc_arr.end(), ObLSPartGroupDesc::cnt_cmp);
      ObLSPartGroupDesc *ls_max = ls_part_desc_arr.at(ls_cnt - 1);
      ObLSPartGroupDesc *ls_min = ls_part_desc_arr.at(0);
      //If difference in number of partition groups between LS does not exceed 1, this balance group is balanced
      if (ls_part_desc_arr.at(ls_cnt - 1)->get_part_groups().count() - ls_part_desc_arr.at(0)->get_part_groups().count() <= 1) {
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

      ObLSPartGroupDesc *ls_more = nullptr;
      int64_t ls_more_dest = 0;
      ObLSPartGroupDesc *ls_less = nullptr;
      int64_t ls_less_dest = 0;
      for (int64_t ls_idx = ls_part_desc_arr.count() - 1; ls_idx >= 0; ls_idx--) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt)) ? part_group_sum / ls_cnt : part_group_sum / ls_cnt + 1;
        if (ls_part_desc_arr.at(ls_idx)->get_part_groups().count() > part_dest) {
          ls_more = ls_part_desc_arr.at(ls_idx);
          ls_more_dest = part_dest;
          break;
        }
      }
      for (int64_t ls_idx = 0; ls_idx < ls_part_desc_arr.count(); ls_idx++) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt)) ? part_group_sum / ls_cnt : part_group_sum / ls_cnt + 1;
        if (ls_part_desc_arr.at(ls_idx)->get_part_groups().count() < part_dest) {
          ls_less = ls_part_desc_arr.at(ls_idx);
          ls_less_dest = part_dest;
          break;
        }
      }
      if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not found dest ls", KR(ret));
        break;
      }
      int64_t transfer_cnt = MIN(ls_more->get_part_groups().count() - ls_more_dest, ls_less_dest - ls_less->get_part_groups().count());
      for (int64_t i = 0; OB_SUCC(ret) && i < transfer_cnt; i++) {
        if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(), ls_less->get_ls_id(), ls_more->get_part_groups().at(ls_more->get_part_groups().count() - 1)))) {
          LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
        } else if (OB_FAIL(ls_less->get_part_groups().push_back(ls_more->get_part_groups().at(ls_more->get_part_groups().count() - 1)))) {
          LOG_WARN("push_back fail", KR(ret));
        } else if (OB_FAIL(ls_more->get_part_groups().remove(ls_more->get_part_groups().count() - 1))) {
          LOG_WARN("parts remove", KR(ret));
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_inner end", K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::add_transfer_task_(const ObLSID &src_ls_id, const ObLSID &dest_ls_id, ObTransferPartGroup *part_group, bool modify_ls_desc)
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

// do not support intergroup balance for weighted part_group now
int ObPartitionBalance::process_balance_partition_extend_()
{
  int ret = OB_SUCCESS;

  int64_t ls_cnt = ls_desc_array_.count();
  uint64_t part_group_sum = 0;
  for (int64_t i = 0; i < ls_cnt; i++) {
    part_group_sum += ls_desc_array_.at(i)->get_unweighted_partgroup_cnt();
  }
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), [] (ObLSDesc *l, ObLSDesc *r) {
      if (l->get_unweighted_partgroup_cnt() < r->get_unweighted_partgroup_cnt()) {
        return true;
      } else if (l->get_unweighted_partgroup_cnt() == r->get_unweighted_partgroup_cnt()) {
        if (l->get_data_size() < r->get_data_size()) {
          return true;
        } else if (l->get_data_size() == r->get_data_size()) {
          if (l->get_ls_id() > r->get_ls_id()) {
            return true;
          }
        }
      }
      return false;
    });

    ObLSDesc *ls_max = ls_desc_array_.at(ls_cnt - 1);
    ObLSDesc *ls_min = ls_desc_array_.at(0);
    if ((ls_max->get_unweighted_partgroup_cnt() - ls_min->get_unweighted_partgroup_cnt() <= 1)) {
      break;
    }
    /*
     * example: ls1  ls2  ls3  ls4
     * bg1      3    3    3    4
     * bg2      5    5    5    6
     *
     * we should move ls4.bg1 -> ls3 or ls4.bg2 -> ls3
     */
    ObLSDesc *ls_more_desc = nullptr;
    int64_t ls_more_dest = 0;
    ObLSDesc *ls_less_desc = nullptr;
    int64_t ls_less_dest = 0;
    for (int64_t ls_idx = ls_desc_array_.count() -1; ls_idx >= 0; ls_idx--) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt)) ? part_group_sum / ls_cnt : part_group_sum / ls_cnt + 1;;
      if (ls_desc_array_.at(ls_idx)->get_unweighted_partgroup_cnt() > part_dest) {
        ls_more_desc = ls_desc_array_.at(ls_idx);
        ls_more_dest = part_dest;
        break;
      }
    }
    for (int64_t ls_idx = 0; ls_idx < ls_desc_array_.count(); ls_idx++) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt)) ? part_group_sum / ls_cnt : part_group_sum / ls_cnt + 1;;
      if (ls_desc_array_.at(ls_idx)->get_unweighted_partgroup_cnt() < part_dest) {
        ls_less_desc = ls_desc_array_.at(ls_idx);
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
        }

        // find ls_more/ls_less
        ObLSPartGroupDesc *ls_more = nullptr;
        ObLSPartGroupDesc *ls_less = nullptr;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < iter->second.count(); idx++) {
          if (iter->second.at(idx)->get_ls_id() == ls_more_desc->get_ls_id()) {
            ls_more = iter->second.at(idx);
          } else if (iter->second.at(idx)->get_ls_id() == ls_less_desc->get_ls_id()) {
            ls_less = iter->second.at(idx);
          }
          if (ls_more != nullptr && ls_less != nullptr) {
            break;
          }
        }
        if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not found ls", KR(ret));
          break;
        }
        if (ls_more->get_part_groups().count() > ls_less->get_part_groups().count()) {
          if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(), ls_less->get_ls_id(), ls_more->get_part_groups().at(ls_more->get_part_groups().count() - 1)))) {
            LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()), KPC(ls_more), KPC(ls_less));
          } else if (OB_FAIL(ls_less->get_part_groups().push_back(ls_more->get_part_groups().at(ls_more->get_part_groups().count() - 1)))) {
            LOG_WARN("push_back fail", KR(ret));
          } else if (OB_FAIL(ls_more->get_part_groups().remove(ls_more->get_part_groups().count() - 1))) {
            LOG_WARN("parts remove", KR(ret));
          }
          break;
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_extend end", K(ret), K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::process_balance_partition_disk_()
{
  int ret = OB_SUCCESS;

  int64_t ls_cnt = ls_desc_array_.count();
  int64_t part_size_sum = 0;
  for (int64_t i = 0; i < ls_cnt; i++) {
    part_size_sum += ls_desc_array_.at(i)->get_data_size();
  }
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), [] (ObLSDesc *l, ObLSDesc *r) {
      if (l->get_data_size() < r->get_data_size()) {
        return true;
      } else if (l->get_data_size() == r->get_data_size()) {
        if (l->get_ls_id() > r->get_ls_id()) {
          return true;
        }
      }
      return false;
    });

    ObLSDesc &ls_max = *ls_desc_array_.at(ls_cnt - 1);
    ObLSDesc &ls_min = *ls_desc_array_.at(0);
    if (!check_ls_need_swap_(ls_max.get_data_size(), ls_min.get_data_size())) {
      // disk has balance
      break;
    }
    LOG_INFO("[PART_BALANCE] disk_balance", K(ls_max), K(ls_min));

    /*
     * select swap part_group by size segment
     */
    int64_t swap_cnt = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < sizeof(PART_GROUP_SIZE_SEGMENT) / sizeof(PART_GROUP_SIZE_SEGMENT[0]); idx++) {
      if (OB_FAIL(try_swap_part_group_(ls_max, ls_min, PART_GROUP_SIZE_SEGMENT[idx], swap_cnt))) {
        LOG_WARN("try_swap_part_group fail", KR(ret), K(ls_max), K(ls_min), K(PART_GROUP_SIZE_SEGMENT[idx]));
      } else if (swap_cnt > 0) {
        break;
      }
    }
    if (swap_cnt == 0) {
      // nothing to do
      break;
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_disk end", KR(ret), K(tenant_id_), K(ls_desc_array_));
  return ret;
}

bool ObPartitionBalance::check_ls_need_swap_(uint64_t ls_more_size, uint64_t ls_less_size)
{
  bool need_swap = false;
  if (ls_more_size > PART_BALANCE_THRESHOLD_SIZE) {
    if ((ls_more_size - ls_more_size * GCONF.balancer_tolerance_percentage / 100) > ls_less_size) {
      need_swap = true;
    }
  }
  return need_swap;
}

int ObPartitionBalance::try_swap_part_group_(ObLSDesc &src_ls, ObLSDesc &dest_ls, int64_t part_group_min_size, int64_t &swap_cnt)
{
  int ret = OB_SUCCESS;
  if (!check_ls_need_swap_(src_ls.get_data_size(), dest_ls.get_data_size())) {
    // do nothing
  } else {
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      if (OB_FAIL(try_swap_part_group_in_bg_(iter, src_ls, dest_ls, part_group_min_size, swap_cnt))) {
        LOG_WARN("try swap part group in bg failed", KR(ret),
            K(src_ls), K(dest_ls), K(part_group_min_size), K(swap_cnt));
      }
    }
    FOREACH_X(iter, weighted_bg_map_, OB_SUCC(ret)) {
      if (OB_FAIL(try_swap_part_group_in_bg_(iter, src_ls, dest_ls, part_group_min_size, swap_cnt))) {
        LOG_WARN("try swap part group in bg failed", KR(ret),
            K(src_ls), K(dest_ls), K(part_group_min_size), K(swap_cnt));
      }
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
  ObLSPartGroupDesc *ls_more = nullptr;
  ObLSPartGroupDesc *ls_less = nullptr;
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
  } else  if (ls_more->get_part_groups().count() == 0 || ls_less->get_part_groups().count() == 0) {
    // do nothing
  } else {
    lib::ob_sort(ls_more->get_part_groups().begin(), ls_more->get_part_groups().end(), [] (ObTransferPartGroup *l, ObTransferPartGroup *r) {
      return l->get_data_size() < r->get_data_size();
    });
    lib::ob_sort(ls_less->get_part_groups().begin(), ls_less->get_part_groups().end(), [] (ObTransferPartGroup *l, ObTransferPartGroup *r) {
      return l->get_data_size() < r->get_data_size();
    });

    ObTransferPartGroup *src_part_group = ls_more->get_part_groups().at(ls_more->get_part_groups().count() -1);
    ObTransferPartGroup *dest_part_group = ls_less->get_part_groups().at(0);
    int64_t swap_size = src_part_group->get_data_size() - dest_part_group->get_data_size();
    if ((swap_size >= part_group_min_size)
        && (src_ls.get_data_size() - dest_ls.get_data_size() > swap_size)
        && (src_part_group->get_weight() == dest_part_group->get_weight())) {
      LOG_INFO("[PART_BALANCE] swap_partition", K(*src_part_group), K(*dest_part_group), K(src_ls), K(dest_ls), K(swap_size), K(part_group_min_size));
      if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(), ls_less->get_ls_id(), src_part_group))) {
        LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
      } else if (OB_FAIL(ls_less->get_part_groups().push_back(src_part_group))) {
        LOG_WARN("push_back fail", KR(ret), K(ls_less));
      } else if (OB_FAIL(ls_more->get_part_groups().remove(ls_more->get_part_groups().count() - 1))) {
        LOG_WARN("parts remove", KR(ret), K(ls_more));
      } else if (OB_FAIL(add_transfer_task_(ls_less->get_ls_id(), ls_more->get_ls_id(), ls_less->get_part_groups().at(0)))) {
        LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
      } else if (OB_FAIL(ls_more->get_part_groups().push_back(ls_less->get_part_groups().at(0)))) {
        LOG_WARN("push_back fail", KR(ret), K(ls_more));
      } else if (OB_FAIL(ls_less->get_part_groups().remove(0))) {
        LOG_WARN("parts remove", KR(ret), K(ls_less));
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
                                       iter->second.at(i)->get_part_groups().count(),
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

int ObPartitionBalance::ObLSPartGroupDesc::add_new_part_group(ObTransferPartGroup *&part_group)
{
  int ret = OB_SUCCESS;
  part_group = NULL;
  const int64_t part_group_size = sizeof(ObTransferPartGroup);
  void *buf = alloc_.alloc(part_group_size);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for partition group fail", KR(ret), K(buf), K(part_group_size));
  } else if (OB_ISNULL(part_group = new(buf) ObTransferPartGroup(alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("construct ObTransferPartGroup fail", KR(ret), K(buf), K(part_group_size));
  } else if (OB_FAIL(part_groups_.push_back(part_group))) {
    LOG_WARN("push back new partition group fail", KR(ret), K(part_group), K(part_groups_));
  }
  return ret;
}

int ObPartitionBalance::ObLSPartGroupDesc::remove_part_group(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= part_groups_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", KR(ret), K(idx));
  } else {
    ObTransferPartGroup *pg = part_groups_.at(idx);
    if (OB_FAIL(part_groups_.remove(idx))) {
      LOG_WARN("remove failed", KR(ret), K(idx));
    } else if (OB_NOT_NULL(pg)) {
      pg->~ObTransferPartGroup();
      alloc_.free(pg);
      pg = NULL;
    }
  }
  return ret;
}

} // end rootserver
} // end oceanbase
