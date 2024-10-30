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
    TaskMode task_mode,
    const ObPartDistributionMode &part_distribution_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionBalance has inited", KR(ret), K(this));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)
      || (task_mode != GEN_BG_STAT && task_mode != GEN_TRANSFER_TASK)
      || part_distribution_mode <= ObPartDistributionMode::INVALID
      || part_distribution_mode >= ObPartDistributionMode::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObPartitionBalance run", KR(ret), K(tenant_id), K(schema_service), K(sql_proxy),
            K(task_mode), K(part_distribution_mode));
  } else if (OB_FAIL(bg_builder_.init(tenant_id, "PART_BALANCE", *this, *sql_proxy, *schema_service))) {
    LOG_WARN("init all balance group builder fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(bg_map_.create(40960, lib::ObLabel("PART_BALANCE"), ObModIds::OB_HASH_NODE, tenant_id))) {
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

    //reset
    allocator_.reset();
    ls_desc_array_.reset();
    task_mode_ = task_mode;
    part_distribution_mode_ = part_distribution_mode;
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
  FOREACH(iter, bg_map_) {
    ObArray<ObBalanceGroupInfo*> &bg_ls_arr = iter->second;
    ARRAY_FOREACH_NORET(bg_ls_arr, i) {
      ObBalanceGroupInfo* bg_info = bg_ls_arr.at(i);
      if (OB_NOT_NULL(bg_info)) {
        bg_info->~ObBalanceGroupInfo();
        bg_info = NULL;
      }
    }
    bg_ls_arr.destroy();
  }
  //reset
  job_generator_.reset();
  bg_builder_.destroy();
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.destroy();
  bg_map_.destroy();
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dup_ls_id_.reset();
  sql_proxy_ = NULL;
  task_mode_ = GEN_BG_STAT;
  part_distribution_mode_ = ObPartDistributionMode::INVALID;
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
  LOG_INFO("PART_BALANCE process", KR(ret), K(tenant_id_), K(task_mode_), K(job_id), K(timeout),
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
    const bool in_new_partition_group,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  ObBalanceGroup bg = bg_in; // get a copy
  ObLSDesc *src_ls_desc = nullptr;
  ObTransferPartInfo part_info;
  const bool is_dup_ls_related_part = dup_ls_id_.is_valid() && (src_ls_id == dup_ls_id_ || dest_ls_id == dup_ls_id_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!bg_in.is_valid() || !table_schema.is_valid()
            || !is_valid_id(part_object_id) || !is_valid_id(part_group_uid)
            || !src_ls_id.is_valid_with_tenant(tenant_id_)
            || !dest_ls_id.is_valid_with_tenant(tenant_id_)
            || tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_in), K(table_schema), K(part_object_id),
            K(src_ls_id), K(dest_ls_id), K(tablet_size), K(part_group_uid));
  } else if (OB_FAIL(part_info.init(table_schema.get_table_id(), part_object_id))) {
    LOG_WARN("part_info init fail", KR(ret), K(table_schema.get_table_id()), K(part_object_id));
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
  } else if (OB_FAIL(add_part_to_bg_map_(src_ls_id, bg, table_schema, part_group_uid,
                                        part_info, tablet_size))) {
    LOG_WARN("add new partition group to balance group failed", KR(ret), K(src_ls_id), K(bg),
            K(table_schema), K(part_group_uid), K(part_info), K(tablet_size));
  } else if (in_new_partition_group && FALSE_IT(src_ls_desc->add_partgroup(1, 0))) {
  } else {
    src_ls_desc->add_data_size(tablet_size);
  }
  return ret;
}

int ObPartitionBalance::add_part_to_bg_map_(
    const ObLSID &ls_id,
    ObBalanceGroup &bg,
    const ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    const ObTransferPartInfo &part_info,
    const int64_t tablet_size)
{
  int ret = OB_SUCCESS;
  ObArray<ObBalanceGroupInfo *> *bg_ls_array = NULL;
  bg_ls_array = bg_map_.get(bg);
  if (OB_ISNULL(bg_ls_array)) {
    ObArray<ObBalanceGroupInfo *> bg_ls_array_obj;
    if (OB_FAIL(bg_map_.set_refactored(bg, bg_ls_array_obj))) {
      LOG_WARN("init part to balance group fail", KR(ret), K(ls_id), K(bg));
    } else {
      bg_ls_array = bg_map_.get(bg);
      if (OB_ISNULL(bg_ls_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get part from balance group fail", KR(ret), K(ls_id), K(bg));
      } else if (FALSE_IT(bg_ls_array->set_block_allocator(
          ModulePageAllocator(allocator_, "BGLSArray")))) {
      } else if (OB_FAIL(bg_ls_array->reserve(ls_desc_array_.count()))) {
        LOG_WARN("fail to reserve", KR(ret), K(bg_ls_array));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_desc_array_.count(); i++) {
          ObBalanceGroupInfo *bg_info = nullptr;
          if (OB_ISNULL(bg_info = reinterpret_cast<ObBalanceGroupInfo *>(
              allocator_.alloc(sizeof(ObBalanceGroupInfo))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_ISNULL(bg_info = new(bg_info) ObBalanceGroupInfo(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_FAIL(bg_info->init(bg.id(), ls_desc_array_.at(i)->get_ls_id(),
                                          ls_desc_array_.count(), part_distribution_mode_))) {
            LOG_WARN("failed to init bg_info", KR(ret), K(bg_info));
          } else if (OB_FAIL(bg_ls_array->push_back(bg_info))) {
            LOG_WARN("push_back fail", KR(ret), K(ls_id), K(bg));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(bg_ls_array)) {
    bool find_ls = false;
    ARRAY_FOREACH(*bg_ls_array, idx) {
      ObBalanceGroupInfo *bg_info = bg_ls_array->at(idx);
      if (OB_ISNULL(bg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bg_info is null", KR(ret), K(bg_info), KPC(bg_ls_array), K(idx));
      } else if (bg_info->get_ls_id() == ls_id) {
        if (OB_FAIL(bg_info->append_part(table_schema, part_group_uid, part_info, tablet_size))) {
          LOG_WARN("append_part failed", KR(ret), K(bg_ls_array), K(idx), K(part_info),
                  K(tablet_size), K(part_group_uid), K(table_schema));
        }
        find_ls = true;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!find_ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found LS", KR(ret), K(ls_id), K(part_info), K(bg));
    }
  }

  return ret;
}

int ObPartitionBalance::process_balance_partition_inner_()
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *pg_info = nullptr;
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    int64_t part_group_sum = 0;
    ObArray<ObBalanceGroupInfo *> &bg_ls_array = iter->second;
    int64_t ls_cnt = bg_ls_array.count();
    for (int64_t ls_idx = 0; ls_idx < ls_cnt; ls_idx++) {
      part_group_sum += bg_ls_array.at(ls_idx)->get_part_group_count();
    }
    while (OB_SUCC(ret)) {
      lib::ob_sort(bg_ls_array.begin(), bg_ls_array.end(),
                  [] (ObBalanceGroupInfo* left, ObBalanceGroupInfo* right) {
        if (left->get_part_group_count() < right->get_part_group_count()) {
          return true;
        } else if (left->get_part_group_count() == right->get_part_group_count()) {
          if (left->get_ls_id() > right->get_ls_id()) {
            return true;
          }
        }
        return false;
      });
      ObBalanceGroupInfo *ls_max = bg_ls_array.at(ls_cnt - 1);
      ObBalanceGroupInfo *ls_min = bg_ls_array.at(0);
      //If difference in number of partition groups between LS does not exceed 1, this balance group is balanced
      if (ls_max->get_part_group_count() - ls_min->get_part_group_count() <= 1) {
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
            : part_group_sum / ls_cnt + 1;;
        if (bg_ls_array.at(ls_idx)->get_part_group_count() > part_dest) {
          ls_more = bg_ls_array.at(ls_idx);
          ls_more_dest = part_dest;
          break;
        }
      }
      for (int64_t ls_idx = 0; ls_idx < bg_ls_array.count(); ls_idx++) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
            ? part_group_sum / ls_cnt
            : part_group_sum / ls_cnt + 1;;
        if (bg_ls_array.at(ls_idx)->get_part_group_count() < part_dest) {
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
      int64_t transfer_cnt = MIN(ls_more->get_part_group_count() - ls_more_dest,
                                ls_less_dest - ls_less->get_part_group_count());
      for (int64_t i = 0; OB_SUCC(ret) && i < transfer_cnt; i++) {
        if (OB_FAIL(ls_more->transfer_out(*ls_less, pg_info))) {
          LOG_WARN("failed to transfer partition from ls_more to ls_less", KR(ret),
                  KPC(ls_more), KPC(ls_less));
        } else if (OB_UNLIKELY(nullptr == pg_info
                              || !pg_info->is_valid()
                              || nullptr == pg_info->part_group())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pg_info", KR(ret), KPC(pg_info));
        } else if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(),
                                              ls_less->get_ls_id(), pg_info->part_group()))) {
          LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()),
                  K(ls_less->get_ls_id()), KPC(pg_info));
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
    ObTransferPartGroup *part_group,
    bool modify_ls_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part group is null", KR(ret), KP(part_group));
  } else {
    ARRAY_FOREACH(part_group->get_part_list(), idx) {
      const ObTransferPartInfo &part_info = part_group->get_part_list().at(idx);
      if (OB_FAIL(job_generator_.add_need_transfer_part(src_ls_id, dest_ls_id, part_info))) {
        LOG_WARN("add need transfer part failed", KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (modify_ls_desc && OB_FAIL(update_ls_desc_(src_ls_id, -1, part_group->get_data_size() * -1))) {
      LOG_WARN("update_ls_desc", KR(ret), K(src_ls_id));
    } else if (modify_ls_desc && OB_FAIL(update_ls_desc_(dest_ls_id, 1, part_group->get_data_size()))) {
      LOG_WARN("update_ls_desc", KR(ret), K(dest_ls_id));
    }
  }
  return ret;
}

int ObPartitionBalance::update_ls_desc_(const ObLSID &ls_id, int64_t cnt, int64_t size)
{
  int ret = OB_SUCCESS;

  ObLSDesc *ls_desc = nullptr;
  if (OB_FAIL(ls_desc_map_.get_refactored(ls_id, ls_desc))) {
    LOG_WARN("get_refactored", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found ls", KR(ret), K(ls_id));
  } else {
    ls_desc->add_partgroup(cnt, size);
  }
  return ret;
}

int ObPartitionBalance::process_balance_partition_extend_()
{
  int ret = OB_SUCCESS;

  int64_t ls_cnt = ls_desc_array_.count();
  uint64_t part_group_sum = 0;
  ObIPartGroupInfo *pg_info = nullptr;
  for (int64_t i = 0; i < ls_cnt; i++) {
    part_group_sum += ls_desc_array_.at(i)->get_partgroup_cnt();
  }
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), [] (ObLSDesc *l, ObLSDesc *r) {
      if (l->get_partgroup_cnt() < r->get_partgroup_cnt()) {
        return true;
      } else if (l->get_partgroup_cnt() == r->get_partgroup_cnt()) {
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
    if (ls_max->get_partgroup_cnt() - ls_min->get_partgroup_cnt() <= 1) {
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
    for (int64_t ls_idx = ls_desc_array_.count() - 1; ls_idx >= 0; ls_idx--) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
          ? part_group_sum / ls_cnt
          : part_group_sum / ls_cnt + 1;;
      if (ls_desc_array_.at(ls_idx)->get_partgroup_cnt() > part_dest) {
        ls_more_desc = ls_desc_array_.at(ls_idx);
        ls_more_dest = part_dest;
        break;
      }
    }
    for (int64_t ls_idx = 0; ls_idx < ls_desc_array_.count(); ls_idx++) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
          ? part_group_sum / ls_cnt
          : part_group_sum / ls_cnt + 1;;
      if (ls_desc_array_.at(ls_idx)->get_partgroup_cnt() < part_dest) {
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
    int64_t transfer_cnt = MIN(ls_more_desc->get_partgroup_cnt() - ls_more_dest,
                              ls_less_dest - ls_less_desc->get_partgroup_cnt());
    for (int64_t transfer_idx = 0; OB_SUCC(ret) && transfer_idx < transfer_cnt; transfer_idx++) {
      FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
        // find ls_more/ls_less
        ObBalanceGroupInfo *ls_more = nullptr;
        ObBalanceGroupInfo *ls_less = nullptr;
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
        if (ls_more->get_part_group_count() > ls_less->get_part_group_count()) {
          ObTransferPartGroup *part_group = NULL;
          if (OB_FAIL(ls_more->transfer_out(*ls_less, pg_info))) {
            LOG_WARN("failed to transfer partition from ls_more to ls_less", KR(ret),
                    KPC(ls_more), KPC(ls_less));
          } else if (OB_UNLIKELY(nullptr == pg_info
                                || !pg_info->is_valid()
                                || nullptr == pg_info->part_group())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid pg_info", KR(ret), KPC(pg_info));
          } else if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(),
                                                ls_less->get_ls_id(), pg_info->part_group()))) {
            LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()),
                    K(ls_less->get_ls_id()), KPC(pg_info));
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

int ObPartitionBalance::try_swap_part_group_(
    ObLSDesc &src_ls,
    ObLSDesc &dest_ls,
    int64_t part_group_min_size,
    int64_t &swap_cnt)
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *largest_pg = nullptr;
  ObIPartGroupInfo *smallest_pg = nullptr;
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    if (!check_ls_need_swap_(src_ls.get_data_size(), dest_ls.get_data_size())) {
      break;
    }
    // find ls_more/ls_less
    ObBalanceGroupInfo *ls_more = nullptr;
    ObBalanceGroupInfo *ls_less = nullptr;
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
      break;
    }
    if (ls_more->get_part_group_count() == 0 || ls_less->get_part_group_count() == 0) {
      continue;
    }
    int64_t swap_size = 0;
    if (OB_FAIL(ls_more->get_largest_part_group(largest_pg))) {
      LOG_WARN("failed to get the largest part group", KR(ret), KPC(ls_more));
    } else if (OB_FAIL(ls_less->get_smallest_part_group(smallest_pg))) {
      LOG_WARN("failed to get the smallest part group", KR(ret), KPC(ls_less));
    } else if (OB_UNLIKELY(nullptr == largest_pg || nullptr == smallest_pg
                          || !largest_pg->is_valid() || !smallest_pg->is_valid()
                          || nullptr == largest_pg->part_group()
                          || nullptr == smallest_pg->part_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("largest or smallest part group info is invalid", KR(ret), KPC(largest_pg),
              KPC(smallest_pg));
    } else if (FALSE_IT(swap_size = largest_pg->part_group()->get_data_size() -
                                    smallest_pg->part_group()->get_data_size())) { // empty
    } else if (swap_size >= part_group_min_size
              && src_ls.get_data_size() - dest_ls.get_data_size() > swap_size) {
      LOG_INFO("[PART_BALANCE] swap_partition", KPC(largest_pg->part_group()),
              KPC(smallest_pg->part_group()), K(src_ls), K(dest_ls),
              K(swap_size), K(part_group_min_size));
      if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(), ls_less->get_ls_id(),
                                    largest_pg->part_group()))) {
        LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
      } else if (OB_FAIL(ls_less->append_part_group(*largest_pg))) {
        LOG_WARN("push_back fail", KR(ret), K(ls_less), K(largest_pg));
      } else if (OB_FAIL(ls_more->remove_part_group(*largest_pg))) {
        LOG_WARN("parts remove", KR(ret), K(ls_more), K(largest_pg));
      } else if (OB_FAIL(add_transfer_task_(ls_less->get_ls_id(), ls_more->get_ls_id(),
                                            smallest_pg->part_group()))) {
        LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
      } else if (OB_FAIL(ls_more->append_part_group(*smallest_pg))) {
        LOG_WARN("push_back fail", KR(ret), K(ls_more));
      } else if (OB_FAIL(ls_less->remove_part_group(*smallest_pg))) {
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
  } else if (FALSE_IT(trans_timeout = GCONF.internal_sql_execute_timeout + bg_map_.size() * 100_ms)) {
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, trans_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret), K(trans_timeout));
  } else if (!is_user_tenant(tenant_id_)) {
    // do nothing
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
                                       iter->second.at(i)->get_part_group_count(),
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
