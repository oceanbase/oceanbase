/**
 * Copyright (c) 2023 OceanBase
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

#include "rootserver/balance/ob_partition_balance_helper.h"
#include "storage/ob_common_id_utils.h" // ObCommonIDUtils
#include "rootserver/ob_ls_service_helper.h" // ObLSServiceHelper
#include "rootserver/ob_ls_balance_helper.h" // ObLSBalanceTaskHelper

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;

namespace rootserver
{
#define PB_INFO(fmt, args...) LOG_INFO("[PARTITION_BALANCE] " fmt, ##args)

ObPartTransferJobGenerator::ObPartTransferJobGenerator()
    : inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      primary_zone_num_(OB_INVALID_COUNT),
      unit_group_num_(OB_INVALID_COUNT),
      sql_proxy_(NULL),
      balance_job_(),
      balance_tasks_(),
      dup_ls_ids_(),
      ls_group_id_map_(),
      dup_to_normal_part_map_(),
      normal_to_dup_part_map_(),
      dup_to_dup_part_map_(),
      normal_to_normal_part_map_()
{
}

int ObPartTransferJobGenerator::init(
    const uint64_t tenant_id,
    const int64_t primary_zone_num,
    const int64_t unit_group_num,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t HASH_MAP_SIZE = 128;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartTransferJobGenerator init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || primary_zone_num < 1
      || unit_group_num < 1)
      || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(primary_zone_num),
        K(unit_group_num), KP(sql_proxy));
  } else if (OB_FAIL(ls_group_id_map_.create(
      HASH_MAP_SIZE,
      "PartTransfLSG",
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("ls_group_id_map_ create failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dup_to_normal_part_map_.create(
      HASH_MAP_SIZE,
      "PartTransfDTN",
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("dup_to_normal_part_map_ create failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(normal_to_dup_part_map_.create(
      HASH_MAP_SIZE,
      "PartTransfNTD",
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("normal_to_dup_part_map_ create failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dup_to_dup_part_map_.create(
      HASH_MAP_SIZE,
      "PartTransfDTD",
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("dup_to_dup_part_map_ create failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(normal_to_normal_part_map_.create(
      HASH_MAP_SIZE,
      "PartTransfNTN",
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("normal_to_normal_part_map_ create failed", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    primary_zone_num_ = primary_zone_num;
    unit_group_num_ = unit_group_num;
    sql_proxy_ = sql_proxy;
    balance_job_.reset();
    balance_tasks_.reset();
    inited_ = true;
  }
  return ret;
}

int ObPartTransferJobGenerator::prepare_ls(const share::ObLSStatusInfoIArray &ls_stat_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(ls_stat_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_stat_array", KR(ret), K(ls_stat_array));
  } else if (OB_UNLIKELY(!ls_group_id_map_.empty() || !dup_ls_ids_.empty())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ls_group_id_map_ and dup_ls_ids_ should be empty when prepare_ls", KR(ret),
        "ls_group_id_map_ size", ls_group_id_map_.size(), "dup_ls_ids_ size", dup_ls_ids_.count());
  } else {
    ARRAY_FOREACH(ls_stat_array, idx) {
      const ObLSStatusInfo &ls_stat = ls_stat_array.at(idx);
      if (OB_UNLIKELY(!ls_stat.is_normal())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ls should all be normal when transfer part", KR(ret), K(ls_stat));
      } else if (ls_stat.get_ls_id().is_sys_ls()) {
        // ignore
      } else if (OB_FAIL(ls_group_id_map_.set_refactored(
          ls_stat.get_ls_id(),
          ls_stat.get_ls_group_id()))) {
        LOG_WARN("set_refactored failed", KR(ret), K(ls_stat));
      } else if (ls_stat.is_duplicate_ls()) {
        if (OB_FAIL(dup_ls_ids_.push_back(ls_stat.get_ls_id()))) {
          LOG_WARN("push back failed", KR(ret), K(ls_stat));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(ls_group_id_map_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_group_id_map_ can not be empty", KR(ret), K(tenant_id_), K(ls_stat_array));
    }
  }
  return ret;
}

void ObPartTransferJobGenerator::reset()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  primary_zone_num_ = OB_INVALID_COUNT;
  unit_group_num_ = OB_INVALID_COUNT;
  sql_proxy_ = NULL;
  balance_job_.reset();
  balance_tasks_.reset();
  dup_ls_ids_.reset();
  ls_group_id_map_.reuse();
  dup_to_normal_part_map_.reuse();
  normal_to_dup_part_map_.reuse();
  dup_to_dup_part_map_.reuse();
  normal_to_normal_part_map_.reuse();
}

int ObPartTransferJobGenerator::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_), KP(sql_proxy_));
  } else if (OB_UNLIKELY(ls_group_id_map_.empty())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("need prepare_ls to make ls_group_id_map_ not empty", KR(ret));
  }
  return ret;
}

#define ADD_TO_PART_MAP(target_part_map)                                                                     \
  do {                                                                                                       \
    if (FAILEDx(add_need_transfer_part_(src_ls_id, dest_ls_id, part_info, target_part_map))) {               \
      LOG_WARN("add to part map failed", KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info), K(dup_ls_ids_));\
    }                                                                                                        \
  } while (0)

int ObPartTransferJobGenerator::add_need_transfer_part(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const ObTransferPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!src_ls_id.is_valid()
      || !dest_ls_id.is_valid()
      || !part_info.is_valid()
      || (src_ls_id == dest_ls_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info));
  } else if (common::has_exist_in_array(dup_ls_ids_, src_ls_id)) {
    if (common::has_exist_in_array(dup_ls_ids_, dest_ls_id)) {
      // 1. dup ls -> dup ls
      ADD_TO_PART_MAP(dup_to_dup_part_map_);
    } else {
      // 2. dup ls -> normal ls
      ADD_TO_PART_MAP(dup_to_normal_part_map_);
    }
  } else if (common::has_exist_in_array(dup_ls_ids_, dest_ls_id)) {
    // 3. normal ls -> dup ls
    ADD_TO_PART_MAP(normal_to_dup_part_map_);
  } else {
    // 4. normal ls -> normal ls
    ADD_TO_PART_MAP(normal_to_normal_part_map_);
  }
  return ret;
}

int ObPartTransferJobGenerator::add_need_transfer_part_(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const ObTransferPartInfo &part_info,
    ObTransferPartMap &part_map)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!src_ls_id.is_valid()
      || !dest_ls_id.is_valid()
      || !part_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info));
  } else {
    ObTransferTaskKey task_key(src_ls_id, dest_ls_id);
    ObTransferPartList *part_list_ptr = part_map.get(task_key);
    if (OB_ISNULL(part_list_ptr)) {
      ObTransferPartList part_list;
      if (OB_FAIL(part_map.set_refactored(task_key, part_list))) { // deep copy
        LOG_WARN("set refactored failed", KR(ret), K(task_key), K(part_list));
      } else {
        part_list_ptr = part_map.get(task_key);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(part_list_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get part_list_ptr from map", KR(ret), K(task_key));
    } else if (OB_FAIL(part_list_ptr->push_back(part_info))) {
      LOG_WARN("push back failed", KR(ret), K(part_info));
    }
  }
  return ret;
}

int ObPartTransferJobGenerator::gen_balance_job_and_tasks(
    const ObBalanceJobType &job_type,
    const ObString &balance_strategy)
{
  int ret = OB_SUCCESS;
  balance_job_.reset();
  balance_tasks_.reset();
  ObBalanceJobID job_id;
  ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
  ObString comment;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!job_type.is_valid()
      || balance_strategy.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(job_type), K(balance_strategy));
  } else if (!need_gen_job()) {
    // no partitions need to be transferred
  } else if (OB_FAIL(storage::ObCommonIDUtils::gen_unique_id(tenant_id_, job_id))) {
    LOG_WARN("gen unique id failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(balance_job_.init(
      tenant_id_,
      job_id,
      job_type,
      job_status,
      primary_zone_num_,
      unit_group_num_,
      comment,
      balance_strategy))) {
    LOG_WARN("job init fail", KR(ret), K(tenant_id_), K(job_id), K(job_type), K(job_status),
        K(primary_zone_num_), K(unit_group_num_), K(comment), K(balance_strategy));
  } else {
    if (OB_SUCC(ret) && !dup_to_normal_part_map_.empty()) {
      if (OB_FAIL(gen_transfer_tasks_from_dup_ls_to_normal_ls_())) {
        LOG_WARN("gen transfer tasks from dup ls to normal ls failed", KR(ret), KPC(this));
      }
    }
    if (OB_SUCC(ret) && !normal_to_dup_part_map_.empty()) {
      if (OB_FAIL(gen_transfer_tasks_from_normal_ls_to_dup_ls_())) {
        LOG_WARN("gen transfer tasks from normal ls to dup ls failed", KR(ret), KPC(this));
      }
    }
    if (OB_SUCC(ret) && !dup_to_dup_part_map_.empty()) {
      if (OB_FAIL(gen_transfer_tasks_between_dup_ls_())) {
        LOG_WARN("gen transfer tasks between dup ls failed", KR(ret), KPC(this));
      }
    }
    if (OB_SUCC(ret) && !normal_to_normal_part_map_.empty()) {
      if (OB_FAIL(gen_transfer_tasks_between_normal_ls_())) {
        LOG_WARN("gen transfer tasks between normal ls failed", KR(ret), KPC(this));
      }
    }
  }
  PB_INFO("gen balance job and tasks finished", KR(ret), KPC(this));
  return ret;
}

#define ADD_ALTER_TASK(ls_group_id, src_ls_id)                                           \
  do {                                                                                   \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_alter_task(tenant_id_,                     \
        balance_job_.get_job_id(), ls_group_id, src_ls_id, balance_tasks_))) {           \
      LOG_WARN("add ls alter task failed", KR(ret), K(tenant_id_), K(balance_job_),      \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(balance_tasks_));               \
    }                                                                                    \
  } while (0)

#define ADD_TRANSFER_TASK(ls_group_id, src_ls_id, dest_ls_id, part_list)                           \
  do {                                                                                             \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_transfer_task(tenant_id_, balance_job_.get_job_id(), \
        ls_group_id, src_ls_id, dest_ls_id, part_list, balance_tasks_))) {                         \
      LOG_WARN("add ls transfer task failed", KR(ret), K(tenant_id_), K(balance_job_),             \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(part_list), K(balance_tasks_));           \
    }                                                                                              \
  } while (0)

#define ADD_MERGE_TASK(ls_group_id, src_ls_id, dest_ls_id)                                 \
  do {                                                                                     \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_merge_task(tenant_id_,                       \
        balance_job_.get_job_id(), ls_group_id, src_ls_id, dest_ls_id, balance_tasks_))) { \
      LOG_WARN("add ls merge task failed", KR(ret), K(tenant_id_), K(balance_job_),        \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(balance_tasks_));                 \
    }                                                                                      \
  } while (0)

#define ADD_SPLIT_TASK(ls_group_id, src_ls_id, part_list, dest_ls_id)                                 \
  do {                                                                                                \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_split_task(sql_proxy_, tenant_id_,                      \
        balance_job_.get_job_id(), ls_group_id, src_ls_id, part_list, dest_ls_id, balance_tasks_))) { \
      LOG_WARN("add ls split task failed", KR(ret), K(tenant_id_), K(balance_job_),                   \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(part_list), K(balance_tasks_));              \
    }                                                                                                 \
  } while (0)

// gen tasks by dup_to_normal_part_map_
// alter + transfer + alter
// 1. alter dup_ls to dest ls_group_id
// 2. transfer from dup_ls to normal_ls
// 3. alter ls_group_id of dup_ls to old ls_group_id
int ObPartTransferJobGenerator::gen_transfer_tasks_from_dup_ls_to_normal_ls_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!balance_job_.is_valid()
      || dup_to_normal_part_map_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(balance_job_),
        "dup_to_normal_part_map_ size", dup_to_normal_part_map_.size());
  } else {
    for (ObTransferPartMap::const_iterator iter = dup_to_normal_part_map_.begin();
        OB_SUCC(ret) && iter != dup_to_normal_part_map_.end();
        iter++) {
      const ObLSID &src_ls_id = iter->first.get_src_ls_id();
      const ObLSID &dest_ls_id = iter->first.get_dest_ls_id();
      uint64_t src_ls_group_id = OB_INVALID_ID;
      uint64_t dest_ls_group_id = OB_INVALID_ID;
      if (OB_FAIL(ls_group_id_map_.get_refactored(src_ls_id, src_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(src_ls_id), KPC(this));
      } else if (OB_FAIL(ls_group_id_map_.get_refactored(dest_ls_id, dest_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(dest_ls_id), KPC(this));
      } else if (src_ls_group_id == dest_ls_group_id) {
        ADD_TRANSFER_TASK(dest_ls_group_id, src_ls_id, dest_ls_id, iter->second);
      } else {
        ADD_ALTER_TASK(dest_ls_group_id, src_ls_id);
        ADD_TRANSFER_TASK(dest_ls_group_id, src_ls_id, dest_ls_id, iter->second);
        ADD_ALTER_TASK(src_ls_group_id, src_ls_id);
      }
    }
  }
  return ret;
}

// gen tasks by normal_to_dup_part_map_
// 1. split a new dup ls from normal_ls with valid ls_group_id
// 2. alter original dup ls to dest ls_group_id
// 3. merge new dup ls to original dup ls
// 4. alter ls_group_id of original dup ls to old ls_group_id
int ObPartTransferJobGenerator::gen_transfer_tasks_from_normal_ls_to_dup_ls_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!balance_job_.is_valid()
      || normal_to_dup_part_map_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(balance_job_),
        "normal_to_dup_part_map_ size", normal_to_dup_part_map_.size());
  } else {
    for (ObTransferPartMap::const_iterator iter = normal_to_dup_part_map_.begin();
        OB_SUCC(ret) && iter != normal_to_dup_part_map_.end();
        iter++) {
      const ObLSID &src_ls_id = iter->first.get_src_ls_id();
      const ObLSID &dest_ls_id = iter->first.get_dest_ls_id();
      uint64_t src_ls_group_id = OB_INVALID_ID;
      uint64_t dest_ls_group_id = OB_INVALID_ID;
      if (OB_FAIL(ls_group_id_map_.get_refactored(src_ls_id, src_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(src_ls_id));
      } else if (OB_FAIL(ls_group_id_map_.get_refactored(dest_ls_id, dest_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(dest_ls_id));
      } else {
        ObLSID tmp_ls_id;
        ADD_SPLIT_TASK(0, src_ls_id, iter->second, tmp_ls_id); // ls_group_id = 0 means split a dup ls
        if (src_ls_group_id != dest_ls_group_id) {
          ADD_ALTER_TASK(src_ls_group_id, dest_ls_id); // alter original dup ls is more efficient
        }
        ADD_MERGE_TASK(src_ls_group_id, tmp_ls_id, dest_ls_id);
        if (src_ls_group_id != dest_ls_group_id) {
          ADD_ALTER_TASK(dest_ls_group_id, dest_ls_id); // reset ls_group_id
        }
      }
    }
  }
  return ret;
}

// gen tasks by normal_to_normal_part_map_
// transfer or (split + alter + merge)
int ObPartTransferJobGenerator::gen_transfer_tasks_between_normal_ls_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!balance_job_.is_valid() || normal_to_normal_part_map_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(balance_job_),
        "normal_to_normal_part_map_ size", normal_to_normal_part_map_.size());
  } else {
    for (ObTransferPartMap::const_iterator iter = normal_to_normal_part_map_.begin();
        OB_SUCC(ret) && iter != normal_to_normal_part_map_.end();
        iter++) {
      const ObLSID &src_ls_id = iter->first.get_src_ls_id();
      const ObLSID &dest_ls_id = iter->first.get_dest_ls_id();
      uint64_t src_ls_group_id = OB_INVALID_ID;
      uint64_t dest_ls_group_id = OB_INVALID_ID;
      if (OB_FAIL(ls_group_id_map_.get_refactored(src_ls_id, src_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(src_ls_id));
      } else if (OB_FAIL(ls_group_id_map_.get_refactored(dest_ls_id, dest_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(dest_ls_id));
      }
      if (OB_FAIL(ret)) {
      } else if (src_ls_group_id == dest_ls_group_id) {
        ADD_TRANSFER_TASK(dest_ls_group_id, src_ls_id, dest_ls_id, iter->second);
      } else {
        // split + alter + merge
        ObLSID tmp_ls_id;
        ADD_SPLIT_TASK(src_ls_group_id, src_ls_id, iter->second, tmp_ls_id);
        ADD_ALTER_TASK(dest_ls_group_id, tmp_ls_id);
        ADD_MERGE_TASK(dest_ls_group_id, tmp_ls_id, dest_ls_id);
      }
    } // end for
  }
  return ret;
}

// gen tasks by dup_to_dup_part_map_
// if both dup ls have the same ls_group_id: transfer
// else : 2 * alter + transfer + 2 * alter
//       1. alter both dup_ls to a ls_group_id
//       2. transfer from src dup_ls to dest dup_ls
//       3. alter ls_group_id of both dup_ls to old ls_group_id
int ObPartTransferJobGenerator::gen_transfer_tasks_between_dup_ls_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!balance_job_.is_valid()
      || dup_to_dup_part_map_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(balance_job_),
        "dup_to_dup_part_map_ size", dup_to_dup_part_map_.size());
  } else {
    for (ObTransferPartMap::const_iterator iter = dup_to_dup_part_map_.begin();
        OB_SUCC(ret) && iter != dup_to_dup_part_map_.end();
        iter++) {
      const ObLSID &src_ls_id = iter->first.get_src_ls_id();
      const ObLSID &dest_ls_id = iter->first.get_dest_ls_id();
      uint64_t src_ls_group_id = OB_INVALID_ID;
      uint64_t dest_ls_group_id = OB_INVALID_ID;
      uint64_t tmp_ls_group_id = OB_INVALID_ID;
      if (OB_FAIL(ls_group_id_map_.get_refactored(src_ls_id, src_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(src_ls_id));
      } else if (OB_FAIL(ls_group_id_map_.get_refactored(dest_ls_id, dest_ls_group_id))) {
        LOG_WARN("get_refactored failed", KR(ret), K(dest_ls_id));
      } else if (OB_FAIL(choose_dup_ls_transfer_ls_group_id_(
          src_ls_id,
          dest_ls_id,
          tmp_ls_group_id))) {
        LOG_WARN("choose dup ls transfer ls_group_id failed",
            KR(ret), K(src_ls_id), K(dest_ls_id), K(tmp_ls_group_id));
      } else if (tmp_ls_group_id == src_ls_group_id
          && tmp_ls_group_id == dest_ls_group_id) {
        ADD_TRANSFER_TASK(tmp_ls_group_id, src_ls_id, dest_ls_id, iter->second);
      } else { // 2 * alter + transfer + 2 * alter
        if (tmp_ls_group_id != src_ls_group_id) {
          ADD_ALTER_TASK(tmp_ls_group_id, src_ls_id);
        }
        if (tmp_ls_group_id != dest_ls_group_id) {
          ADD_ALTER_TASK(tmp_ls_group_id, dest_ls_id);
        }
        // after transfer, alter ls to original ls group id
        ADD_TRANSFER_TASK(tmp_ls_group_id, src_ls_id, dest_ls_id, iter->second);
        if (tmp_ls_group_id != src_ls_group_id) {
          ADD_ALTER_TASK(src_ls_group_id, src_ls_id); // reset ls group id
        }
        if (tmp_ls_group_id != dest_ls_group_id) {
          ADD_ALTER_TASK(dest_ls_group_id, dest_ls_id); // reset ls group id
        }
      }
    }
  }
  return ret;
}

int ObPartTransferJobGenerator::choose_dup_ls_transfer_ls_group_id_(
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  ls_group_id = OB_INVALID_ID;
  uint64_t src_ls_group_id = OB_INVALID_ID;
  uint64_t dest_ls_group_id = OB_INVALID_ID;
  uint64_t other_ls_group_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!src_ls_id.is_valid()
      || !dest_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(ls_group_id_map_.get_refactored(src_ls_id, src_ls_group_id))) {
    LOG_WARN("get_refactored failed", KR(ret), K(src_ls_id));
  } else if (OB_FAIL(ls_group_id_map_.get_refactored(dest_ls_id, dest_ls_group_id))) {
    LOG_WARN("get_refactored failed", KR(ret), K(src_ls_id));
  } else {
    for (ObLSGroupIDMap::const_iterator iter = ls_group_id_map_.begin();
        OB_SUCC(ret) && iter != ls_group_id_map_.end();
        iter++) {
      if (0 != iter->second && OB_INVALID_ID != iter->second) {
        other_ls_group_id = iter->second;
        break;
      }
    }
  }
  if (FAILEDx(ObLSBalanceTaskHelper::choose_ls_group_id_for_transfer_between_dup_ls(
      src_ls_group_id,
      dest_ls_group_id,
      other_ls_group_id,
      ls_group_id))) {
    LOG_WARN("choose ls_group_id for transfer between dup ls failed",
        KR(ret), K(src_ls_group_id), K(dest_ls_group_id), K(other_ls_group_id));
  }
  return ret;
}

} // end rootserver
} // end oceanbase