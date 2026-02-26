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
using namespace storage;

namespace rootserver
{
#define PB_INFO(fmt, args...) LOG_INFO("[PARTITION_BALANCE] " fmt, ##args)

ObPartTransferJobGenerator::ObPartTransferJobGenerator()
    : inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
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
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartTransferJobGenerator init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_ISNULL(sql_proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), KP(sql_proxy));
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
    const ObBalanceStrategy &balance_strategy,
    const ObBalanceJobID &job_id,
    const int64_t balance_timeout)
{
  int ret = OB_SUCCESS;
  balance_job_.reset();
  balance_tasks_.reset();
  ObBalanceJobID new_job_id = job_id;
  ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
  ObString comment;
  int64_t max_end_time = balance_timeout > 0
      ? (ObTimeUtility::current_time() + balance_timeout)
      : OB_INVALID_TIMESTAMP;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!job_type.is_valid()
      || !balance_strategy.is_valid()
      || balance_timeout < 0
      || !need_gen_job())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(job_type), K(balance_strategy),
        K(balance_timeout), "need_gen_job", need_gen_job());
  } else if (!new_job_id.is_valid() && OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, new_job_id))) {
    LOG_WARN("gen unique id failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(balance_job_.init(
      tenant_id_,
      new_job_id,
      job_type,
      job_status,
      comment,
      balance_strategy,
      max_end_time))) {
    LOG_WARN("job init fail", KR(ret), K(tenant_id_), K(job_id), K(new_job_id), K(job_type), K(job_status),
        K(comment), K(balance_strategy), K(max_end_time));
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
        balance_job_.get_job_id(), ls_group_id, src_ls_id,                               \
        balance_job_.get_balance_strategy(), balance_tasks_))) {                         \
      LOG_WARN("add ls alter task failed", KR(ret), K(tenant_id_), K(balance_job_),      \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(balance_tasks_));               \
    }                                                                                    \
  } while (0)

#define ADD_TRANSFER_TASK(ls_group_id, src_ls_id, dest_ls_id, part_list)                           \
  do {                                                                                             \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_transfer_task(tenant_id_, balance_job_.get_job_id(), \
        ls_group_id, src_ls_id, dest_ls_id, part_list,                                             \
        balance_job_.get_balance_strategy(), balance_tasks_))) {                                   \
      LOG_WARN("add ls transfer task failed", KR(ret), K(tenant_id_), K(balance_job_),             \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(part_list), K(balance_tasks_));           \
    }                                                                                              \
  } while (0)

#define ADD_MERGE_TASK(ls_group_id, src_ls_id, dest_ls_id)                                 \
  do {                                                                                     \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_merge_task(tenant_id_,                       \
        balance_job_.get_job_id(), ls_group_id, src_ls_id, dest_ls_id,                     \
        balance_job_.get_balance_strategy(), balance_tasks_))) {                           \
      LOG_WARN("add ls merge task failed", KR(ret), K(tenant_id_), K(balance_job_),        \
          K(src_ls_id), K(dest_ls_id), K(ls_group_id), K(balance_tasks_));                 \
    }                                                                                      \
  } while (0)

#define ADD_SPLIT_TASK(ls_group_id, src_ls_id, part_list, dest_ls_id)                                 \
  do {                                                                                                \
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_split_task(sql_proxy_, tenant_id_,                      \
        balance_job_.get_job_id(), ls_group_id, src_ls_id, part_list,                                 \
        balance_job_.get_balance_strategy(), dest_ls_id, balance_tasks_))) {                          \
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
  } else if (balance_job_.get_balance_strategy().is_part_balance_intra_group_weight() &&
      OB_FAIL(optimize_transfer_path_for_weight_balance())) {
    LOG_WARN("optimize transfer path for weight balance failed", KR(ret), K(balance_job_));
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

// e.g. original transfer path: LS1->LS2, LS2->LS3, LS3->LS4
//      optimized transfer path: LS1->LS4
int ObPartTransferJobGenerator::optimize_transfer_path_for_weight_balance()
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObTransferPartInfo, ObArray<ObTransferTaskKey>> part_map;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!balance_job_.is_valid()
      || !balance_job_.get_balance_strategy().is_part_balance_intra_group_weight())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(balance_job_));
  } else if (normal_to_normal_part_map_.empty()) {
    // do nothing
  } else if (OB_FAIL(gen_part_map_by_transfer_map_(part_map))) {
    LOG_WARN("gen_part_map_by_transfer_map_ failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(merge_transfer_task_for_each_part_(part_map))) {
    LOG_WARN("merge_transfer_task_for_each_part_ failed", KR(ret), K(tenant_id_));
  } else {
    normal_to_normal_part_map_.reuse();
    FOREACH_X(iter, part_map, OB_SUCC(ret)) {
      const ObTransferPartInfo &part_info = iter->first;
      const ObArray<ObTransferTaskKey> &task_key_arr = iter->second;
      if (0 == task_key_arr.count()) {
        //源端和目的端相等，不需要transfer
      } else if (OB_UNLIKELY(1 != task_key_arr.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part info can not transfer twice", KR(ret), K(task_key_arr), K(part_info));
      } else {
        const ObLSID &src_ls_id = task_key_arr.at(0).get_src_ls_id();
        const ObLSID &dest_ls_id = task_key_arr.at(0).get_dest_ls_id();
        ADD_TO_PART_MAP(normal_to_normal_part_map_);
      }
    }
  }
  return ret;
}

int ObPartTransferJobGenerator::gen_part_map_by_transfer_map_(
    hash::ObHashMap<ObTransferPartInfo, ObArray<ObTransferTaskKey>> &part_map)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(part_map.created())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("part map has been created", KR(ret));
  } else if (OB_FAIL(part_map.create(
      HASH_MAP_SIZE,
      "TmpPartMap",
      ObModIds::OB_HASH_NODE,
      tenant_id_))) {
    LOG_WARN("part_map create failed", KR(ret), K(tenant_id_));
  } else {
    FOREACH_X(iter, normal_to_normal_part_map_, OB_SUCC(ret)) {
      const ObTransferTaskKey &task_key = iter->first;
      ARRAY_FOREACH(iter->second, idx) {
        ObTransferPartInfo &part_info = iter->second.at(idx);
        ObArray<ObTransferTaskKey> *task_key_arr_ptr = part_map.get(part_info);
        if (OB_ISNULL(task_key_arr_ptr)) {
          ObArray<ObTransferTaskKey> new_task_key_arr;
          if (OB_FAIL(part_map.set_refactored(part_info, new_task_key_arr))) { // deep copy
            LOG_WARN("set refactored failed", KR(ret), K(part_info));
          } else {
            task_key_arr_ptr = part_map.get(part_info);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(task_key_arr_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail get part_list_ptr from map", KR(ret), K(task_key));
        } else if (OB_FAIL(task_key_arr_ptr->push_back(task_key))) {
          LOG_WARN("push back failed", KR(ret), K(task_key));
        }
      }
    }
  }
  return ret;
}

int ObPartTransferJobGenerator::merge_transfer_task_for_each_part_(
    hash::ObHashMap<ObTransferPartInfo, ObArray<ObTransferTaskKey>> &part_map)
{
  int ret = OB_SUCCESS;
  if (part_map.empty()) {
    // do nothing
  } else {
    FOREACH_X(iter, part_map, OB_SUCC(ret)) {
      const ObTransferPartInfo &part = iter->first;
      ObArray<ObTransferTaskKey> &task_key_arr = iter->second;
      if (task_key_arr.count() <= 1) {
        // do nothing
      } else {
        ObArray<ObLSID> src_ls;
        ObArray<ObLSID> dest_ls;
        for (int64_t i = 0; OB_SUCC(ret) && i < task_key_arr.count(); ++i) {
          ObTransferTaskKey &key = task_key_arr.at(i);
          if (OB_FAIL(src_ls.push_back(key.get_src_ls_id()))) {
            LOG_WARN("failed to push back src ls", KR(ret), K(key));
          } else if (OB_FAIL(dest_ls.push_back(key.get_dest_ls_id()))) {
            LOG_WARN("failed to push back dest ls", KR(ret), K(key));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (src_ls.count() != dest_ls.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src ls must equal to dest ls", KR(ret), K(src_ls), K(dest_ls));
        } else {
          for (int64_t i = src_ls.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
            int64_t index = 0;
            if (has_exist_in_array(dest_ls, src_ls.at(i), &index)) {
              //源端在目的端存在，可以消除掉
              if (OB_FAIL(src_ls.remove(i))) {
                LOG_WARN("failed to remove ls", KR(ret), K(i), K(src_ls));
              } else if (index < 0 || index >= dest_ls.count()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("index in unexpected", KR(ret), K(index), K(dest_ls));
              } else if (OB_FAIL(dest_ls.remove(index))) {
                LOG_WARN("failed to remove ls", KR(ret), K(index), K(dest_ls));
              }
            }
          }//end for
          if (OB_FAIL(ret)) {
          } else if (0 == src_ls.count() && 0 == dest_ls.count()) {
            //存在概率src等于dest, 安全起见不删除
            LOG_INFO("part no need transfer", K(part), K(task_key_arr));
            task_key_arr.reset();
          } else if (1 != src_ls.count() || 1 != dest_ls.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("src ls must equal to dest ls", KR(ret), K(src_ls), K(dest_ls), K(task_key_arr));
          } else {
            LOG_INFO("task key before merge", K(part), K(task_key_arr));
            task_key_arr.reset();
            ObTransferTaskKey key(src_ls.at(0), dest_ls.at(0));
            if (OB_FAIL(task_key_arr.push_back(key))) {
              LOG_WARN("failed to push back key", KR(ret), K(key));
            }
            LOG_INFO("merge task key success", K(part), K(key));
          }
        }
      }
    }
  }
  return ret;
}

} // end rootserver
} // end oceanbase
