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
#include "rootserver/ob_ls_balance_helper.h"
#include "rootserver/ob_primary_ls_service.h"//fetch max ls id
#include "src/storage/tx/ob_trans_service.h"
#include "ob_ls_balance_helper.h"

#define ISTAT(fmt, args...) FLOG_INFO("[LS_BALANCE] " fmt, ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[LS_BALANCE] " fmt, ##args)

namespace oceanbase
{
using namespace share;
namespace rootserver
{
//////ObUnitGroupBalanceInfo
void ObUnitGroupBalanceInfo::reset()
{
  target_ls_count_ = OB_INVALID_COUNT;
  unit_group_.reset();
  redundant_ls_array_.reset();
  normal_ls_array_.reset();
}

int ObUnitGroupBalanceInfo::add_ls_status_info(const ObLSStatusInfo &ls_info)
{
  int ret = OB_SUCCESS;
  //TODO has ls group id not match
  if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info));
  } else if (normal_ls_array_.count() >= target_ls_count_
             || !is_active_unit_group()) {
    if (OB_FAIL(redundant_ls_array_.push_back(ls_info))) {
      LOG_WARN("failed to push back ls info", KR(ret), K(ls_info));
    }
  } else if (OB_FAIL(normal_ls_array_.push_back(ls_info))) {
    LOG_WARN("failed to push back ls info", KR(ret), K(ls_info));
  }
  return ret;
}

int ObUnitGroupBalanceInfo::remove_redundant_ls(const int64_t &index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index >= redundant_ls_array_.count() || index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index));
  } else if (OB_FAIL(redundant_ls_array_.remove(index))) {
    LOG_WARN("failed to remove index", KR(ret), K(index));
  }
  return ret;
}

int ObUnitGroupBalanceInfo::get_and_remove_ls_status(share::ObLSStatusInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  ObLSStatusInfoArray &ls_array = redundant_ls_array_.count() > 0 ? redundant_ls_array_ : normal_ls_array_;
  const int64_t ls_count = ls_array.count();
  if (ls_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls array is empty", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_info.assign(ls_array.at(ls_count - 1)))) {
    LOG_WARN("failed to assign ls info", KR(ret), K(ls_count), K(ls_array));
  } else if (OB_FAIL(ls_array.remove(ls_count - 1))) {
    LOG_WARN("failed to remove ls from array", KR(ret), K(ls_count), K(ls_array));
  }
  return ret;
}


//////////////ObLSBalanceTaskHelper

ObLSBalanceTaskHelper::ObLSBalanceTaskHelper() :
    inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    primary_zone_num_(0),
    ls_scale_out_factor_(0),
    enable_transfer_(false),
    balance_strategy_(),
    unit_group_balance_array_(),
    sql_proxy_(NULL),
    job_(),
    task_array_(),
    tenant_ls_bg_info_(),
    dup_ls_stat_array_()
{
}

int ObLSBalanceTaskHelper::init(
    const uint64_t tenant_id,
    const share::ObLSStatusInfoArray &status_array,
    const ObIArray<share::ObSimpleUnitGroup> &unit_group_array,
    const int64_t primary_zone_num,
    ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(status_array.empty()
      || unit_group_array.empty()
      || 0 >= primary_zone_num
      || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status_array), K(unit_group_array),
                                 K(primary_zone_num), K(tenant_id));
  } else {
    //1. get scale_out_factor
    uint64_t data_version = 0;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
    } else if (data_version < MOCK_DATA_VERSION_4_2_5_1
        || (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_4_1_0)) {
      //not valid to use scale_out_factor
      ls_scale_out_factor_ = 1;
    } else {
      ls_scale_out_factor_ = tenant_config->ls_scale_out_factor;
    }
    //2. init all unit balance info
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array.count(); ++i) {
      ObUnitGroupBalanceInfo balance_info(unit_group_array.at(i),
          primary_zone_num * ls_scale_out_factor_);
      if (OB_FAIL(unit_group_balance_array_.push_back(balance_info))) {
        LOG_WARN("failed to push back balance info", KR(ret), K(balance_info), K(i));
      }
    }
    int64_t index = OB_INVALID_INDEX_INT64;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_array.count(); ++i) {
      const ObLSStatusInfo &ls_status = status_array.at(i);
      if (ls_status.is_duplicate_ls()) {
        if (OB_FAIL(dup_ls_stat_array_.push_back(ls_status))) {
          LOG_WARN("push back failed", KR(ret), K(ls_status));
        } else {
          continue;
        }
      } else if (OB_FAIL(find_unit_group_balance_index(ls_status.unit_group_id_, index))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          //normal, ls status must has target unit_group,
          //but maybe migrate unit and ls group balance concurrency
          LOG_WARN("has ls in not valid unit group", KR(ret), K(ls_status), K(unit_group_array));
          // ret = OB_SUCCESS;
          // index = unit_group_balance_array_.count();
          // ObSimpleUnitGroup unit_group(ls_status.unit_group_id_, ObUnit::UNIT_STATUS_DELETING);
          // ObUnitGroupBalanceInfo balance_info(unit_group, primary_zone_num * ls_scale_out_factor_);
          // if (OB_FAIL(unit_group_balance_array_.push_back(balance_info))) {
          //   LOG_WARN("failed to push back balance info", KR(ret), K(balance_info));
          // }
        } else {
          LOG_WARN("failed to find index", KR(ret), K(ls_status));
        }
      }
      if (FAILEDx(unit_group_balance_array_.at(index).add_ls_status_info(ls_status))) {
        LOG_WARN("failed to add ls status info", KR(ret), K(ls_status));
      }
    }//end for i
    //4. other parameters
    if (OB_SUCC(ret)) {
      primary_zone_num_ = primary_zone_num;
      tenant_id_ = tenant_id;
      enable_transfer_ = tenant_config->enable_transfer;
      balance_strategy_.reset();
      sql_proxy_ = sql_proxy;
      job_.reset();
      task_array_.reset();
      if (OB_FAIL(generate_balance_job_strategy_())) {
        LOG_WARN("failed to get balance strategy", KR(ret));
      } else {
        inited_ = true;
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::find_unit_group_balance_index(const uint64_t unit_group_id, int64_t &index)
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX_INT64;
  if (OB_UNLIKELY(OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_group_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      if (unit_group_id == unit_group_balance_array_.at(i).get_unit_group_id()) {
        index = i;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_INDEX_INT64 == index) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to find ls unit group", KR(ret), K(unit_group_id), K(unit_group_balance_array_));
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::check_need_ls_balance(bool &need_balance)
{
  int ret = OB_SUCCESS;
  need_balance = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    need_balance = balance_strategy_.is_valid();
  }
  return ret;
}

int ObLSBalanceTaskHelper::check_need_modify_ls_group_(
    const ObUnitGroupBalanceInfo &balance_info,
    bool &need_modify)
{
  int ret = OB_SUCCESS;
  need_modify = false;
  uint64_t ls_group_id = OB_INVALID_ID;
  ARRAY_FOREACH_X(balance_info.get_normal_ls_array(), i, cnt, OB_SUCC(ret) && !need_modify) {
    const ObLSStatusInfo &ls_status_info = balance_info.get_normal_ls_array().at(i);
    if (OB_INVALID_ID == ls_status_info.ls_group_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls group id not expected", KR(ret), K(ls_status_info));
    } else if (OB_INVALID_ID == ls_group_id) {
      ls_group_id = ls_status_info.ls_group_id_;
    } else if (ls_group_id != ls_status_info.ls_group_id_) {
      need_modify = true;
      ISTAT("unit group has different ls group, need modify",
          K(ls_group_id), K(ls_status_info), K(balance_info));
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_ls_balance_task()
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  int64_t normal_ls_count = OB_INVALID_COUNT;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(generate_balance_job_())) {
    LOG_WARN("failed to generate job", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or schema service is null", KR(ret), K(sql_proxy_), K(schema_service));
  } else if (FALSE_IT(normal_ls_count = job_.get_primary_zone_num() * job_.get_unit_group_num() * ls_scale_out_factor_)) {
  } else if (OB_FAIL(tenant_ls_bg_info_.init(tenant_id_, normal_ls_count))) {
    LOG_WARN("init tenant_ls_bg_info_ failed", KR(ret), K(tenant_id_), K(normal_ls_count));
    // build tenant all balance group info for ALL LS
  } else if (OB_FAIL(tenant_ls_bg_info_.build("LS_BALANCE", *sql_proxy_, *schema_service))) {
    LOG_WARN("build tenant all balance group info for all LS failed", KR(ret), K(tenant_id_));
  } else {
    if (job_.get_balance_strategy().is_ls_balance_by_alter()) {
      if (OB_FAIL(generate_alter_task_())) {
        LOG_WARN("failed to generate alter task", KR(ret));
      }
    } else if (job_.get_balance_strategy().is_ls_balance_by_factor()) {
      if (OB_FAIL(generate_factor_task_())) {
        LOG_WARN("failed to genrate factorn task", KR(ret));
      }
    } else if (job_.get_balance_strategy().is_ls_balance_by_migrate()) {
      // 1. first migrate task
      if (OB_FAIL(generate_migrate_task_())) {
        LOG_WARN("failed to generate migrate task", KR(ret));
      }
    } else if (job_.get_balance_strategy().is_ls_balance_by_expand()) {
      // 2. try expand
      if (OB_FAIL(generate_expand_task_())) {
        LOG_WARN("failed to generate expand task", KR(ret));
      }
    } else if (job_.get_balance_strategy().is_ls_balance_by_shrink()) {
      // 3. try shrink
      if (OB_FAIL(generate_shrink_task_())) {
        LOG_WARN("failed to generate expand task", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no other balance job", KR(ret), K_(job));
    }
    if (OB_SUCC(ret) && 0 == task_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("has no task", KR(ret), K(job_));
    }
    ISTAT("generate task", KR(ret), K(job_), K(task_array_));
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_balance_job_strategy_()
{

  balance_strategy_.reset();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(unit_group_balance_array_.count() <= 0)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), K(unit_group_balance_array_));
  } else {
    //广播日志流处理
    if (enable_transfer_) {
      if (has_redundant_dup_ls_()) {
        balance_strategy_ = ObBalanceStrategy::LB_SHRINK;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_ls_stat_array_.count() && !balance_strategy_.is_valid(); i++) {
      ObLSStatusInfo &ls_info = dup_ls_stat_array_.at(i);
      if (0 != ls_info.get_ls_group_id()) {
        balance_strategy_ = ObBalanceStrategy::LB_ALTER;
      }
    }//end for i to check dup ls
  }//end for check dup ls
  //除了广播日志流之外的其他日志流
  if (OB_SUCC(ret) && !balance_strategy_.is_valid()) {
    bool lack_ls = false;
    bool redundant_ls = false;
    bool need_modify_ls_group = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.get_lack_ls_count() > 0) {
        lack_ls = true;
        ISTAT("unit group has little ls than expected", K(balance_info));
      }
      if (balance_info.get_redundant_ls_count() > 0) {
        redundant_ls = true;
        ISTAT("unit group has more ls than expected", K(balance_info));
      }
      if (FAILEDx(check_need_modify_ls_group_(balance_info, need_modify_ls_group))) {
        LOG_WARN("check need modify ls group failed", KR(ret), K(balance_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (need_modify_ls_group) {
        balance_strategy_ = ObBalanceStrategy::LB_ALTER;
      } else if (!enable_transfer_) {
        bool need_balance = false;
        if (OB_FAIL(check_ls_count_balanced_between_normal_unitgroup_(need_balance))) {
          LOG_WARN("failed to check need balance", KR(ret), K(need_balance));
        } else if (!need_balance) {
          balance_strategy_.reset();
        } else {
          balance_strategy_ = ObBalanceStrategy::LB_MIGRATE;
        }
      } else if (ls_scale_out_factor_ > 1 && (lack_ls || redundant_ls)) {
        //在扩缩容时，不用考虑多出来的广播日志流
        //日志流个数不符合预期，并且ls_scale_out_factor_ 大于1，就走特殊的逻辑
        balance_strategy_ = ObBalanceStrategy::LB_SCALE_OUT_FACTOR;
      } else if (lack_ls && redundant_ls) {
        balance_strategy_ = ObBalanceStrategy::LB_MIGRATE;
      } else if (lack_ls) {
        balance_strategy_ = ObBalanceStrategy::LB_EXPAND;
      } else if (redundant_ls) {
        balance_strategy_ = ObBalanceStrategy::LB_SHRINK;
      } else {
        //no need balance
        balance_strategy_.reset();
      }
    }
  }
  ISTAT("get balance strategy", K(balance_strategy_), K(enable_transfer_), K(ls_scale_out_factor_),
      K(unit_group_balance_array_),
      K(dup_ls_stat_array_));
  return ret;

}

int ObLSBalanceTaskHelper::generate_balance_job_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(unit_group_balance_array_.count() <= 0)
      || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), KP(sql_proxy_), K(unit_group_balance_array_));
  } else if (!balance_strategy_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("balance strategy is invalid", KR(ret), K(balance_strategy_));
  } else {
    ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_LS);
    ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
    int64_t unit_group_num = 0;
    ObBalanceJobID job_id;
    ObString comment;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.is_active_unit_group()) {
        unit_group_num++;
      }
    }

    if (FAILEDx(ObCommonIDUtils::gen_unique_id(tenant_id_, job_id))) {
      LOG_WARN("generate unique id for balance job fail", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(job_.init(tenant_id_, job_id, job_type, job_status, primary_zone_num_,
            unit_group_num, comment, balance_strategy_))) {
      LOG_WARN("failed to init job", KR(ret), K(tenant_id_), K(job_id), K(job_type),
          K(job_status), K(primary_zone_num_), K(unit_group_num), K(balance_strategy_));
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_alter_task_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    //先调整广播日志流的属性, 这个时候可能存在即将被删除的unit_group
    //广播日志流可能也存在在这种unit_group上面，广播日志流的ls_group_id不为0的情况下，需要清理掉
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_ls_stat_array_.count(); ++i) {
      ObLSStatusInfo &ls_info = dup_ls_stat_array_.at(i);
      if (0 != ls_info.get_ls_group_id()) {
        if (OB_FAIL(construct_ls_alter_task_(ls_info.get_ls_id(), 0/*ls_group_id*/))) {
          LOG_WARN("construct ls alter task failed", KR(ret), K(dup_ls_stat_array_));
        }
      }
    }//end for i
    //处理正常的日志流
    uint64_t ls_group_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      ls_group_id = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_count(); ++j) {
        const ObLSStatusInfo &ls_status_info = balance_info.get_normal_ls_array().at(j);
        if (OB_INVALID_ID == ls_group_id) {
          ls_group_id = ls_status_info.ls_group_id_;
        } else if (ls_group_id != ls_status_info.ls_group_id_) {
          if (OB_FAIL(construct_ls_alter_task_(ls_status_info.ls_id_, ls_group_id))) {
            LOG_WARN("failed to construct ls alter task", KR(ret),
                     K(ls_status_info), K(ls_group_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_factor_task_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(ls_scale_out_factor_ <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scale out factor not valid to generate task", KR(ret), K(ls_scale_out_factor_));
  } else {
    //1. 检查是否有deleting状态的unit group，把这部分日志流打散到其他的可用的unit_group中
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.is_active_unit_group()) {
        //nothing todo
      } else {
        //把这个需要删除的unit_group上面的日志流均匀的分给剩下的unit_group
        if (OB_FAIL(generate_migrate_task_for_deleting_unit_(balance_info))) {
          LOG_WARN("failed to generate migrate task", KR(ret), K(balance_info));
        }
      }
    }
    //2. 检查每个unit_group组，梳理日志流个数
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (!balance_info.is_active_unit_group()) {
        //防御性检查，第一步操作已经把deleting状态unit_group中的日志流全部迁移给了其他可用的unit_group
        if (balance_info.get_all_ls_count() > 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit group is deleting", KR(ret), K(balance_info));
        }
      } else if (balance_info.get_lack_ls_count() > 0) {
        if (OB_FAIL(generate_expand_task_for_factor_(balance_info))) {
          LOG_WARN("failed to generate expand task", KR(ret), K(balance_info));
        }
      } else if (balance_info.get_redundant_ls_count() > 0) {
        //need shrink ls
        if (OB_FAIL(generate_shrink_task_for_factor_(balance_info))) {
          LOG_WARN("failed to generate shrink task", KR(ret), K(balance_info));
        }
      } else {
        //nothing todo
      }
    }//end for
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_migrate_task_for_deleting_unit_(ObUnitGroupBalanceInfo &balance_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (balance_info.is_active_unit_group()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is active", KR(ret), K(balance_info));
  } else {
    for (int64_t j = balance_info.get_redundant_ls_count() - 1; OB_SUCC(ret) && j >= 0; --j) {
      //把日志流迁移到最少的unit_group里面
      int64_t index = OB_INVALID_INDEX;
      int64_t max_index = OB_INVALID_INDEX;//no used
      const ObLSStatusInfo &ls_status = balance_info.get_redundant_ls_array().at(j);
      if (OB_FAIL(get_min_max_ls_unitgroup_(index, max_index))) {
        LOG_WARN("failed to get min ls count", KR(ret), K(index));
      } else if (OB_INVALID_INDEX == index || index >= unit_group_balance_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index is invalid", KR(ret), K(index), K(unit_group_balance_array_));
      } else {
        ObUnitGroupBalanceInfo &dest_balance_info = unit_group_balance_array_.at(index);
        if (balance_info.get_unit_group_id() == dest_balance_info.get_unit_group_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group can not has more ls and lack ls", KR(ret), K(j),
              K(balance_info), K(dest_balance_info));
        } else if (OB_FAIL(generate_ls_alter_task_(ls_status, dest_balance_info))) {
          LOG_WARN("failed to generate ls alter task", KR(ret), K(ls_status), K(dest_balance_info));
        } else if (OB_FAIL(balance_info.remove_redundant_ls(j))) {
          LOG_WARN("failed to remove redundant ls", KR(ret), K(j));
        }
      }
    }//end for j
  }
  return ret;
}

int ObLSBalanceTaskHelper::get_min_max_ls_unitgroup_(int64_t &min_index, int64_t &max_index)
{
  int ret = OB_SUCCESS;
  min_index = OB_INVALID_INDEX;
  max_index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(unit_group_balance_array_.count() <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    int64_t smallest_ls_count = INT64_MAX;
    int64_t max_ls_count = -1;
    for (int64_t k = 0; OB_SUCC(ret) && k < unit_group_balance_array_.count(); ++k) {
      ObUnitGroupBalanceInfo &dest_balance_info = unit_group_balance_array_.at(k);
      const int64_t ls_count = dest_balance_info.get_all_ls_count();
      if (!dest_balance_info.is_active_unit_group()) {
      } else {
        if (smallest_ls_count > ls_count) {
          smallest_ls_count = ls_count;
          min_index = k;
        }
        if (max_ls_count < ls_count) {
          max_ls_count = ls_count;
          max_index = k;
        }
      }
    }
    if (OB_SUCC(ret) && (INT64_MAX == smallest_ls_count || -1 == max_ls_count
          || OB_INVALID_INDEX == min_index || OB_INVALID_INDEX == max_index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", KR(ret), K(smallest_ls_count),
          K(max_ls_count), K(min_index), K(max_index), K(unit_group_balance_array_));
    }
  }
  return ret;

}
int ObLSBalanceTaskHelper::check_ls_count_balanced_between_normal_unitgroup_(bool &need_balance)
{
  int ret = OB_SUCCESS;
   need_balance = false;
  if (OB_UNLIKELY(unit_group_balance_array_.count() <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  }
  //1. 存在unit_group处于deleting状态，并且内部存在日志流
  if (OB_SUCC(ret) && !need_balance) {
    for (int64_t k = 0; OB_SUCC(ret) && k < unit_group_balance_array_.count() && !need_balance; ++k) {
      ObUnitGroupBalanceInfo &dest_balance_info = unit_group_balance_array_.at(k);
      const int64_t ls_count = dest_balance_info.get_all_ls_count();
      if (!dest_balance_info.is_active_unit_group()) {
        if (ls_count > 0) {
          need_balance = true;
        }
      }
    }//end for check deleting unit group
  }
  //2. unit_group间的日志流个数差值大于1
  if (OB_SUCC(ret) && !need_balance) {
    int64_t min_index = OB_INVALID_INDEX;
    int64_t max_index = OB_INVALID_INDEX;
    if (OB_FAIL(get_min_max_ls_unitgroup_(min_index, max_index))) {
      LOG_WARN("failed to get min max ls unitgroup", KR(ret));
    } else if (OB_UNLIKELY(min_index < 0 || min_index >= unit_group_balance_array_.count()
          || max_index < 0 || max_index >= unit_group_balance_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min or max index is not expected", KR(ret), K(min_index), K(max_index),
          K(unit_group_balance_array_));
    } else {
      ObUnitGroupBalanceInfo &src_group = unit_group_balance_array_.at(max_index);
      ObUnitGroupBalanceInfo &dest_group = unit_group_balance_array_.at(min_index);
      //need balance
      if (src_group.get_all_ls_count() - dest_group.get_all_ls_count() > 1) {
        need_balance = true;
      }
    }
  }

  return ret;
}

int ObLSBalanceTaskHelper::generate_expand_task_for_factor_(const ObUnitGroupBalanceInfo &balance_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(ls_scale_out_factor_ <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scale out factor not valid to generate task", KR(ret), K(ls_scale_out_factor_));
  } else if (!balance_info.is_active_unit_group()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is not active", KR(ret), K(balance_info));
  } else {
    const int64_t lack_count = balance_info.get_lack_ls_count();
    if (0 == balance_info.get_normal_ls_count()) {
      //need create new ls
      uint64_t ls_group_id = 0;
      if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_, tenant_id_, ls_group_id))) {
        LOG_WARN("failed to feath new ls group id", KR(ret), K(tenant_id_));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < lack_count; ++i) {
        //create new ls
        if (OB_FAIL(add_create_ls_task(tenant_id_, job_.get_job_id(),
                ls_group_id, job_.get_balance_strategy(), sql_proxy_, task_array_))) {
          LOG_WARN("failed to add create ls task", KR(ret), K(tenant_id_), K(job_), K(ls_group_id));
        }
      }
    } else {
      //need expand ls
      const uint64_t ls_group_id = balance_info.get_normal_ls_array().at(0).ls_group_id_;
      ObSplitLSParamArray src_ls;
      ObArray<ObSplitLSParamArray> dest_ls;
      const double src_factor = 1;
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_count(); ++j) {
        ObSplitLSParam param(&balance_info.get_normal_ls_array().at(j), src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(j));
        }
      }//end for j
      if (FAILEDx(construct_expand_dest_param_(lack_count, src_ls, dest_ls))) {
        LOG_WARN("failed to construct expand dest param", KR(ret), K(lack_count), K(src_ls));
      } else if (lack_count != dest_ls.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lack count not equal to dest ls count", KR(ret), K(lack_count), K(dest_ls));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < lack_count; ++j) {
        if (OB_FAIL(generate_balance_task_for_expand_(dest_ls.at(j),
                ls_group_id))) {
          LOG_WARN("failed to get balance task", KR(ret), K(j), K(ls_group_id),
              "dest_ls_param", dest_ls.at(j));
        }
      }//end for j
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_shrink_task_for_factor_(const ObUnitGroupBalanceInfo &balance_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(ls_scale_out_factor_ <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scale out factor not valid to generate task", KR(ret), K(ls_scale_out_factor_));
  } else if (!balance_info.is_active_unit_group()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is not active", KR(ret), K(balance_info));
  } else {
    const int64_t normal_ls_count = balance_info.get_normal_ls_count();
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    const double src_factor = 1;
    for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_redundant_ls_count(); ++j) {
      ObSplitLSParam param(&balance_info.get_redundant_ls_array().at(j), src_factor);
      if (OB_FAIL(src_ls.push_back(param))) {
        LOG_WARN("failed to push back param", KR(ret), K(param), K(j));
      }
    }
    if (FAILEDx(construct_shrink_src_param_(normal_ls_count, src_ls, dest_ls))) {
      LOG_WARN("failed to construct shrink src param", KR(ret), K(normal_ls_count), K(src_ls));
    } else if (dest_ls.count() != normal_ls_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("split param count not equal to normal ls count", KR(ret), K(normal_ls_count), K(dest_ls));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < normal_ls_count; ++j) {
      if (OB_FAIL(generate_task_for_shrink_(dest_ls.at(j),
              balance_info.get_normal_ls_array().at(j)))) {
        LOG_WARN("failed to generate task for shrink", KR(ret), K(dest_ls), K(j), K(balance_info));
      }
    }

  }
  return ret;

}


int ObLSBalanceTaskHelper::generate_migrate_task_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    //1. 先把deleting状态的unit_group清空
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (!balance_info.is_active_unit_group()) {
        if (OB_FAIL(generate_migrate_task_for_deleting_unit_(balance_info))) {
          LOG_WARN("failed to generate migrate task", KR(ret), K(balance_info));
        }
      }
    }//end for
    if (OB_FAIL(ret)) {
    } else if (!enable_transfer_) {
      if (OB_FAIL(generate_migrate_task_while_disable_transfer_())) {
        LOG_WARN("failed to generate migrate task", KR(ret));
      }
    } else if (OB_FAIL(generate_migrate_task_while_enable_transfer_())) {
      LOG_WARN("failed to generate migrate task", KR(ret));
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_migrate_task_while_disable_transfer_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    bool has_task = true;
    //在进入这个日志流均衡之前，已经处理过unit_group的deleting
    while (has_task && OB_SUCC(ret)) {
      has_task = false;
      int64_t min_index = OB_INVALID_INDEX;
      int64_t max_index = OB_INVALID_INDEX;
      //找到最多日志流个数的unit_group, 然后找到最小unit_group的
      if (OB_FAIL(get_min_max_ls_unitgroup_(min_index, max_index))) {
        LOG_WARN("failed to get min or max unit group", KR(ret));
      } else if (OB_UNLIKELY(min_index < 0 || min_index >= unit_group_balance_array_.count()
            || max_index < 0 || max_index >= unit_group_balance_array_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("min or max index is not expected", KR(ret), K(min_index), K(max_index),
            K(unit_group_balance_array_));
      } else {
        ObUnitGroupBalanceInfo &src_group = unit_group_balance_array_.at(max_index);
        ObUnitGroupBalanceInfo &dest_group = unit_group_balance_array_.at(min_index);
        //need balance
        share::ObLSStatusInfo ls_status;
        if (src_group.get_all_ls_count() - dest_group.get_all_ls_count() <= 1) {
          has_task = false;
        } else if (OB_FAIL(src_group.get_and_remove_ls_status(ls_status))) {
          LOG_WARN("failed to get and remove ls status", KR(ret), K(src_group));
        } else if (OB_FAIL(generate_ls_alter_task_(ls_status, dest_group))) {
          LOG_WARN("failed to generate ls alter task", KR(ret), K(ls_status), K(dest_group));
        } else {
          has_task = true;
        }
      }
    }//end while
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_migrate_task_while_enable_transfer_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    //get a redundant ls, and found one unit group less ls
    bool new_task = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count() && new_task; ++i) {
      ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      for (int64_t j = balance_info.get_redundant_ls_count() - 1; OB_SUCC(ret) && j >= 0 && new_task; --j) {
        //get one unit group, which less than primary_zone_unit_num
        const ObLSStatusInfo &ls_status = balance_info.get_redundant_ls_array().at(j);
        new_task = false;
        //一个ls_status只能生成一个ls_alter任务，在生成任务后，要跳出循环
        for (int64_t k = 0; OB_SUCC(ret) && k < unit_group_balance_array_.count() && !new_task; ++k) {
          ObUnitGroupBalanceInfo &dest_balance_info = unit_group_balance_array_.at(k);
          if (dest_balance_info.get_lack_ls_count() > 0) {
            new_task = true;
            if (balance_info.get_unit_group_id() == dest_balance_info.get_unit_group_id()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("ls group can not has more ls and lack ls", KR(ret),
                       K(i), K(k), K(j), K(balance_info), K(dest_balance_info));
            } else if (OB_FAIL(generate_ls_alter_task_(ls_status, dest_balance_info))) {
              LOG_WARN("failed to generate ls alter task", KR(ret), K(ls_status), K(dest_balance_info));
            }
          }
        }//end for k
        if (OB_SUCC(ret) && new_task) {
          //remove ls status from the unit group
          if (OB_FAIL(balance_info.remove_redundant_ls(j))) {
            LOG_WARN("failed to remove redundant ls", KR(ret), K(j));
          }
        }
      }//end for j
    }//end for i
  }
  return ret;

}

int ObLSBalanceTaskHelper::generate_ls_alter_task_(
    const ObLSStatusInfo &ls_status_info,
    ObUnitGroupBalanceInfo &dest_unit_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_status_info), K(ls_scale_out_factor_), K(dest_unit_group));
  } else {
    uint64_t ls_group_id = OB_INVALID_ID;
    ObLSStatusInfo dest_ls_status;
    if (dest_unit_group.get_normal_ls_count() > 0) {
      ls_group_id = dest_unit_group.get_normal_ls_array().at(0).ls_group_id_;
    } else if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_, tenant_id_, ls_group_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id_));
    }
    if (FAILEDx(construct_ls_alter_task_(ls_status_info.ls_id_, ls_group_id))) {
      LOG_WARN("failed to construct ls alter task", KR(ret), K(ls_status_info), K(ls_group_id));
    } else if (OB_FAIL(dest_ls_status.init(ls_status_info.tenant_id_,
                                           ls_status_info.ls_id_, ls_group_id,
                                           ls_status_info.status_,
                                           ls_status_info.unit_group_id_,
                                           ls_status_info.primary_zone_,
                                           ls_status_info.get_flag()))) {
      LOG_WARN("failed to init ls status", KR(ret), K(ls_group_id), K(ls_status_info));
    } else if (OB_FAIL(dest_unit_group.add_ls_status_info(dest_ls_status))) {
      LOG_WARN("failed to add ls status info", KR(ret), K(dest_ls_status));
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_expand_task_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(1 != ls_scale_out_factor_)) {
    //ls_scale_out_factor_ > 1应该走generate_expand_task_for_factor_
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scale out factor not valid to generate task", KR(ret), K(ls_scale_out_factor_));
  } else {
    int64_t lack_count = 0;
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    const double src_factor = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo & balance_info = unit_group_balance_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_count(); ++j) {
        ObSplitLSParam param(&balance_info.get_normal_ls_array().at(j), src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        lack_count += balance_info.get_lack_ls_count();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(0 >= lack_count || src_ls.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not generate expand task", KR(ret), K(lack_count), K(src_ls));
    } else if (OB_FAIL(construct_expand_dest_param_(lack_count, src_ls, dest_ls))) {
      LOG_WARN("failed to construct expand dest param", KR(ret), K(lack_count), K(src_ls));
    }
    int64_t dest_ls_index = 0;
    uint64_t ls_group_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ls_group_id = OB_INVALID_ID;
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.get_normal_ls_count() > 0) {
        ls_group_id = balance_info.get_normal_ls_array().at(0).ls_group_id_;
      } else if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_, tenant_id_, ls_group_id))) {
        LOG_WARN("failed to fetch new ls group id", KR(ret), K(tenant_id_));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_lack_ls_count(); ++j) {
        if (OB_UNLIKELY(dest_ls_index >= dest_ls.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest ls index not expected", KR(ret), K(dest_ls_index));
        } else if (OB_FAIL(generate_balance_task_for_expand_(dest_ls.at(dest_ls_index), ls_group_id))) {
          LOG_WARN("failed to get balance task", KR(ret), K(i), K(j), K(ls_group_id),
                   "dest_ls_param", dest_ls.at(dest_ls_index));
        } else {
          ++dest_ls_index;
        }
      }
      if (OB_SUCC(ret)) {
        lack_count += balance_info.get_lack_ls_count();
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_shrink_task_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (has_redundant_dup_ls_()) {
    if (OB_FAIL(generate_task_for_dup_ls_shrink_())) {
      LOG_WARN("generate task for dup ls shrink failed", KR(ret), K(dup_ls_stat_array_));
    }
  } else if (OB_UNLIKELY(1 != ls_scale_out_factor_)) {
    //ls_scale_out_factor_ > 1应该走generate_shrink_task_for_factor_
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scale out factor not valid to generate task", KR(ret), K(ls_scale_out_factor_));
  } else { // generate normal ls shrink task
    const int64_t normal_ls_count = job_.get_primary_zone_num() * job_.get_unit_group_num();
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    const double src_factor = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo & balance_info = unit_group_balance_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_redundant_ls_count(); ++j) {
        ObSplitLSParam param(&balance_info.get_redundant_ls_array().at(j), src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(i), K(j));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(src_ls.empty())) {
      ret= OB_ERR_UNEXPECTED;
      LOG_WARN("generate shrink task without redundant ls", KR(ret),
          K(unit_group_balance_array_), K(dup_ls_stat_array_));
    } else if (OB_FAIL(construct_shrink_src_param_(normal_ls_count, src_ls, dest_ls))) {
      LOG_WARN("failed to construct shrink src param", KR(ret), K(normal_ls_count), K(src_ls));
    } else if (normal_ls_count != dest_ls.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("normal ls count must be equal to split param arry", KR(ret), K(normal_ls_count), K(dest_ls));
    } else {
      int64_t dest_index = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
        const ObUnitGroupBalanceInfo & balance_info = unit_group_balance_array_.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_count(); ++j) {
          if (OB_UNLIKELY(dest_ls.count() <= dest_index)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("src ls is unexpected", KR(ret), K(dest_ls), K(dest_index));
          } else if (OB_FAIL(generate_task_for_shrink_(
                        dest_ls.at(dest_index),
                        balance_info.get_normal_ls_array().at(j)))) {
            LOG_WARN("failed to generate task for shrink", KR(ret), K(dest_index), K(dest_ls), K(j), K(balance_info));
          } else
            dest_index++;
        }
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_task_for_shrink_(
    const ObSplitLSParamArray &src_split_param,
    const ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!job_.is_valid() || src_split_param.count() <= 0
                         || !ls_status_info.is_valid())) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), K(job_), K(src_split_param), K(ls_status_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_split_param.count(); ++i) {
      const ObSplitLSParam &param = src_split_param.at(i);
      ObLSID merge_ls_id;
      ObLSID target_ls_id = ls_status_info.ls_id_;
      if (fabs(param.get_current_factor() - 1.0) < OB_DOUBLE_EPSINON) {
        //nothing
        merge_ls_id = param.get_ls_info()->ls_id_;
      } else {
        if (param.get_ls_info()->ls_group_id_ == ls_status_info.ls_group_id_) {
          //need_transfer, no need merge
          if (OB_FAIL(generate_transfer_task_(param, ls_status_info))) {
            LOG_WARN("failed to generate transfer task", KR(ret), K(param));
          }
        } else {
          // need split
          ObSplitLSParamArray tmp_split_param;
          int64_t task_index = OB_INVALID_INDEX_INT64;
          if (OB_FAIL(tmp_split_param.push_back(param))) {
            LOG_WARN("failed to push back param", KR(ret), K(param));
          } else if (OB_FAIL(generate_ls_split_task_(tmp_split_param, target_ls_id, task_index))) {
            LOG_WARN("failed to generate ls info", KR(ret), K(tmp_split_param));
          } else {
            merge_ls_id = task_array_.at(task_index).get_dest_ls_id();
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (param.get_ls_info()->ls_group_id_ != ls_status_info.ls_group_id_) {
          //need alter task
          if (OB_FAIL(construct_ls_alter_task_(merge_ls_id, ls_status_info.ls_group_id_))) {
            LOG_WARN("failed to construct ls alter task", KR(ret), K(merge_ls_id), K(ls_status_info));
          }
        }
      }
      if (OB_SUCC(ret) && merge_ls_id.is_valid()) {
        //need merge
        if (OB_FAIL(construct_ls_merge_task_(merge_ls_id, target_ls_id,
                                            ls_status_info.ls_group_id_))) {
          LOG_WARN("failed to construct ls merge task", KR(ret), K(merge_ls_id), K(ls_status_info));
        }
      }
    }//end for
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_task_for_dup_ls_shrink_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(dup_ls_stat_array_.count() <= 1
      || unit_group_balance_array_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid array", KR(ret), K(dup_ls_stat_array_), K(unit_group_balance_array_));
  } else {
    ObLSStatusInfo::Compare cmp;
    lib::ob_sort(dup_ls_stat_array_.begin(), dup_ls_stat_array_.end(), cmp);
    const ObLSID dest_ls_id = dup_ls_stat_array_.at(0).get_ls_id(); // smallest dup ls id
    const uint64_t dest_ls_group_id = dup_ls_stat_array_.at(0).get_ls_group_id();
    const uint64_t src_ls_group_id = dup_ls_stat_array_.at(1).get_ls_group_id();
    uint64_t other_ls_group_id = OB_INVALID_ID;
    uint64_t chosen_ls_group_id = OB_INVALID_ID;
    // 1.choose a valid ls_group_id
    ARRAY_FOREACH_X(unit_group_balance_array_, i, cnt, OB_INVALID_ID == other_ls_group_id) {
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (!balance_info.is_active_unit_group()) {
        // skip
      } else {
        ARRAY_FOREACH_X(balance_info.get_normal_ls_array(), j, count, OB_INVALID_ID == other_ls_group_id) {
          const ObLSStatusInfo &ls_info = balance_info.get_normal_ls_array().at(j);
          if (ls_info.is_valid() || 0 != ls_info.get_ls_group_id()) {
            other_ls_group_id = ls_info.get_ls_group_id();
          }
        }
      }
    }
    if (FAILEDx(choose_ls_group_id_for_transfer_between_dup_ls(
        src_ls_group_id,
        dest_ls_group_id,
        other_ls_group_id,
        chosen_ls_group_id))) {
      LOG_WARN("choose ls_group_id for transfer between dup ls failed",
          KR(ret), K(src_ls_group_id), K(dest_ls_group_id), K(other_ls_group_id));
    }
    // 2. all redundant dup ls merge to the dup ls with smallest id
    ARRAY_FOREACH(dup_ls_stat_array_, idx) {
      ObLSID src_ls_id;
      const ObLSStatusInfo &ls_status = dup_ls_stat_array_.at(idx);
      const ObLSID &ls_id = ls_status.get_ls_id();
      if (OB_UNLIKELY(!ls_status.is_duplicate_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ls status", KR(ret), K(ls_status));
      } else if (chosen_ls_group_id != ls_status.get_ls_group_id()
          && OB_FAIL(construct_ls_alter_task_(ls_id, chosen_ls_group_id))) {
        LOG_WARN("construct ls alter task failed", KR(ret), K(ls_id), K(chosen_ls_group_id));
      } else if (0 == idx) {
        // skip
      } else {
        src_ls_id = ls_status.get_ls_id();
        if (OB_FAIL(construct_ls_merge_task_(src_ls_id, dest_ls_id, chosen_ls_group_id))) {
          LOG_WARN("construct ls merge task failed", KR(ret), K(src_ls_id), K(dest_ls_id), K(chosen_ls_group_id));
        }
      }
    }
    // 3. alter dest dup ls to original ls_group_id
    chosen_ls_group_id = 0;
    if (FAILEDx(construct_ls_alter_task_(dest_ls_id, chosen_ls_group_id))) {
      LOG_WARN("construct ls alter task failed", KR(ret), K(dest_ls_id), K(chosen_ls_group_id));
    }
  }

  return ret;
}

int ObLSBalanceTaskHelper::generate_transfer_task_(
    const ObSplitLSParam &param, const ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !ls_status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(param), K(ls_status_info));
  } else {
    ObTransferPartList part_list;
    if (OB_FAIL(construct_ls_part_info_(param, ls_status_info.ls_id_, part_list))) {
      LOG_WARN("failed to construct ls part info", KR(ret), K(param));
    } else if (OB_FAIL(add_ls_transfer_task(
        tenant_id_,
        job_.get_job_id(),
        ls_status_info.ls_group_id_,
        param.get_ls_info()->ls_id_,
        ls_status_info.ls_id_,
        part_list,
        job_.get_balance_strategy(),
        task_array_))) {
      LOG_WARN("add ls transfer task failed", KR(ret), K(tenant_id_), K(job_), K(part_list));
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_shrink_src_param_(
    const int64_t target_count,
    ObSplitLSParamArray &src_ls,
    ObIArray<ObSplitLSParamArray> &dest_split_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(0 == target_count || 0 == src_ls.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_count), K(src_ls));
  } else {
    const double each_ls_target_factor = double(src_ls.count()) / (target_count);
    if (each_ls_target_factor <= OB_DOUBLE_EPSINON) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too many ls", KR(ret), K(each_ls_target_factor), K(target_count), K(src_ls));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_count; ++i) {
      double need_factor = each_ls_target_factor;
      ObSplitLSParamArray src_array;
      for (int64_t j = 0; OB_SUCC(ret) && j < src_ls.count() && need_factor > OB_DOUBLE_EPSINON; ++j) {
        ObSplitLSParam &param = src_ls.at(j);
        double get_factor = param.reduce_enough_factor(need_factor);
        if (!(get_factor)) { // strictly equal to zero
          //empty
        } else if (OB_DOUBLE_EPSINON >= get_factor) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("factor is too small", KR(ret), K(need_factor), K(src_ls), K(src_array), K(dest_split_array));
        } else {
          need_factor -= get_factor;
          if (OB_DOUBLE_EPSINON >= param.get_current_factor()) {
            param.reduce_all();
            //for ex
            //if current ls is 3, need shrink to 2, first ls need transfer, second need merge
            get_factor = 1;
          }
          ObSplitLSParam split_param(param.get_ls_info(), get_factor);
          LOG_TRACE("split param", KR(ret), K(split_param), K(i), K(j));
          if (OB_FAIL(src_array.push_back(split_param))) {
            LOG_WARN("failed to push back split param", KR(ret), K(split_param));
          }
        }
      }//end for j
      if (FAILEDx(dest_split_array.push_back(src_array))) {
        LOG_WARN("failed to push back src array", KR(ret), K(i), K(src_array));
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_expand_dest_param_(
    const int64_t lack_ls_count,
    ObSplitLSParamArray &src_ls,
    ObIArray<ObSplitLSParamArray> &dest_split_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(0 == lack_ls_count || 0 == src_ls.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(lack_ls_count), K(src_ls));
  } else {
    const double each_ls_target_factor = double(src_ls.count()) / (src_ls.count() + lack_ls_count);
    if (each_ls_target_factor <= OB_DOUBLE_EPSINON) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too many lack ls count", KR(ret), K(each_ls_target_factor), K(lack_ls_count), K(src_ls));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < lack_ls_count; ++i) {
      double need_factor = each_ls_target_factor;
      ObSplitLSParamArray src_array;
      for (int64_t j = 0; OB_SUCC(ret) && j < src_ls.count() && need_factor > OB_DOUBLE_EPSINON; ++j) {
        ObSplitLSParam &param = src_ls.at(j);
        double get_factor = param.reduce_factor_for_dest(need_factor, each_ls_target_factor);
        if (get_factor > OB_DOUBLE_EPSINON) {
          ObSplitLSParam split_param(param.get_ls_info(), get_factor);
          need_factor -= get_factor;
          if (OB_FAIL(src_array.push_back(split_param))) {
            LOG_WARN("failed to push back split param", KR(ret), K(split_param));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(0 >= src_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src array is empty", KR(ret), K(src_ls));
      } else if (OB_FAIL(dest_split_array.push_back(src_array))) {
        LOG_WARN("failed to push back src array", KR(ret), K(i), K(src_array));
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_balance_task_for_expand_(
    const ObSplitLSParamArray &dest_split_param, const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!job_.is_valid() || dest_split_param.count() <= 0
                         || OB_INVALID_ID == ls_group_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(job_), K(dest_split_param), K(ls_group_id));
  } else {
    //generate new ls info for split
    int64_t task_begin_index = OB_INVALID_INDEX_INT64;
    ObLSID target_ls_id;
    if (OB_FAIL(generate_ls_split_task_(dest_split_param, target_ls_id, task_begin_index))) {
      LOG_WARN("failed to generate ls info", KR(ret), K(dest_split_param));
    } else if (OB_UNLIKELY(task_begin_index < 0 || task_begin_index > task_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task_begin_index is invalid", KR(ret), K(task_begin_index));
    }

    for (int64_t i = task_begin_index; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      ObBalanceTask &task = task_array_.at(i);
      if (task.get_task_type().is_split_task()) {
        //由于上述split任务里面会生成transfer任务，所以这里只考虑分裂任务
        //只有分裂任务会生成新的日志流
        if (ls_group_id != task.get_ls_group_id()) {
          if (OB_FAIL(construct_ls_alter_task_(task.get_dest_ls_id(), ls_group_id))) {
            LOG_WARN("failed to init task", KR(ret), K(task), K(ls_group_id));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = task_begin_index + 1; OB_SUCC(ret) && i < task_array_.count(); ++i) {
        if (task_array_.at(i).get_task_type().is_split_task()) {
          if (OB_FAIL(construct_ls_merge_task_(
              task_array_.at(i).get_dest_ls_id(),
              target_ls_id,
              ls_group_id))) {
            LOG_WARN("failed to construct ls merge task", KR(ret),
                K(task_array_.at(i)), K(target_ls_id), K(ls_group_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_ls_split_task_(
    const ObSplitLSParamArray &dest_split_param,
    share::ObLSID &target_ls_id,
    int64_t &task_begin_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!job_.is_valid() || dest_split_param.count() <= 0)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), K(job_), K(dest_split_param));
  }
  ObTransferPartList part_list;//TODO
  task_begin_index = task_array_.count();
  //记录last_ls_group_id用于减少ls_split任务。例如primary_zone2->3的场景。
  //没必要生成两个split任务，只需要生成一个ls_split + ls_transfer任务即可。
  //对于同一个日志流生成的多个分裂任务，如果前面的split任务和本任务是同一个ls_group_id
  //只需要生成transfer任务，减少无效的日志流和transfer
  uint64_t last_ls_group_id = OB_INVALID_ID;
  ObLSID dest_ls_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < dest_split_param.count(); ++i) {
    // split task has equal ls group id with source
    //TODO part_list fill partition_info of task
    const share::ObLSStatusInfo *src_ls = dest_split_param.at(i).get_ls_info();
    if (OB_ISNULL(src_ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src ls is null", KR(ret), K(i), K(dest_split_param));
    } else if (OB_INVALID_ID == last_ls_group_id || last_ls_group_id != src_ls->ls_group_id_) {
      // If the ls_group is not the same as the previous task, a split task needs to be generated and a new LS is required.
      if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(sql_proxy_, tenant_id_, dest_ls_id))) {
        LOG_WARN("failed to fetch new ls id", KR(ret), K_(tenant_id));
      } else if (!target_ls_id.is_valid() && FALSE_IT(target_ls_id = dest_ls_id)) {
      } else if (OB_FAIL(construct_ls_part_info_(dest_split_param.at(i), target_ls_id, part_list))) {
        LOG_WARN("failed to construct ls part info", KR(ret), KPC(src_ls));
      } else if (OB_FAIL(add_ls_split_task(
        sql_proxy_,
        tenant_id_,
        job_.get_job_id(),
        src_ls->ls_group_id_,
        src_ls->ls_id_,
        part_list,
        job_.get_balance_strategy(),
        dest_ls_id,
        task_array_))) {
        LOG_WARN("add ls split task failed", KR(ret), K(tenant_id_), K(job_), KPC(src_ls), K(dest_ls_id), K(part_list));
      } else {
        last_ls_group_id = src_ls->ls_group_id_;
      }
    } else { // If the task belongs to the same ls_group, the transfer task can be directly generated without creating LS
      if (OB_UNLIKELY(!dest_ls_id.is_valid() || !target_ls_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last ls group id is valid, ls_id is invalid",
            KR(ret), K(last_ls_group_id), K(dest_ls_id), K(target_ls_id));
      } else if (OB_FAIL(construct_ls_part_info_(dest_split_param.at(i), target_ls_id, part_list))) {
        LOG_WARN("failed to construct ls part info", KR(ret), KPC(src_ls));
      } else if (OB_FAIL(add_ls_transfer_task(
          tenant_id_,
          job_.get_job_id(),
          src_ls->ls_group_id_,
          src_ls->ls_id_,
          dest_ls_id,
          part_list,
          job_.get_balance_strategy(),
          task_array_))) {
        LOG_WARN("add ls transfer task failed", KR(ret), K(tenant_id_),
            K(job_), KPC(src_ls), K(dest_ls_id), K(part_list));
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_ls_alter_task_(
    const share::ObLSID &ls_id,
    const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!job_.is_valid() || !ls_id.is_valid()
                         || OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_), K(ls_id), K(ls_group_id));
  } else if (OB_FAIL(add_ls_alter_task(
      tenant_id_,
      job_.get_job_id(),
      ls_group_id,
      ls_id,
      job_.get_balance_strategy(),
      task_array_))) {
    LOG_WARN("add ls alter task failed", KR(ret), K(tenant_id_), K(job_), K(ls_group_id), K(ls_id));
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_ls_merge_task_(
    const share::ObLSID &src_ls_id, const share::ObLSID &dest_ls_id,
    const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!job_.is_valid() || !src_ls_id.is_valid()
                         || OB_INVALID_ID == ls_group_id
                         || !dest_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_), K(src_ls_id), K(ls_group_id), K(dest_ls_id));
  } else if (OB_FAIL(add_ls_merge_task(
      tenant_id_,
      job_.get_job_id(),
      ls_group_id,
      src_ls_id,
      dest_ls_id,
      job_.get_balance_strategy(),
      task_array_))) {
    LOG_WARN("add ls merge task failed", KR(ret), K(tenant_id_), K(job_),
        K(ls_group_id), K(dest_ls_id), K(src_ls_id));
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_ls_part_info_(
    const ObSplitLSParam &src_ls,
    const share::ObLSID &dest_ls_id,
    ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObLSID src_ls_id = src_ls.get_ls_id();
  const double factor = src_ls.get_current_factor();
  ObLSBalanceGroupInfo *src_ls_bg_info = NULL;
  ObLSBalanceGroupInfo *dst_ls_bg_info = NULL;

  part_list.reset();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!src_ls.is_valid() || !src_ls_id.is_valid() || !dest_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls or dest ls is invalid", KR(ret), K(src_ls), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(tenant_ls_bg_info_.get(src_ls_id, src_ls_bg_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ISTAT("src ls is empty, no need to transfer out", KR(ret), K(src_ls_id));
    } else {
      LOG_WARN("get src ls balance group info fail", KR(ret), K(src_ls_id), K(src_ls));
    }
  } else if (OB_FAIL(tenant_ls_bg_info_.get_or_create(dest_ls_id, dst_ls_bg_info))) {
    LOG_WARN("get dest ls balance group info fail", KR(ret), K(dest_ls_id));
  } else if (OB_ISNULL(src_ls_bg_info) || OB_ISNULL(dst_ls_bg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls balance group info", KR(ret), K(src_ls_bg_info), K(src_ls_id),
        K(dst_ls_bg_info), K(dest_ls_id));
  } else if (OB_FAIL(src_ls_bg_info->transfer_out_by_factor(*dst_ls_bg_info, factor, part_list))) {
    LOG_WARN("transfer out part list from LS balance group info fail", KR(ret), K(factor),
        KPC(src_ls_bg_info), KPC(dst_ls_bg_info), K(part_list));
  }
  return ret;
}

#define GEN_BALANCE_TASK(task_type, ls_group_id, src_ls, dest_ls, part_list, balance_strategy) \
  do {                                                                                         \
    if (OB_SUCC(ret)) {                                                                        \
      ObBalanceTask task;                                                                      \
      ObBalanceTaskID task_id;                                                                 \
      if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, task_id))) {                       \
        LOG_WARN("gen_unique_id", KR(ret), K(tenant_id));                                      \
      } else if (OB_FAIL(task.simple_init(tenant_id, balance_job_id, task_id,                  \
          task_type, ls_group_id, src_ls, dest_ls, part_list, balance_strategy))) {            \
        LOG_WARN("init task fail", KR(ret), K(tenant_id), K(balance_job_id), K(task_id),       \
            K(ls_group_id), K(src_ls), K(dest_ls), K(part_list), K(balance_strategy));         \
      } else if (OB_FAIL(task_array.push_back(task))) {                                        \
        LOG_WARN("push_back fail", KR(ret), K(task));                                          \
      } else {                                                                                 \
        LOG_INFO("gen balance task successfully", K(task));                                    \
      }                                                                                        \
    }                                                                                          \
  } while (0)

int ObLSBalanceTaskHelper::add_ls_alter_task(
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObBalanceStrategy &balance_strategy,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_ALTER);
    ObTransferPartList empty_part_list;
    ObLSID dest_ls_id; // -1
    GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, dest_ls_id, empty_part_list, balance_strategy);
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_ls_transfer_task(
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObTransferPartList &part_list,
    const share::ObBalanceStrategy &balance_strategy,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || !dest_ls_id.is_valid()
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(dest_ls_id), K(part_list), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_TRANSFER);
    GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, dest_ls_id, part_list, balance_strategy);
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_ls_split_task(
    ObMySQLProxy *sql_proxy,
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObTransferPartList &part_list,
    const share::ObBalanceStrategy &balance_strategy,
    share::ObLSID &new_ls_id,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  // part_list may be empty when split a empty LS
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || OB_ISNULL(sql_proxy)
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(part_list), KP(sql_proxy), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_SPLIT);
    if (!new_ls_id.is_valid()
        && OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(sql_proxy, tenant_id, new_ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    } else {
      GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, new_ls_id, part_list, balance_strategy);
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_ls_merge_task(
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObBalanceStrategy &balance_strategy,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || !dest_ls_id.is_valid()
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(dest_ls_id), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_MERGE);
    ObTransferPartList empty_part_list;
    GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, dest_ls_id, empty_part_list, balance_strategy);
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_create_ls_task(
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObBalanceStrategy &balance_strategy,
      ObMySQLProxy *sql_proxy,
      common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(balance_strategy), KP(sql_proxy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_CREATE);
    ObTransferPartList empty_part_list;
    ObLSID ls_id;
    if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(sql_proxy, tenant_id, ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    } else {
      GEN_BALANCE_TASK(task_type, ls_group_id, ls_id, ls_id, empty_part_list, balance_strategy);
    }
  }
  return ret;

}

// if ls_group_id of both src_ls and dest_ls are 0, choose other valid ls_group_id
int ObLSBalanceTaskHelper::choose_ls_group_id_for_transfer_between_dup_ls(
    const uint64_t src_ls_group_id,
    const uint64_t dest_ls_group_id,
    const uint64_t other_ls_group_id,
    uint64_t &chosen_ls_group_id)
{
  int ret = OB_SUCCESS;
  chosen_ls_group_id = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_ID == src_ls_group_id
      || OB_INVALID_ID == dest_ls_group_id
      || OB_INVALID_ID == other_ls_group_id
      || 0 == other_ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls_group_id), K(dest_ls_group_id), K(other_ls_group_id));
  } else if (0 != dest_ls_group_id) {
    chosen_ls_group_id = dest_ls_group_id;
  } else if (0 != src_ls_group_id) {
    chosen_ls_group_id = src_ls_group_id;
  } else { // ls_group_id of both src_ls and dest_ls are 0, use a valid ls_group_id
    chosen_ls_group_id = other_ls_group_id;
  }
  LOG_INFO("choose ls_group_id for transfer between dup ls finshed", KR(ret),
      K(chosen_ls_group_id), K(src_ls_group_id), K(dest_ls_group_id), K(other_ls_group_id));
  return ret;
}

#undef ISTAT
#undef WSTAT

}
}
