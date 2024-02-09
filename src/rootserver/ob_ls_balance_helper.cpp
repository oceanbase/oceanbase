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
#include "lib/mysqlclient/ob_mysql_transaction.h"//trans
#include "observer/ob_server_struct.h"//GCTX
#include "share/schema/ob_schema_getter_guard.h"//ObSchemaGetGuard
#include "share/schema/ob_multi_version_schema_service.h"//ObMultiSchemaService
#include "share/schema/ob_table_schema.h"//ObTableSchema
#include "share/ob_balance_define.h"  // ObBalanceTaskID, ObBalanceJobID
#include "storage/tx/ob_unique_id_service.h" // ObUniqueIDService
#include "storage/ob_common_id_utils.h"     // ObCommonIDUtils
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
  primary_zone_count_ = OB_INVALID_COUNT;
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
  } else if (normal_ls_array_.count() >= primary_zone_count_
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


//////////////ObLSBalanceTaskHelper

ObLSBalanceTaskHelper::ObLSBalanceTaskHelper() :
    inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    primary_zone_num_(0),
    unit_group_balance_array_(),
    sql_proxy_(NULL),
    job_(),
    task_array_(),
    tenant_ls_bg_info_()
{
}

int ObLSBalanceTaskHelper::init(const uint64_t tenant_id,
           const share::ObLSStatusInfoArray &status_array,
           const ObIArray<share::ObSimpleUnitGroup> &unit_group_array,
           const int64_t primary_zone_num, ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == status_array.count() || 0 == unit_group_array.count()
                  || 0 >= primary_zone_num || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status_array), K(unit_group_array),
                                 K(primary_zone_num), K(tenant_id));
  } else if (OB_FAIL(tenant_ls_bg_info_.init(tenant_id))) {
    LOG_WARN("init tenant LS balance group info fail", KR(ret), K(tenant_id));
  } else {
    //1. init all unit balance info
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array.count(); ++i) {
      ObUnitGroupBalanceInfo balance_info(unit_group_array.at(i), primary_zone_num);
      if (OB_FAIL(unit_group_balance_array_.push_back(balance_info))) {
        LOG_WARN("failed to push back balance info", KR(ret), K(balance_info), K(i));
      }
    }
    int64_t index = OB_INVALID_INDEX_INT64;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_array.count(); ++i) {
      const ObLSStatusInfo &ls_status = status_array.at(i);
      if (OB_FAIL(find_unit_group_balance_index(ls_status.unit_group_id_, index))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          //normal, ls status must has target unit_group,
          //but maybe migrate unit and ls group balance concurrency
          LOG_WARN("has ls in not valid unit group", KR(ret), K(ls_status), K(unit_group_array));
          ret = OB_SUCCESS;
          index = unit_group_balance_array_.count();
          ObSimpleUnitGroup unit_group(ls_status.unit_group_id_, ObUnit::UNIT_STATUS_DELETING);
          ObUnitGroupBalanceInfo balance_info(unit_group, primary_zone_num);
          if (OB_FAIL(unit_group_balance_array_.push_back(balance_info))) {
            LOG_WARN("failed to push back balance info", KR(ret), K(balance_info));
          }
        } else {
          LOG_WARN("failed to find index", KR(ret), K(ls_status));
        }
      }
      if (FAILEDx(unit_group_balance_array_.at(index).add_ls_status_info(ls_status))) {
        LOG_WARN("failed to add ls status info", KR(ret), K(ls_status));
      }
    }
  }
  if (OB_SUCC(ret)) {
    primary_zone_num_ = primary_zone_num;
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    job_.reset();
    task_array_.reset();
    inited_ = true;
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
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(unit_group_balance_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group balance array not expected", KR(ret));
  } else {
    need_balance = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count() && !need_balance; ++i) {
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.get_lack_ls_count() > 0 || balance_info.get_redundant_ls_array().count() > 0) {
        //has more ls or less ls
        need_balance = true;
        ISTAT("has more or less ls, need balance", K(balance_info));
      }
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::generate_ls_balance_task()
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(generate_balance_job_())) {
    LOG_WARN("failed to generate job", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or schema service is null", KR(ret), K(sql_proxy_), K(schema_service));
  }
  // build tenant all balance group info for ALL LS
  else if (OB_FAIL(tenant_ls_bg_info_.build("LS_BALANCE", *sql_proxy_, *schema_service))) {
    LOG_WARN("build tenant all balance group info for all LS fail", KR(ret));
  } else {
    if (0 == job_.get_balance_strategy().string().compare(share::LS_BALANCE_BY_ALTER)) {
      if (OB_FAIL(generate_alter_task_())) {
        LOG_WARN("failed to generate alter task", KR(ret));
      }
    } else if (0 == job_.get_balance_strategy().string().compare(share::LS_BALANCE_BY_MIGRATE)) {
      // 1. first migrate task
      if (OB_FAIL(generate_migrate_task_())) {
        LOG_WARN("failed to generate migrate task", KR(ret));
      }
    } else if (0 == job_.get_balance_strategy().string().compare(share::LS_BALANCE_BY_EXPAND)) {
    //2. try expand
      if (OB_FAIL(generate_expand_task_())) {
        LOG_WARN("failed to generate expand task", KR(ret));
      }
    } else if (0 == job_.get_balance_strategy().string().compare(share::LS_BALANCE_BY_SHRINK)) {
    //3. try shrink
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
  } else {
    bool lack_ls = false;
    bool redundant_ls = false;
    bool need_modify_ls_group = false;
    ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_LS);
    ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
    int64_t unit_group_num = 0;
    ObBalanceJobID job_id;
    ObString comment;
    const char* balance_stradegy = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.is_active_unit_group()) {
        unit_group_num++;
      }
      if (balance_info.get_lack_ls_count() > 0) {
        lack_ls = true;
        ISTAT("unit group has little ls than expected", K(balance_info));
      }
      if (balance_info.get_redundant_ls_array().count() > 0) {
        redundant_ls = true;
        ISTAT("unit group has more ls than expected", K(balance_info));
      }
      uint64_t ls_group_id = OB_INVALID_ID;
      for (int64_t j = 0;
           OB_SUCC(ret) && j < balance_info.get_normal_ls_array().count() &&
           !need_modify_ls_group; ++j) {
        const ObLSStatusInfo &ls_status_info = balance_info.get_normal_ls_array().at(j);
        if (OB_INVALID_ID == ls_status_info.ls_group_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group id not expected", KR(ret), K(ls_status_info));
        } else if (OB_INVALID_ID == ls_group_id) {
          ls_group_id = ls_status_info.ls_group_id_;
        } else if (ls_group_id != ls_status_info.ls_group_id_) {
          need_modify_ls_group = true;
          ISTAT("unit group has different ls group", K(ls_group_id), K(ls_status_info), K(balance_info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (need_modify_ls_group) {
        balance_stradegy = share::LS_BALANCE_BY_ALTER;
      } else if (lack_ls && redundant_ls) {
        balance_stradegy = share::LS_BALANCE_BY_MIGRATE;
      } else if (lack_ls) {
        balance_stradegy = share::LS_BALANCE_BY_EXPAND;
      } else if (redundant_ls) {
        balance_stradegy = share::LS_BALANCE_BY_SHRINK;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("must has balance job for ls", KR(ret), K(unit_group_balance_array_));
      }

      if (FAILEDx(ObCommonIDUtils::gen_unique_id(tenant_id_, job_id))) {
        LOG_WARN("generate unique id for balance job fail", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(job_.init(tenant_id_, job_id, job_type, job_status, primary_zone_num_,
              unit_group_num, comment, ObString(balance_stradegy)))) {
        LOG_WARN("failed to init job", KR(ret), K(tenant_id_), K(job_id), K(job_type),
            K(job_status), K(primary_zone_num_), K(unit_group_num), K(balance_stradegy));
      }
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
    uint64_t ls_group_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      ls_group_id = OB_INVALID_ID;
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_array().count(); ++j) {
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

int ObLSBalanceTaskHelper::generate_migrate_task_()
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
      for (int64_t j = balance_info.get_redundant_ls_array().count() - 1; OB_SUCC(ret) && j >= 0 && new_task; --j) {
        //get one unit group, which less than primary_zone_unit_num
        const ObLSStatusInfo &ls_status = balance_info.get_redundant_ls_array().at(j);
        new_task = false;
        for (int64_t k = 0; OB_SUCC(ret) && k < unit_group_balance_array_.count(); ++k) {
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

int ObLSBalanceTaskHelper::generate_ls_alter_task_(const ObLSStatusInfo &ls_status_info, ObUnitGroupBalanceInfo &dest_unit_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_status_info.is_valid()
                      || dest_unit_group.get_lack_ls_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_status_info), K(dest_unit_group));
  } else {
    uint64_t ls_group_id = OB_INVALID_ID;
    ObLSStatusInfo dest_ls_status;
    if (dest_unit_group.get_normal_ls_array().count() > 0) {
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
  } else {
    int64_t lack_count = 0;
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    const double src_factor = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo & balance_info = unit_group_balance_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_array().count(); ++j) {
        ObSplitLSParam param(&balance_info.get_normal_ls_array().at(j), src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        lack_count += balance_info.get_lack_ls_count();
      }
    }
    if (FAILEDx(construct_expand_dest_param_(lack_count, src_ls, dest_ls))) {
      LOG_WARN("failed to construct expand dest param", KR(ret), K(lack_count), K(src_ls));
    }
    int64_t dest_ls_index = 0;
    uint64_t ls_group_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      ls_group_id = OB_INVALID_ID;
      const ObUnitGroupBalanceInfo &balance_info = unit_group_balance_array_.at(i);
      if (balance_info.get_normal_ls_array().count() > 0) {
        ls_group_id = balance_info.get_normal_ls_array().at(0).ls_group_id_;
      } else if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_, tenant_id_, ls_group_id))) {
        LOG_WARN("failed to fetch new ls group id", KR(ret), K(tenant_id_));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_lack_ls_count(); ++j) {
        if (OB_UNLIKELY(dest_ls_index >= dest_ls.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest ls index not expected", KR(ret), K(dest_ls_index));
        } else if (OB_FAIL(generate_balance_task_for_expand_(dest_ls.at(dest_ls_index),
                                                      ls_group_id))) {
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
  } else {
    const int64_t normal_ls_count = job_.get_primary_zone_num() * job_.get_unit_group_num();
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    const double src_factor = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo & balance_info = unit_group_balance_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_redundant_ls_array().count(); ++j) {
        ObSplitLSParam param(&balance_info.get_redundant_ls_array().at(j), src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(i), K(j));
        }
      }
    }
    if (FAILEDx(construct_shrink_src_param_(normal_ls_count, src_ls, dest_ls))) {
      LOG_WARN("failed to construct expand dest param", KR(ret), K(normal_ls_count), K(src_ls));
    }
    int64_t dest_index = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_balance_array_.count(); ++i) {
      const ObUnitGroupBalanceInfo & balance_info = unit_group_balance_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_info.get_normal_ls_array().count(); ++j) {
        if (OB_UNLIKELY(dest_ls.count() < dest_index)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src ls is unexpected", KR(ret), K(dest_ls), K(dest_ls));
        } else if (OB_FAIL(generate_task_for_shrink_(
                       dest_ls.at(dest_index++),
                       balance_info.get_normal_ls_array().at(j)))) {
          LOG_WARN("failed to generate task for shrink", KR(ret), K(dest_index), K(dest_ls), K(j), K(balance_info));
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
          } else if (OB_FAIL(generate_ls_split_task_(tmp_split_param, task_index))) {
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
        if (OB_FAIL(construct_ls_merge_task_(merge_ls_id, ls_status_info.ls_id_,
                                            ls_status_info.ls_group_id_))) {
          LOG_WARN("failed to construct ls merge task", KR(ret), K(merge_ls_id), K(ls_status_info));
        }
      }
    }//end for
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
    ObBalanceTaskType task_type(
        ObBalanceTaskType::BALANCE_TASK_TRANSFER);  // transfer task
    ObBalanceTask task;
    ObTransferPartList part_list;
    ObBalanceTaskID task_id;
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, task_id))) {
      LOG_WARN("gen_unique_id for balance task failed", KR(ret), K(task_id),
               K_(tenant_id));
    } else if (OB_FAIL(construct_ls_part_info_(param, part_list))) {
      LOG_WARN("failed to construct ls part info", KR(ret), K(param));
    } else if (OB_FAIL(task.simple_init(
                   tenant_id_, job_.get_job_id(), task_id, task_type,
                   ls_status_info.ls_group_id_,
                   param.get_ls_info()->ls_id_, ls_status_info.ls_id_, part_list))) {
      LOG_WARN("failed to init task", KR(ret), K(tenant_id_), K(job_),
               K(task_id), K(task_type), K(part_list));
    } else if (OB_FAIL(task_array_.push_back(task))) {
      LOG_WARN("failed to push back task", KR(ret), K(task));
    }
    ISTAT("generate transfer task", KR(ret), K(task), K(job_));
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_shrink_src_param_(const int64_t target_count, ObSplitLSParamArray &src_ls,
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

int ObLSBalanceTaskHelper::construct_expand_dest_param_(const int64_t lack_ls_count, ObSplitLSParamArray &src_ls,
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
    if (OB_FAIL(generate_ls_split_task_(dest_split_param, task_begin_index))) {
      LOG_WARN("failed to generate ls info", KR(ret), K(dest_split_param));
    } else if (OB_UNLIKELY(task_begin_index < 0 || task_begin_index > task_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task_begin_index is invalid", KR(ret), K(task_begin_index));
    }

    for (int64_t i = task_begin_index; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      if (ls_group_id != task_array_.at(i).get_ls_group_id()) {
        if (OB_FAIL(construct_ls_alter_task_(task_array_.at(i).get_dest_ls_id(), ls_group_id))) {
          LOG_WARN("failed to init task", KR(ret), K(task_array_.at(i)), K(ls_group_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObLSID dest_ls_id = task_array_.at(task_begin_index).get_dest_ls_id();
      for (int64_t i = task_begin_index + 1; OB_SUCC(ret) && i < task_array_.count(); ++i) {
        if (task_array_.at(i).get_task_type().is_split_task()) {
          if (OB_FAIL(construct_ls_merge_task_(task_array_.at(i).get_dest_ls_id(),
                  dest_ls_id, ls_group_id))) {
            LOG_WARN("failed to construct ls merge task", KR(ret),
                K(task_array_.at(i)), K(dest_ls_id), K(ls_group_id));
          }
        }
      }
    }
  }
  return ret;
}
int ObLSBalanceTaskHelper::generate_ls_split_task_(const ObSplitLSParamArray &dest_split_param,
                                                       int64_t &task_begin_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!job_.is_valid() || dest_split_param.count() <= 0)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), K(job_), K(dest_split_param));
  }
  ObBalanceTask task;
  ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_SPLIT);//split task
  ObTransferPartList part_list;//TODO
  task_begin_index = task_array_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < dest_split_param.count(); ++i) {
    // split task has equal ls group id with source
    //TODO part_list fill partition_info of task
    task.reset();
    ObLSID dest_ls_id;
    ObBalanceTaskID task_id;
    const share::ObLSStatusInfo *src_ls = dest_split_param.at(i).get_ls_info();
    if (OB_ISNULL(src_ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src ls is null", KR(ret), K(i), K(dest_split_param));
    } else if (OB_FAIL(construct_ls_part_info_(dest_split_param.at(i), part_list))) {
      LOG_WARN("failed to construct ls part info", KR(ret), KPC(src_ls));
    } else if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(sql_proxy_, tenant_id_, dest_ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, task_id))) {
      LOG_WARN("failed to gen unique id", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(task.simple_init(tenant_id_, job_.get_job_id(), task_id, task_type,
                          src_ls->ls_group_id_, src_ls->ls_id_, dest_ls_id,
                          part_list))) {
      LOG_WARN("failed to init task", KR(ret), K(tenant_id_), K(job_), K(task_id), K(task_type),
               KPC(src_ls), K(dest_ls_id), K(part_list));
    } else if (OB_FAIL(task_array_.push_back(task))) {
      LOG_WARN("failed to push back task", KR(ret), K(task));
    }
    ISTAT("generate split task", KR(ret), K(task), K(job_));
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_ls_alter_task_(const share::ObLSID &ls_id, const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!job_.is_valid() || !ls_id.is_valid()
                         || OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_), K(ls_id), K(ls_group_id));
  } else {
    //for alter
    ObBalanceTask task;
    ObBalanceTaskID task_id;
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_ALTER);
    ObTransferPartList part_list;
    ObLSID dest_ls_id;
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, task_id))) {
      LOG_WARN("failed to gen unique id", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(task.simple_init(tenant_id_, job_.get_job_id(),
                                            task_id, task_type, ls_group_id,
                                            ls_id,
                                            dest_ls_id,
                                            part_list))) {
      LOG_WARN("failed to init task", KR(ret), K(tenant_id_), K(job_),
                   K(task_id), K(task_type), K(ls_id), K(part_list));
    } else if (OB_FAIL(task_array_.push_back(task))) {
      LOG_WARN("failed to push back task", KR(ret), K(task));
    }
    ISTAT("generate alter task", KR(ret), K(task), K(job_));
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
  } else {
    //for merge
    ObBalanceTask task;
    ObBalanceTaskID task_id;
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_MERGE);// merge task
    ObTransferPartList part_list;
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, task_id))) {
      LOG_WARN("failed to gen unique id", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(task.simple_init(tenant_id_, job_.get_job_id(),
                                            task_id, task_type, ls_group_id,
                                            src_ls_id,
                                            dest_ls_id,
                                            part_list))) {
      LOG_WARN("failed to init task", KR(ret), K(tenant_id_), K(job_),
                   K(task_id), K(task_type), K(dest_ls_id), K(src_ls_id), K(part_list));
    } else if (OB_FAIL(task_array_.push_back(task))) {
      LOG_WARN("failed to push back task", KR(ret), K(task));
    }
    ISTAT("generate merge task", KR(ret), K(task), K(job_));
  }
  return ret;
}

int ObLSBalanceTaskHelper::construct_ls_part_info_(const ObSplitLSParam &src_ls, ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObLSID src_ls_id = src_ls.get_ls_id();
  const double factor = src_ls.get_current_factor();
  ObLSBalanceGroupInfo *ls_bg_info = NULL;

  part_list.reset();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!src_ls.is_valid() || !src_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls is invalid", KR(ret), K(src_ls), K(src_ls_id));
  } else if (OB_FAIL(tenant_ls_bg_info_.get(src_ls_id, ls_bg_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ISTAT("src ls is empty, no need to transfer out", KR(ret), K(src_ls_id));
    } else {
      LOG_WARN("get src ls balance group info fail", KR(ret), K(src_ls_id), K(src_ls));
    }
  } else if (OB_ISNULL(ls_bg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls balance group info", KR(ret), K(ls_bg_info), K(src_ls_id));
  } else if (OB_FAIL(ls_bg_info->transfer_out_by_factor(factor, part_list))) {
    LOG_WARN("transfer out part list from LS balance group info fail", KR(ret), K(factor),
        KPC(ls_bg_info), K(part_list));
  }
  return ret;
}

#undef ISTAT
#undef WSTAT

}
}
