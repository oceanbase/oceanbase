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

#define USING_LOG_PREFIX COMMON
#include "ob_dag_warning_history_mgr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"

namespace oceanbase
{
using namespace common;
namespace share
{

/*
 * ObDagWarningInfo Func
 * */


const char * ObDagWarningInfo::ObDagStatusStr[ODS_MAX] = { "WARNING", "RETRYED"};

const char * ObDagWarningInfo::get_dag_status_str(enum ObDagStatus status)
{
  const char *str = "";
  if (status >= ODS_MAX || status < ODS_WARNING) {
    str = "invalid_type";
  } else {
    str = ObDagStatusStr[status];
  }
  return str;
}

ObDagWarningInfo::ObDagWarningInfo() :
    tenant_id_(0),
    task_id_(),
    dag_type_(share::ObDagType::DAG_TYPE_MAX),
    dag_ret_(OB_SUCCESS),
    dag_status_(ODS_MAX),
    gmt_create_(0),
    gmt_modified_(0),
    retry_cnt_(0),
    warning_info_()
{
  MEMSET(warning_info_, '\0', common::OB_DAG_WARNING_INFO_LENGTH);
}

ObDagWarningInfo::~ObDagWarningInfo()
{
  reset();
}

bool ObDagWarningInfo::operator == (const ObDagWarningInfo &other) const
{
  bool bret = false;
  bret = tenant_id_ == other.tenant_id_
      && task_id_.equals(other.task_id_)
      && dag_type_ == other.dag_type_
      && dag_ret_ == other.dag_ret_
      && dag_status_ == other.dag_status_
      && gmt_create_ == other.gmt_create_
      && gmt_modified_ == other.gmt_modified_
      && retry_cnt_ == other.retry_cnt_
      && 0 == strcmp(warning_info_, other.warning_info_);
  return bret;
}

ObDagWarningInfo & ObDagWarningInfo::operator = (const ObDagWarningInfo &other)
{
  tenant_id_ = other.tenant_id_;
  task_id_ = other.task_id_;
  dag_type_ = other.dag_type_;
  dag_ret_ = other.dag_ret_;
  dag_status_ = other.dag_status_;
  gmt_create_ = other.gmt_create_;
  gmt_modified_ = other.gmt_modified_;
  retry_cnt_ = other.retry_cnt_;
  strncpy(warning_info_, other.warning_info_, strlen(other.warning_info_) + 1);
  return *this;
}

/*
 * ObDagWarningHistoryManager Func
 * */

int ObDagWarningHistoryManager::add_dag_warning_info(share::ObIDag *dag)
{
  int ret = OB_SUCCESS;
  const int64_t key = dag->hash();
  common::SpinWLockGuard guard(lock_);
  if (OB_SUCCESS == dag->get_dag_ret()) { // delete old item
    if (OB_FAIL(del_with_no_lock(key))) {
      if (OB_HASH_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "failed to del dag warning info", K(ret), K(key));
      } else {
        ret = OB_SUCCESS;
      }
    }
  } else if (!dag->ignore_warning()) {
    ObDagWarningInfo *info = NULL;
    if (OB_FAIL(get_with_no_lock(key, info)) || OB_ISNULL(info)) {
      if (OB_HASH_NOT_EXIST == ret) {// first add
        if (OB_FAIL(alloc_and_add_with_no_lock(key, info)) || OB_ISNULL(info)) { // alloc a node for key
          if (OB_SIZE_OVERFLOW != ret) {
            COMMON_LOG(WARN, "failed to alloc dag warning info", K(ret), K(key), K(info));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          dag->gene_warning_info(*info);
          dag->gene_basic_warning_info(*info); // only once
          info->dag_status_ = ObDagWarningInfo::ODS_WARNING;
        }
      } else {
        COMMON_LOG(WARN, "failed to get dag warning info", K(ret), K(key), K(info));
      }
    } else { // update
      dag->gene_warning_info(*info);
      info->retry_cnt_++;
      info->dag_status_ = ObDagWarningInfo::ODS_RETRYED;
    }
  }
  return ret;
}

int ObDagWarningHistoryManager::get_info(const int64_t pos, ObDagWarningInfo &info)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  if (pos >= max_cnt_) {
    ret = OB_ITER_END;
  } else if (node_array_.is_empty(pos)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    info = *(node_array_.at(pos));
  }
  return ret;
}

/*
 * ObDagWarningInfoIterator Func
 * */

int ObDagWarningInfoIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObDagWarnInfoIterator has been opened", K(ret));
  } else if (!::oceanbase::common::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObDagWarningInfoIterator::get_next_info(ObDagWarningInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    do {
      ret = ObDagWarningHistoryManager::get_instance().get_info(cur_idx_, info);
      ++cur_idx_;
    } while (OB_ENTRY_NOT_EXIST == ret
        || (OB_SYS_TENANT_ID != tenant_id_ && info.tenant_id_ != tenant_id_));
  }
  return ret;
}

}//storage
}//oceanbase
