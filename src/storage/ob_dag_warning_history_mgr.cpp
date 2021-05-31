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

#define USING_LOG_PREFIX STORAGE
#include "ob_dag_warning_history_mgr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
using namespace common;
namespace storage {

/*
 * ObDagWarningInfo Func
 * */

ObDagWarningInfo::ObDagWarningInfo()
    : tenant_id_(0),
      task_id_(),
      dag_type_(share::ObIDag::DAG_TYPE_MAX),
      dag_ret_(OB_SUCCESS),
      dag_status_(ODS_MAX),
      gmt_create_(0),
      gmt_modified_(0),
      warning_info_()
{}

ObDagWarningInfo::~ObDagWarningInfo()
{
  reset();
}

bool ObDagWarningInfo::operator==(const ObDagWarningInfo& other) const
{
  bool bret = false;
  bret = tenant_id_ == other.tenant_id_ && task_id_.equals(other.task_id_) && dag_type_ == other.dag_type_ &&
         dag_ret_ == other.dag_ret_ && dag_status_ == other.dag_status_ && gmt_create_ == other.gmt_create_ &&
         gmt_modified_ == other.gmt_modified_ && 0 == strcmp(warning_info_, other.warning_info_);
  return bret;
}

ObDagWarningInfo& ObDagWarningInfo::operator=(const ObDagWarningInfo& other)
{
  tenant_id_ = other.tenant_id_;
  task_id_ = other.task_id_;
  dag_type_ = other.dag_type_;
  dag_ret_ = other.dag_ret_;
  dag_status_ = other.dag_status_;
  gmt_create_ = other.gmt_create_;
  gmt_modified_ = other.gmt_modified_;
  strncpy(warning_info_, other.warning_info_, strlen(other.warning_info_) + 1);
  return *this;
}

/*
 * ObDagWarningHistoryManager Func
 * */

int ObDagWarningHistoryManager::add_dag_warning_info(share::ObIDag* dag)
{
  int ret = OB_SUCCESS;
  const int64_t key = dag->hash();
  common::SpinWLockGuard guard(lock_);
  if (OB_SUCCESS == dag->get_dag_ret()) {  // delete old item
    if (OB_FAIL(del(key))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to del dag warning info", K(ret), K(key));
      }
    }
  } else if (!dag->ignore_warning()) {
    ObDagWarningInfo* info = NULL;
    if (OB_FAIL(get(key, info)) || OB_ISNULL(info)) {
      if (OB_HASH_NOT_EXIST == ret) {                                // first add
        if (OB_FAIL(alloc_and_add(key, info)) || OB_ISNULL(info)) {  // alloc a node for key
          if (OB_SIZE_OVERFLOW != ret) {
            STORAGE_LOG(WARN, "failed to alloc dag warning info", K(ret), K(key), K(info));
          }
        } else {
          dag->gene_warning_info(*info);
          dag->gene_basic_warning_info(*info);  // only once
          info->dag_status_ = ODS_WARNING;
        }
      } else {
        STORAGE_LOG(WARN, "failed to get dag warning info", K(ret), K(key), K(info));
      }
    } else {  // update
      dag->gene_warning_info(*info);
      info->dag_status_ = ODS_RETRYED;
    }
  }
  return ret;
}

int ObDagWarningHistoryManager::get_info(const int64_t pos, ObDagWarningInfo& info)
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

int ObDagWarningInfoIterator::open()
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObDagWarnInfoIterator has been opened, ", K(ret));
  } else {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObDagWarningInfoIterator::get_next_info(ObDagWarningInfo& info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    do {
      ret = ObDagWarningHistoryManager::get_instance().get_info(cur_idx_, info);
      ++cur_idx_;
    } while (OB_ENTRY_NOT_EXIST == ret);
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
