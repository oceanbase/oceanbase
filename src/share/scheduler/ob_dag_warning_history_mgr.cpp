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
    compaction::ObIDiagnoseInfo(),
    task_id_(),
    dag_type_(share::ObDagType::DAG_TYPE_MAX),
    dag_ret_(OB_SUCCESS),
    dag_status_(ODS_MAX),
    gmt_create_(0),
    gmt_modified_(0),
    retry_cnt_(0),
    hash_(0),
    location_()
{
}

ObDagWarningInfo::~ObDagWarningInfo()
{
  reset();
}

void ObDagWarningInfo::shallow_copy(ObIDiagnoseInfo *other)
{
  ObDagWarningInfo *info = nullptr;
  if (OB_NOT_NULL(other) && OB_NOT_NULL(info = dynamic_cast<ObDagWarningInfo *>(other))) {
    tenant_id_ = info->tenant_id_;
    priority_ = info->priority_;
    task_id_ = info->task_id_;
    dag_type_ = info->dag_type_;
    dag_ret_ = info->dag_ret_;
    dag_status_ = info->dag_status_;
    gmt_create_ = info->gmt_create_;
    gmt_modified_ = info->gmt_modified_;
    retry_cnt_ = info->retry_cnt_;
    hash_ = info->hash_;
    location_ = info->location_;
  }
}

void ObDagWarningInfo::update(ObIDiagnoseInfo *other)
{
  ObDagWarningInfo *info = nullptr;
  if (OB_NOT_NULL(other) && OB_NOT_NULL(info = dynamic_cast<ObDagWarningInfo *>(other))) {
    gmt_create_ = info->gmt_create_;
    retry_cnt_ = info->retry_cnt_+1;
    dag_status_ = ObDagWarningInfo::ODS_RETRYED;
  }
}

int64_t ObDagWarningInfo::get_hash() const
{
  return hash_;
}
/*
 * ObDagWarningHistoryManager Func
 * */

int ObDagWarningHistoryManager::mtl_init(ObDagWarningHistoryManager *&dag_warning_history)
{
  int64_t max_size = cal_max();
  return dag_warning_history->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE, max_size);
}

int64_t ObDagWarningHistoryManager::cal_max()
{
  const uint64_t tenant_id = MTL_ID();
  int64_t max_size = std::min(lib::get_tenant_memory_limit(tenant_id) * MEMORY_PERCENTAGE / 100,
                          static_cast<int64_t>(POOL_MAX_SIZE));
  return max_size;
}

int ObDagWarningHistoryManager::add_dag_warning_info(share::ObIDag *dag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagWarningHistoryManager is not init", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(dag));
  } else {
    const int64_t key = dag->hash();
    if (OB_SUCCESS == dag->get_dag_ret()) { // delete old item
      if (OB_FAIL(delete_info(key))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          COMMON_LOG(WARN, "failed to delete dag warning info when add info", K(ret));
        }
      }
    } else if (!dag->ignore_warning()) {
      ObDagWarningInfo tmp_info;
      tmp_info.hash_ = key;
      compaction::ObInfoParamBuffer allocator;
      if(OB_FAIL(dag->gene_warning_info(tmp_info, allocator))) {
        COMMON_LOG(WARN, "failed to gene dag warning info", K(ret));
      } else if (OB_FAIL(alloc_and_add(key, &tmp_info))) {
        COMMON_LOG(WARN, "failed to add dag warning info", K(ret));
      }
    }
  }
  return ret;
}

int ObDagWarningHistoryManager::add_dag_warning_info(ObDagWarningInfo &input_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObDagWarningHistoryManager is not init", K(ret));
  } else if (OB_ISNULL(input_info.info_param_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument. info param is null", K(ret), K(input_info));
  } else if (OB_FAIL(alloc_and_add(input_info.get_hash(), &input_info))) {
    COMMON_LOG(WARN, "failed to add dag warning info", K(ret));
  }
  return ret;
}
}//storage
}//oceanbase
