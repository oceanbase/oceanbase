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

#define USING_LOG_PREFIX RS
#include "rootserver/backup/ob_cancel_validate_scheduler.h"
#include "share/backup/ob_backup_struct.h"

using namespace oceanbase::share;

namespace oceanbase {
namespace rootserver {

ObCancelValidateScheduler::ObCancelValidateScheduler()
    : is_inited_(false), tenant_id_(OB_INVALID_ID), job_id_(-1), root_validate_(NULL)
{}

ObCancelValidateScheduler::~ObCancelValidateScheduler()
{}

int ObCancelValidateScheduler::init(const uint64_t tenant_id, const int64_t job_id, common::ObMySQLProxy& sql_proxy,
    rootserver::ObRootValidate& root_validate)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cancel validate scheduler init twice", KR(ret));
  } else if (OB_FAIL(updater_.init(sql_proxy))) {
    LOG_WARN("failed to init cancel validate scheduler", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    job_id_ = job_id;
    root_validate_ = &root_validate;
    is_inited_ = true;
  }
  return ret;
}

int ObCancelValidateScheduler::start_schedule_cancel_validate()
{
  int ret = OB_SUCCESS;
  share::ObBackupValidateTaskInfo validate_info;
  share::ObBackupValidateTaskInfo update_validate_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cancel validate scheduler do not init", KR(ret));
  } else if (OB_FAIL(updater_.get_task(job_id_, validate_info))) {
    LOG_WARN("failed to get validate task info", KR(ret));
  } else if (ObBackupValidateTaskInfo::CANCEL == validate_info.status_) {
    LOG_INFO("validate task already canceled", KR(ret), K(validate_info));
  } else {
    update_validate_info = validate_info;
    update_validate_info.status_ = ObBackupValidateTaskInfo::CANCEL;
    if (OB_FAIL(updater_.update_task(validate_info, update_validate_info))) {
      LOG_WARN("failed to update validate task info", KR(ret), K(validate_info));
    } else {
      LOG_INFO("update task success", KR(ret), K(validate_info), K(update_validate_info));
      root_validate_->wakeup();
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
