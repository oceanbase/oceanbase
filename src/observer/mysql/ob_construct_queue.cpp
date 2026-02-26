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

#define USING_LOG_PREFIX SERVER

#include "ob_construct_queue.h"
#include "ob_mysql_request_manager.h"

using namespace oceanbase::obmysql;

ObConstructQueueTask::ObConstructQueueTask()
    :request_manager_(NULL),
    is_tp_trigger_(false)
{

}

ObConstructQueueTask::~ObConstructQueueTask()
{
  request_manager_ = NULL;
  is_tp_trigger_ = false;
}

int ObConstructQueueTask::init(const ObMySQLRequestManager *request_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(request_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else {
    request_manager_ = const_cast<ObMySQLRequestManager*>(request_manager);
    disable_timeout_check();
  }
  return ret;
}

void ObConstructQueueTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  // for testing hung scene
  int64_t code = 0;
  code = OB_E(EventTable::EN_SQL_AUDIT_CONSTRUCT_BACK_THREAD_STUCK) OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != code && is_tp_trigger_)) {
    sleep(abs(code));
    LOG_INFO("Construct sleep", K(abs(code)));
    is_tp_trigger_ = false;
  } else if (OB_SUCCESS == code) {
    is_tp_trigger_ = true;
  }

  if (OB_ISNULL(request_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else if (OB_FAIL(request_manager_->get_queue().prepare_alloc_queue())) {
    LOG_WARN("fail to prepare alloc queue", K(ret));
  }
}