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

#include "storage/ddl/ob_ddl_dag_thread_pool.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "sql/session/ob_sql_session_info.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::sql;

int ObDDLDagThreadPool::init(const int64_t thread_count, ObDDLIndependentDag *ddl_dag, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(thread_count <= 0 || nullptr == ddl_dag || nullptr == session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret), K(thread_count), KP(ddl_dag), KP(session_info));
  } else if (OB_FAIL(set_thread_count(thread_count))) {
    LOG_WARN("set thread count failed", K(ret));
  } else {
    set_run_wrapper(MTL_CTX());
    ddl_dag_ = ddl_dag;
    session_info_ = session_info;
    is_inited_ = true;
  }
  return ret;
}

void ObDDLDagThreadPool::run1()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    char thread_name[OB_THREAD_NAME_BUF_LEN] = { 0 };
    snprintf(thread_name, OB_THREAD_NAME_BUF_LEN, "DDL_%ld", ddl_dag_->get_ddl_task_param().ddl_task_id_);
    lib::set_thread_name(thread_name);
    ObCurTraceId::set(ddl_dag_->get_dag_id());
    CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_DDL);
    THIS_WORKER.set_session(session_info_);
    THIS_WORKER.set_compatibility_mode(ddl_dag_->get_compat_mode());

    FLOG_INFO("ddl dag thread start", "thread_idx", get_thread_idx(), KPC(ddl_dag_));
    IGNORE_RETURN ddl_dag_->process();
    FLOG_INFO("ddl dag thread stop", "thread_idx", get_thread_idx(), KPC(ddl_dag_));
  }
}
