/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_olap_async_job_stmt.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObOLAPAsyncSubmitJobStmt::ObOLAPAsyncSubmitJobStmt(ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_OLAP_ASYNC_JOB_SUBMIT)
{
}

ObOLAPAsyncSubmitJobStmt::ObOLAPAsyncSubmitJobStmt()
    : ObCMDStmt(NULL, stmt::T_OLAP_ASYNC_JOB_SUBMIT)
{
}

int64_t ObOLAPAsyncSubmitJobStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
  }
  return pos;
}


ObOLAPAsyncCancelJobStmt::ObOLAPAsyncCancelJobStmt(ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_OLAP_ASYNC_JOB_CANCEL)
{
}

ObOLAPAsyncCancelJobStmt::ObOLAPAsyncCancelJobStmt()
    : ObCMDStmt(NULL, stmt::T_OLAP_ASYNC_JOB_CANCEL)
{
}

int64_t ObOLAPAsyncCancelJobStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
  }
  return pos;
}