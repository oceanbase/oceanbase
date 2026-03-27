/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_strings.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_event_stmt.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObCreateEventStmt::ObCreateEventStmt(ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_EVENT_JOB_CREATE), event_info_()
{
}

ObCreateEventStmt::ObCreateEventStmt()
    : ObCMDStmt(NULL, stmt::T_EVENT_JOB_CREATE),  event_info_()
{
}

int64_t ObCreateEventStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
  }
  return pos;
}

ObAlterEventStmt::ObAlterEventStmt(ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_EVENT_JOB_ALTER), event_info_()
{
}

ObAlterEventStmt::ObAlterEventStmt()
    : ObCMDStmt(NULL, stmt::T_EVENT_JOB_ALTER),  event_info_()
{
}

int64_t ObAlterEventStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
  }
  return pos;
}

ObDropEventStmt::ObDropEventStmt(ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_EVENT_JOB_DROP), event_info_()
{
}

ObDropEventStmt::ObDropEventStmt()
    : ObCMDStmt(NULL, stmt::T_EVENT_JOB_DROP),  event_info_()
{
}

int64_t ObDropEventStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
  }
  return pos;
}
