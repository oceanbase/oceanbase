/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_alter_table_column_action.h"
#include "share/ob_ddl_common.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

int ObAlterTableColumnAction::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObAlterTableColumnAction::register_ddl_object_locks()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObAlterTableColumnAction::generate_schemas()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObAlterTableColumnAction::calc_schema_version_cnt()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObAlterTableColumnAction::operate_schemas()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObAlterTableColumnAction::before_commit()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAlterTableColumnAction::clean_on_fail()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObAlterTableColumnAction::map_table_lock_retry_to_parallel_ddl_conflict_(int &ret)
{
  if (share::ObDDLUtil::is_table_lock_retry_ret_code(ret)) {
    int orig_ret = ret;
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("table lock retry mapped to parallel DDL conflict", K(orig_ret), KR(ret));
  }
}
