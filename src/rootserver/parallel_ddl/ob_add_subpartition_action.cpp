/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_add_subpartition_action.h"
#include "rootserver/ob_ddl_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObAddSubpartitionAction::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(obrpc::ObAlterTableArg::ADD_SUB_PARTITION != get_arg().alter_part_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter_part_type is not ADD_SUB_PARTITION", KR(ret),
             "alter_part_type", get_arg().alter_part_type_);
  }
  return ret;
}

int ObAddSubpartitionAction::calc_schema_version_cnt()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    // each table needs 2 schema versions:
    //   add_inc_partition_info(1) + update_table_attribute(1)
    // plus alter_table_options: +1v base + 1v per foreign key info on the data table.
    // Keep this consistent with update_table_options, which iterates get_foreign_key_infos().
    const int64_t table_cnt = new_table_schemas_.count();
    const int64_t cnt = table_cnt * 2 + 1 + orig_table_schema->get_foreign_key_infos().count();
    add_schema_version_cnt(cnt);
  }
  return ret;
}

int ObAddSubpartitionAction::before_commit()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    common::ObArray<const ObTableSchema*> inc_table_schema_ptrs;
    if (OB_FAIL(get_ddl_service()->handle_tablets_for_alter_partitions(
        get_arg(),
        inc_table_schemas_,
        del_table_schemas_,
        new_table_schemas_,
        get_schema_guard_wrapper(),
        get_trans(),
        inc_table_schema_ptrs))) {
      LOG_WARN("fail to handle tablets", KR(ret));
    }
  }
  return ret;
}
