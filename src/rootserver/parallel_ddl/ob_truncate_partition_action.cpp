/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_truncate_partition_action.h"
#include "rootserver/ob_ddl_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObTruncatePartitionAction::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(obrpc::ObAlterTableArg::TRUNCATE_PARTITION != get_arg().alter_part_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter_part_type is not TRUNCATE_PARTITION", KR(ret),
             "alter_part_type", get_arg().alter_part_type_);
  }
  return ret;
}

int ObTruncatePartitionAction::calc_schema_version_cnt()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (OB_FAIL(pre_decide_global_index_op_types_())) {
    LOG_WARN("fail to pre decide global index op types", KR(ret));
  } else {
    // 1. Each table (data + local index + lob + mlog): 2v
    //    truncate_table_partitions 1v + update_table_attribute 1v
    const int64_t table_cnt = new_table_schemas_.count();
    int64_t cnt = table_cnt * 2;
    // 2. handle_tablets_for_alter_partitions: +1v for dropping old tablets.
    cnt += 1;
    // 3. alter_table_options: +1v base + 1v per foreign key info on the data table.
    //    Keep this consistent with update_table_options, which iterates get_foreign_key_infos().
    cnt += 1 + orig_table_schema->get_foreign_key_infos().count();
    // 4. Global index: per-index version count based on pre-decided op_type.
    if (OB_FAIL(calc_global_index_schema_version_cnt_(cnt))) {
      LOG_WARN("fail to calc global index schema version cnt", KR(ret));
    } else {
      add_schema_version_cnt(cnt);
    }
  }
  return ret;
}

int ObTruncatePartitionAction::before_commit()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  }
  if (OB_SUCC(ret)) {
    common::ObArray<const ObTableSchema*> inc_table_schema_ptrs;
    if (OB_FAIL(get_ddl_service()->handle_tablets_for_alter_partitions(
        get_arg(),
        inc_table_schemas_,
        del_table_schemas_,
        new_table_schemas_,
        get_schema_guard_wrapper(),
        get_trans(),
        inc_table_schema_ptrs))) {
      LOG_WARN("fail to handle tablets for alter partitions", KR(ret));
    } else if (OB_FAIL(get_ddl_service()->submit_global_index_async_tasks(
        const_cast<obrpc::ObAlterTableArg &>(get_arg()),
        get_tenant_id(),
        orig_table_schema,
        inc_table_schemas_,
        del_table_schemas_,
        get_schema_guard_wrapper(),
        get_trans(),
        get_res(),
        get_ddl_task_records()))) {
      LOG_WARN("fail to submit global index async tasks", KR(ret));
    }
  }
  return ret;
}
