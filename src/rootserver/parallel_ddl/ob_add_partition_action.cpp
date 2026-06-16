/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_add_partition_action.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_location_ddl_service.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObAddPartitionAction::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(obrpc::ObAlterTableArg::ADD_PARTITION != get_arg().alter_part_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter_part_type is not ADD_PARTITION", KR(ret),
             "alter_part_type", get_arg().alter_part_type_);
  }
  return ret;
}

int ObAddPartitionAction::check_legitimacy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAlterTablePartitionAction::check_legitimacy())) {
    LOG_WARN("fail to check alter partition", KR(ret));
  } else if (get_arg().alter_table_schema_.is_external_table()
      && OB_FAIL(ObLocationDDLService::check_location_constraint(get_arg().alter_table_schema_))) {
    LOG_WARN("fail to check location constraint", KR(ret));
  }
  return ret;
}

int ObAddPartitionAction::calc_schema_version_cnt()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else {
    // each table (data table + local index + lob + mlog) needs 3 schema versions:
    //   add_inc_partition_info(1) + update_partition_option(1) + update_table_attribute(1)
    // plus alter_table_options: +1v base + 1v per foreign key info on the data table.
    // Keep this consistent with update_table_options, which iterates get_foreign_key_infos().
    const int64_t table_cnt = new_table_schemas_.count();
    int64_t cnt = table_cnt * 3 + 1 + orig_table_schema->get_foreign_key_infos().count();
    if (orig_table_schema->is_external_table()) {
      // external table may consume 1 extra version for updating default partition's part_idx
      int64_t base_part_idx = OB_INVALID_INDEX;
      bool need_update = false;
      if (OB_FAIL(orig_table_schema->get_base_part_idx_for_add_part(base_part_idx))) {
        LOG_WARN("fail to get base part idx", KR(ret));
      } else if (OB_FAIL(ObDDLOperator::check_need_update_external_default_part_idx(
          *orig_table_schema,
          base_part_idx + get_arg().alter_table_schema_.get_partition_num(),
          need_update))) {
        LOG_WARN("fail to check need update external default part idx", KR(ret));
      } else if (need_update) {
        cnt += 1;
      }
    }
    if (OB_SUCC(ret)) {
      add_schema_version_cnt(cnt);
    }
  }
  return ret;
}

int ObAddPartitionAction::before_commit()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(orig_table_schema = get_orig_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (orig_table_schema->is_external_table()) {
    if (OB_UNLIKELY(inc_table_schemas_.count() < 1) || OB_ISNULL(inc_table_schemas_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc table schemas is empty or null", KR(ret),
               "cnt", inc_table_schemas_.count());
    } else if (OB_FAIL(get_ddl_service()->fill_external_table_res_arg(
        *inc_table_schemas_.at(0),
        get_res().res_arg_array_))) {
      LOG_WARN("fail to fill external table res arg", KR(ret));
    }
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
      LOG_WARN("fail to handle tablets", KR(ret));
    }
  }
  return ret;
}
