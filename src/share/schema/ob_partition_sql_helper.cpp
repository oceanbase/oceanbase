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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_partition_sql_helper.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_partition_modify.h"
#include "share/ob_rpc_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/timezone/ob_timezone_info.h"
#include "share/ob_time_zone_info_manager.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"

namespace oceanbase
{
using namespace obrpc;
namespace share
{
namespace schema
{
using namespace common;

int ObPartDMLGenerator::gen_dml(ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  PartInfo part_info;
  if (OB_FAIL(extract_part_info(part_info))) {
    LOG_WARN("extract part info failed", K(ret));
  } else if (OB_FAIL(convert_to_dml(part_info, dml))) {
    LOG_WARN("convert to dml failed", K(ret));
  }
  return ret;
}

int ObPartDMLGenerator::gen_high_bound_val_str(
    const bool is_oracle_mode,
    const ObRowkey &high_bound_val,
    ObString &high_bound_val_str,
    ObString &b_high_bound_val_str,
    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  //TODO:@yanhua add session timezone_info is better
  ObTimeZoneInfo tz_info;
  tz_info.set_offset(0);
  if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {
    LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
             is_oracle_mode, high_bound_val, high_bound_val_,
             OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos, false, &tz_info))) {
    LOG_WARN("Failed to convert rowkey to sql text", K(ret));
  } else {
    high_bound_val_str.assign_ptr(high_bound_val_, static_cast<int32_t>(pos));
  }
  if (OB_SUCC(ret)) {
    pos = 0;
    if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(
        high_bound_val, b_high_bound_val_,
        OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else {
      b_high_bound_val_str.assign_ptr(b_high_bound_val_, static_cast<int32_t>(pos));
    }
  }

  return ret;
}
int ObPartDMLGenerator::gen_list_val_str(
    const bool is_oracle_mode,
    const common::ObIArray<common::ObNewRow>& list_value,
    common::ObString &list_val_str,
    common::ObString &b_list_val_str,
    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  //TODO:@yanhua add session timezone_info is better
  ObTimeZoneInfo tz_info;
  tz_info.set_offset(0);
  if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {
    LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
             is_oracle_mode, list_value, list_val_,
             OB_MAX_B_PARTITION_EXPR_LENGTH, pos, false, &tz_info))) {
    LOG_WARN("failed to convert row to sql test", K(ret));
  } else {
    list_val_str.assign_ptr(list_val_, static_cast<int32_t>(pos));
  }
  pos = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_hex(list_value, b_list_val_,
                                                           OB_MAX_B_PARTITION_EXPR_LENGTH, pos))) {
    LOG_WARN("failed to convert rowkey to hex", K(ret));
  } else {
    b_list_val_str.assign_ptr(b_list_val_, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObPartDMLGenerator::gen_interval_part_name(int64_t part_id, ObString &part_name)
{
  int ret = OB_SUCCESS;
  int64_t str_size = common::OB_MAX_PARTITION_NAME_LENGTH;
  int32_t str_len = 0;
  memset(&interval_part_name_, 0, str_size);
  str_len += snprintf(interval_part_name_, str_size, "SYS_P%ld", part_id);
  part_name.assign_ptr(interval_part_name_, str_len);
  return ret;
}

int ObPartSqlHelper::iterate_all_part(
    const bool only_history)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table_->is_user_partition_table()) {
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString part_sql;
    ObSqlString part_history_sql;
    ObSqlString value_str;
    const ObPartitionOption &part_expr = table_->get_part_option();
    int64_t part_num = part_expr.get_part_num();
    ObPartition **part_array = table_->get_part_array();
    int64_t count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
      dml.reset();
      ObPartition *part = NULL;
      if (OB_ISNULL(part_array) || OB_ISNULL(part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), KP(part_array), K(i));
      } else {
        part = part_array[i];
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_part_dml_column(exec_tenant_id, table_, *part, dml))) {
          LOG_WARN("add dml column failed", K(ret), K(*part));
        }
      }
      if (OB_SUCC(ret)) {
        if (!only_history) {
          if (0 == count) {
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_PART_TNAME, part_sql))) {
              LOG_WARN("splice_insert_sql failed", K(ret));
            }
          } else if (count < MAX_DML_NUM) {
            value_str.reset();
            if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(part_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          const int64_t deleted = is_deleted() ? 1 : 0;
          if (OB_FAIL(dml.add_column("is_deleted", deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (0 == count) {
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
              LOG_WARN("splice_insert_sql failed", K(ret));
            }
          } else if (count < MAX_DML_NUM) {
            value_str.reset();
            if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }
        count++;
      }
      if (OB_SUCC(ret)) {
        if (count >= MAX_DML_NUM || i == part_num - 1) {
          int64_t affected_rows = 0;
          if (!only_history) {
            if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
              LOG_WARN("execute sql failed", K(ret), K(part_sql));
            } else if (affected_rows != count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("affected_rows is unexpected", K(ret), K(count), K(affected_rows));
            }
          }
          if (OB_SUCC(ret)) {
            affected_rows = 0;
            if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
              LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
            } else if (affected_rows != count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("history affected_rows is unexpected", K(ret), K(count), K(affected_rows));
            }
          }
          if (OB_SUCC(ret)){
            count = 0;
            part_sql.reset();
            part_history_sql.reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObPartSqlHelper::iterate_all_sub_part(const bool only_history)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table_->is_user_subpartition_table()) {
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString value_str;
    ObSqlString part_sql;
    ObSqlString part_history_sql;
    int64_t part_num = table_->get_part_option().get_part_num();
    ObPartition **part_array = table_->get_part_array();
    ObSubPartition **subpart_array = NULL;
    ObSubPartition *subpart = NULL;
    int64_t sub_part_num = 0;
    int64_t count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
      int64_t part_id = -1;
      if (OB_ISNULL(part_array) || OB_ISNULL(part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpart_array is invalid", KR(ret));
      } else {
        subpart_array = part_array[i]->get_subpart_array();
        sub_part_num = part_array[i]->get_subpartition_num();
        part_id = part_array[i]->get_part_id();
        if (part_id < 0 || sub_part_num < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_id or sub_part_num is invalid", K(ret), K(part_id), K(sub_part_num));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < sub_part_num; j++) {
        dml.reset();
        int64_t sub_part_id = -1;
        if (OB_ISNULL(subpart_array) || OB_ISNULL(subpart_array[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart_array is invalid", KR(ret));
        } else {
          subpart = subpart_array[j];
          sub_part_id = subpart->get_sub_part_id();
          if (sub_part_id < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub_part_id or is invalid", K(ret), K(sub_part_id));
          } else if (OB_FAIL(add_subpart_dml_column(exec_tenant_id, table_, part_id, sub_part_id, *subpart, dml))) {
            LOG_WARN("add dml column failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (!only_history) {
            if (0 == count) {
              if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_SUB_PART_TNAME, part_sql))) {
                LOG_WARN("splice_insert_sql failed", K(ret));
              }
            } else if (count < MAX_DML_NUM) {
              value_str.reset();
              if (OB_FAIL(dml.splice_values(value_str))) {
                LOG_WARN("splice_values failed", K(ret));
              } else if (OB_FAIL(part_sql.append_fmt(", (%s)", value_str.ptr()))) {
                LOG_WARN("append_fmt failed", K(value_str), K(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            const int64_t deleted = is_deleted() ? 1 : 0;
            if (OB_FAIL(dml.add_column("is_deleted", deleted))) {
              LOG_WARN("add column failed", K(ret));
            } else if (0 == count) {
              if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME, part_history_sql))) {
                LOG_WARN("splice_insert_sql failed", K(ret));
              }
            } else if (count < MAX_DML_NUM) {
              value_str.reset();
              if (OB_FAIL(dml.splice_values(value_str))) {
                LOG_WARN("splice_values failed", K(ret));
              } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
                LOG_WARN("append_fmt failed", K(value_str), K(ret));
              }
            }
          }
          count++;
        }
        if (OB_SUCC(ret)) {
          if (count >= MAX_DML_NUM || (i == part_num - 1 && j == sub_part_num - 1)) {
            int64_t affected_rows = 0;
            if (!only_history) {
              if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
                LOG_WARN("execute sql failed", K(ret), K(part_sql));
              } else if (affected_rows != count) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("affected_rows is unexpected", K(affected_rows), K(count), K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              affected_rows = 0;
              if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
                LOG_WARN("execute sql failed", K(ret), K(part_sql));
              } else if (affected_rows != count) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("affected_rows is unexpected", K(affected_rows), K(count), K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              count = 0;
              part_sql.reset();
              part_history_sql.reset();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPartSqlHelper::iterate_all_def_sub_part(const bool only_history)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table_->is_user_subpartition_table()
             && table_->has_sub_part_template_def()) {
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString value_str;
    ObSqlString part_sql;
    ObSqlString part_history_sql;
    const int64_t def_sub_part_num = table_->get_sub_part_option().get_part_num();
    ObSubPartition **def_subpart_array = table_->get_def_subpart_array();
    int64_t count = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < def_sub_part_num; j++) {
      dml.reset();
      if (OB_ISNULL(def_subpart_array) || OB_ISNULL(def_subpart_array[j])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("def_subpart is null", KR(ret), KP(def_subpart_array), K(j));
      } else if (OB_FAIL(add_def_subpart_dml_column(
                 exec_tenant_id, table_, j, *(def_subpart_array[j]), dml))) {
        LOG_WARN("add dml column failed", K(ret));
      } else {
        if (!only_history) {
          if (0 == count) {
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_DEF_SUB_PART_TNAME, part_sql))) {
              LOG_WARN("splice_insert_sql failed", K(ret));
            }
          } else if (count < MAX_DML_NUM) {
            value_str.reset();
            if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(part_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          const int64_t deleted = is_deleted() ? 1 : 0;
          if (OB_FAIL(dml.add_column("is_deleted", deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (0 == count) {
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_DEF_SUB_PART_HISTORY_TNAME,
                                              part_history_sql))) {
              LOG_WARN("splice_insert_sql failed", K(ret));
            }
          } else if (count < MAX_DML_NUM) {
            value_str.reset();
            if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }
        count++;
      }
      if (OB_SUCC(ret)) {
        if (count >= MAX_DML_NUM || j == def_sub_part_num - 1) {
          int64_t affected_rows = 0;
          if (!only_history) {
            if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
              LOG_WARN("execute sql failed", K(ret), K(part_sql));
            } else if (affected_rows != count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("affected_rows is unexpected", K(affected_rows), K(count), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            affected_rows = 0;
            if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
              LOG_WARN("execute sql failed", K(ret), K(part_sql));
            } else if (affected_rows != count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("affected_rows is unexpected", K(affected_rows), K(count), K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            count = 0;
            part_sql.reset();
            part_history_sql.reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObPartSqlHelper::iterate_part_info(const bool only_history)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table_->is_user_partition_table()) {
    ObDMLSqlSplicer dml;
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(add_part_info_dml_column(exec_tenant_id, table_, dml))) {
      LOG_WARN("add dml column failed", K(ret));
    } else {
      ObDMLExecHelper exec(sql_client_, exec_tenant_id);
      int64_t affected_rows = 0;
      if (!only_history) {
        if (OB_FAIL(exec.exec_insert(share::OB_ALL_PART_INFO_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected rows is not correct", K(ret), K(affected_rows));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t deleted = is_deleted() ? 1 : 0;
        affected_rows = 0;
        if (OB_FAIL(dml.add_column("is_deleted", deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(exec.exec_insert(share::OB_ALL_PART_INFO_HISTORY_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected rows is not correct", K(ret), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_partition_info()
{
  int ret = OB_SUCCESS;
  const bool is_only_history = false;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(iterate_part_info(is_only_history))) {
    LOG_WARN("iterate part info failed", K(ret));
  } else if (OB_FAIL(iterate_all_part(is_only_history))) {
    LOG_WARN("add all part failed", K(ret));
  } else if (OB_FAIL(iterate_all_sub_part(is_only_history))) {
    LOG_WARN("add all subpart failed", K(ret));
  } else if (OB_FAIL(iterate_all_def_sub_part(is_only_history))) {
    LOG_WARN("add all def subpart failed", K(ret));
  }
  return ret;
}

int ObAddPartInfoHelper::add_part_info_dml_column(const uint64_t exec_tenant_id,
                                                  const ObPartitionSchema *table,
                                                  ObDMLSqlSplicer &dml)
{
   int ret = OB_SUCCESS;
   if (OB_ISNULL(table)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("table is null", K(ret));
   } else {
     const ObPartitionOption &part_option = table->get_part_option();
     const ObPartitionOption &subpart_option = table->get_sub_part_option();
     if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                exec_tenant_id, table->get_tenant_id())))
       || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, table->get_table_id())))
       || OB_FAIL(dml.add_column("part_type", part_option.get_part_func_type()))
       || OB_FAIL(dml.add_column(OBJ_GET_K(part_option, part_num)))
       || OB_FAIL(dml.add_column("part_space", 0))
       || OB_FAIL(dml.add_column("part_expr", part_option.get_part_func_expr_str()))
       || OB_FAIL(dml.add_column("sub_part_type", subpart_option.get_part_func_type()))
       || OB_FAIL(dml.add_column("def_sub_part_num", subpart_option.get_part_num()))
       || OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))
       || OB_FAIL(dml.add_column("sub_part_expr", subpart_option.get_part_func_expr()))) {
       LOG_WARN("add column failed", K(ret));
     }
   }
   return ret;
}

int ObAddPartInfoHelper::add_part_dml_column(const uint64_t exec_tenant_id,
                                             const ObPartitionSchema *table,
                                             const ObPartition &part,
                                             ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (part.get_part_idx() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_idx is invalid", KR(ret), K(part));
  } else {
    int64_t sub_part_num = 0;
    if (PARTITION_LEVEL_TWO == table->get_part_level()) {
      sub_part_num = part.get_sub_part_num();
    }
    PartitionType partition_type = part.get_partition_type();
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table->get_tenant_id())))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table->get_table_id())))
        || OB_FAIL(dml.add_pk_column("part_id", part.get_part_id()))
        || OB_FAIL(dml.add_pk_column("part_idx", part.get_part_idx()))
        || OB_FAIL(dml.add_column("schema_version", table->get_schema_version()))
        || OB_FAIL(dml.add_column("sub_part_num", sub_part_num))
        || OB_FAIL(dml.add_column("sub_part_space", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("new_sub_part_space", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("status", PARTITION_STATUS_INVALID))
        || OB_FAIL(dml.add_column("spare1", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("spare3", "" /*unused now*/))
        || OB_FAIL(dml.add_column("comment", "" /*unused now*/))
        || OB_FAIL(dml.add_column("tablespace_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, part.get_tablespace_id())))
        || OB_FAIL(dml.add_column("partition_type", partition_type))
        || OB_FAIL(dml.add_column("tablet_id", part.get_tablet_id().id()))
        || OB_FAIL(dml.add_column("part_name", ObHexEscapeSqlStr(part.get_part_name())))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (OB_FAIL(add_part_high_bound_val_column(table, part, dml))) {
      LOG_WARN("add part high bound failed", K(ret), K(table), K(part));
    } else if (OB_FAIL(add_part_list_val_column(table, part, dml))) {
      LOG_WARN("add list val failed", K(ret), K(table), K(part));
    }
  }
  return ret;
}

// For some paths, hash-like subpartition info will only store in sub_part_option from resolver,
// which means subpart from def_sub_part_array/sub_part_array may be NULL.
int ObAddPartInfoHelper::add_subpart_dml_column(const uint64_t exec_tenant_id,
                                                const ObPartitionSchema *table,
                                                const int64_t part_id,
                                                const int64_t subpart_id,
                                                const ObSubPartition &subpart,
                                                ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (subpart.get_sub_part_idx() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpart_idx is invalid", KR(ret), K(subpart));
  } else {
    PartitionType partition_type = subpart.get_partition_type();
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table->get_tenant_id())))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table->get_table_id())))
        || OB_FAIL(dml.add_pk_column("part_id", part_id))
        || OB_FAIL(dml.add_pk_column("sub_part_id", subpart_id))
        || OB_FAIL(dml.add_column("schema_version", table->get_schema_version()))
        || OB_FAIL(dml.add_column("status", PARTITION_STATUS_INVALID))
        || OB_FAIL(dml.add_column("spare1", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("spare3", "" /*unused now*/))
        || OB_FAIL(dml.add_column("comment", "" /*unused now*/))
        || OB_FAIL(dml.add_column("sub_part_idx", subpart.get_sub_part_idx()))
        || OB_FAIL(dml.add_column("source_partition_id", -1))
        || OB_FAIL(dml.add_column("tablespace_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, subpart.get_tablespace_id())))
        || OB_FAIL(dml.add_column("tablet_id", subpart.get_tablet_id().id()))
        || OB_FAIL(dml.add_column("sub_part_name", ObHexEscapeSqlStr(subpart.get_part_name())))) {
        LOG_WARN("dml add part info failed", K(ret));
    } else if (OB_FAIL(add_subpart_high_bound_val_column(table, subpart, dml))) {
      LOG_WARN("add part high bound failed", K(ret), KPC(table), K(subpart_id));
    } else if (OB_FAIL(add_subpart_list_val_column(table, subpart, dml))) {
      LOG_WARN("add list value failed", K(ret), K(table), K(subpart_id));
    }
  }
  return ret;
}

// For some paths, hash-like subpartition info will only store in sub_part_option from resolver,
// which means subpart from def_sub_part_array/sub_part_array may be NULL.
int ObAddPartInfoHelper::add_def_subpart_dml_column(const uint64_t exec_tenant_id,
                                                    const ObPartitionSchema *table,
                                                    const int64_t def_subpart_idx,
                                                    const ObSubPartition &subpart,
                                                    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    // For def subpartition, sub_part_idx and sub_part_id should be equal
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table->get_tenant_id())))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table->get_table_id())))
        || OB_FAIL(dml.add_pk_column("sub_part_id", def_subpart_idx))
        || OB_FAIL(dml.add_column("schema_version", table->get_schema_version()))
        || OB_FAIL(dml.add_column("spare1", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/))
        || OB_FAIL(dml.add_column("spare3", "" /*unused now*/))
        || OB_FAIL(dml.add_column("comment", "" /*unused now*/))
        || OB_FAIL(dml.add_column("sub_part_idx", def_subpart_idx))
        || OB_FAIL(dml.add_column("tablespace_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, subpart.get_tablespace_id())))
        || OB_FAIL(dml.add_column("sub_part_name", ObHexEscapeSqlStr(subpart.get_part_name())))) {
        LOG_WARN("dml add part info failed", K(ret));
    } else if (OB_FAIL(add_subpart_high_bound_val_column(table, subpart, dml))) {
      LOG_WARN("add part high bound failed", K(ret), KPC(table), K(def_subpart_idx));
    } else if (OB_FAIL(add_subpart_list_val_column(table, subpart, dml))) {
      LOG_WARN("add list value failed", K(ret), K(table), K(def_subpart_idx));
    }
  }

  return ret;
}

int ObAddPartInfoHelper::add_part_high_bound_val_column(const ObPartitionSchema *table,
                                                        const ObBasePartition &part,
                                                        ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table->is_range_part()) {
    if (OB_FAIL(add_high_bound_val_column(part, dml))) {
      LOG_WARN("add high bound val column failed", K(ret));
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_part_list_val_column(const ObPartitionSchema *table,
                                                        const ObBasePartition &part,
                                                        ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table->is_list_part()) {
    if (OB_FAIL(add_list_val_column(part, dml))) {
      LOG_WARN("add high bound val column failed", K(ret));
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_subpart_high_bound_val_column(const ObPartitionSchema *table,
                                                        const ObBasePartition &part,
                                                        ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table->is_range_subpart()) {
    if (OB_FAIL(add_high_bound_val_column(part, dml))) {
      LOG_WARN("add high bound val column failed", K(ret));
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_subpart_list_val_column(const ObPartitionSchema *table,
                                                        const ObBasePartition &part,
                                                        ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table->is_list_subpart()) {
    if (OB_FAIL(add_list_val_column(part, dml))) {
      LOG_WARN("add high bound val column failed", K(ret));
    }
  }
  return ret;
}

template<typename P>
int ObAddPartInfoHelper::add_high_bound_val_column(const P &part_option,
                                                   ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (high_bound_val_ == NULL) {
    high_bound_val_ = static_cast<char *>(allocator_.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
    if (OB_ISNULL(high_bound_val_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("high_bound_val is null", K(ret), K(high_bound_val_));
    }
  }
  // determine if it is a list partition
  if (OB_SUCC(ret)) {
    MEMSET(high_bound_val_, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    int64_t pos = 0;
    //TODO:@yanhua add session timezone_info is better
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    bool is_oracle_mode = false;
    if (OB_ISNULL(table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ptr is null", KR(ret));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(table_->get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(table_->get_tenant_id()));
    } else if (OB_FAIL(table_->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to get compat mode", KR(ret), KPC_(table));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
               is_oracle_mode, part_option.get_high_bound_val(), high_bound_val_,
               OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(ret));
    } else if (OB_FAIL(dml.add_column("high_bound_val",
                                      ObHexEscapeSqlStr(ObString(pos, high_bound_val_))))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(
        part_option.get_high_bound_val(), high_bound_val_,
        OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else if (OB_FAIL(dml.add_column("b_high_bound_val", ObString(pos, high_bound_val_)))) {
      LOG_WARN("Failed to add column b_high_bound_val", K(ret));
    } else {
      LOG_DEBUG("high bound info", "high_bound_val", ObString(pos, high_bound_val_).ptr(), K(pos));
    } //do nothing
  }
  return ret;
}

template<class P>
int ObAddPartInfoHelper::add_list_val_column(const P &part_option,
                                                   ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (list_val_ == NULL) {
    list_val_ = static_cast<char *>(allocator_.alloc(OB_MAX_B_PARTITION_EXPR_LENGTH));
    if (OB_ISNULL(list_val_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("list_val is null", K(ret), K(list_val_));
    }
  }
  // determine if it is a list partition, if it is a list partition
  if (OB_SUCC(ret)) {
    MEMSET(list_val_, 0, OB_MAX_B_PARTITION_EXPR_LENGTH);
    int64_t pos = 0;
    //TODO:@yanhua add session timezone_info is better
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0); 
    bool is_oracle_mode = false;
    if (OB_ISNULL(table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ptr is null", KR(ret));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(table_->get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(table_->get_tenant_id()));
    } else if (OB_FAIL(table_->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to get compat mode", KR(ret), KPC_(table));
    } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
               is_oracle_mode, part_option.get_list_row_values(), list_val_,
               OB_MAX_B_PARTITION_EXPR_LENGTH, pos, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(ret));
    } else if (OB_FAIL(dml.add_column("list_val",
                                      ObHexEscapeSqlStr(ObString(pos, list_val_))))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_hex(
        part_option.get_list_row_values(), list_val_, OB_MAX_B_PARTITION_EXPR_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else if (OB_FAIL(dml.add_column("b_list_val", ObString(pos, list_val_)))) {
      LOG_WARN("Failed to add column b_list_val", K(ret));
    }
  }
  return ret;
}

int ObDropPartInfoHelper::delete_partition_info()
{
  int ret = OB_SUCCESS;
  const bool is_only_history = true;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(iterate_part_info(is_only_history))) {
    LOG_WARN("drop part info failed", K(ret));
  } else if (OB_FAIL(iterate_all_part(is_only_history))) {
    LOG_WARN("drop all part failed", K(ret));
  } else if (OB_FAIL(iterate_all_sub_part(is_only_history))) {
    LOG_WARN("drop all sub part failed", K(ret));
  } else if (OB_FAIL(iterate_all_def_sub_part(is_only_history))) {
    LOG_WARN("drop all def sub part failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_part_info_dml_column(
    const uint64_t exec_tenant_id,
    const ObPartitionSchema *table,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, table->get_tenant_id())))
             || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                      exec_tenant_id, table->get_table_id())))
             || OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_part_dml_column(const uint64_t exec_tenant_id,
                                              const ObPartitionSchema *table,
                                              const ObPartition &part,
                                              share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, table->get_tenant_id())))
             || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                      exec_tenant_id, table->get_table_id())))
             || OB_FAIL(dml.add_pk_column("part_id", part.get_part_id()))
             || OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_subpart_dml_column(const uint64_t exec_tenant_id,
                                                 const ObPartitionSchema *table,
                                                 const int64_t part_id,
                                                 const int64_t subpart_id,
                                                 const ObSubPartition &subpart,
                                                 share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  UNUSED(subpart);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, table->get_tenant_id())))
             || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                      exec_tenant_id, table->get_table_id())))
             || OB_FAIL(dml.add_pk_column("part_id", part_id))
             || OB_FAIL(dml.add_pk_column("sub_part_id", subpart_id))
             || OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_def_subpart_dml_column(const uint64_t exec_tenant_id,
                                                     const ObPartitionSchema *table,
                                                     const int64_t subpart_idx,
                                                     const ObSubPartition &subpart,
                                                     share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  UNUSED(subpart);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table->get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, table->get_table_id())))
      || OB_FAIL(dml.add_pk_column("sub_part_id", subpart_idx))
      || OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObAddIncSubPartDMLGenerator::convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  PartitionType partition_type = part_info.partition_type_;
  int64_t subpart_idx = part_info.sub_part_idx_;
  if (subpart_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpart_idx is invalid", KR(ret), K(part_info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, part_info.table_id_)))
      || OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_))
      || OB_FAIL(dml.add_pk_column("sub_part_id", part_info.sub_part_id_))
      || OB_FAIL(dml.add_pk_column("sub_part_idx", subpart_idx))
      || OB_FAIL(dml.add_column("sub_part_name", ObHexEscapeSqlStr(part_info.part_name_)))
      || OB_FAIL(dml.add_column("schema_version", part_info.schema_version_))
      || OB_FAIL(dml.add_column("status", part_info.status_))
      || OB_FAIL(dml.add_column("spare1", 0 /*unused now*/))
      || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/))
      || OB_FAIL(dml.add_column("spare3", "" /*unused now*/))
      || OB_FAIL(dml.add_column("comment", "" /*unused now*/))
      || OB_FAIL(dml.add_column("high_bound_val",
                                ObHexEscapeSqlStr(part_info.high_bound_val_)))
      || OB_FAIL(dml.add_column("b_high_bound_val",
                                part_info.b_high_bound_val_))
      || OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(part_info.list_val_)))
      || OB_FAIL(dml.add_column("b_list_val", part_info.b_list_val_))
      || OB_FAIL(dml.add_column("partition_type", partition_type))
      || OB_FAIL(dml.add_column("tablet_id", part_info.tablet_id_.id()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    //nothing todo
  }
  return ret;
}

int ObAddIncSubPartDMLGenerator::extract_part_info(PartInfo &part_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ori_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (part_idx_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_idx", K(part_idx_), K(ret));
  } else {
    part_info.tenant_id_ = ori_table_->get_tenant_id();
    part_info.table_id_ = ori_table_->get_table_id();
    part_info.part_id_ = part_.get_part_id();
    part_info.sub_part_id_ = sub_part_.get_sub_part_id();
    part_info.part_name_ = sub_part_.get_part_name();
    part_info.schema_version_ = schema_version_;
    part_info.status_ = PARTITION_STATUS_INVALID;
    part_info.sub_part_idx_ = sub_part_.get_sub_part_idx();
    part_info.partition_type_ = sub_part_.get_partition_type();
    part_info.tablet_id_ = sub_part_.get_tablet_id();

    bool is_oracle_mode = false;
    if (FAILEDx(ori_table_->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle compat mode", KR(ret), KPC_(ori_table));
    } else if (ori_table_->is_range_subpart()) {
      if (OB_FAIL(gen_high_bound_val_str(is_oracle_mode,
                                         sub_part_.get_high_bound_val(),
                                         part_info.high_bound_val_,
                                         part_info.b_high_bound_val_,
                                         part_info.tenant_id_))) {
        LOG_WARN("generate high bound val failed", K(ret));
      }
    } else if (ori_table_->is_list_subpart()) {
      if (OB_FAIL(gen_list_val_str(
                  is_oracle_mode,
                  sub_part_.get_list_row_values(),
                  part_info.list_val_,
                  part_info.b_list_val_,
                  part_info.tenant_id_))) {
        LOG_WARN("generate listval failed", K(ret));
      }
    } else if (is_hash_like_part(ori_table_->get_sub_part_option().get_part_func_type())) {
      part_info.sub_part_idx_ = subpart_idx_;
    }
  }

  return ret;
}

int ObAddIncPartDMLGenerator::convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  PartitionType partition_type = part_info.partition_type_;
  int64_t part_idx = part_info.part_idx_;
  if (part_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_idx is invalid", KR(ret), K(part_info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, part_info.table_id_)))
      || OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_))
      || OB_FAIL(dml.add_pk_column("part_idx", part_idx))
      || OB_FAIL(dml.add_pk_column("part_name", ObHexEscapeSqlStr(part_info.part_name_)))
      || OB_FAIL(dml.add_column("schema_version", part_info.schema_version_))
      || OB_FAIL(dml.add_column("sub_part_num", part_info.sub_part_num_))
      || OB_FAIL(dml.add_column("sub_part_space", 0 /*unused now*/))
      || OB_FAIL(dml.add_column("new_sub_part_space", 0 /*unused now*/))
      || OB_FAIL(dml.add_column("status", part_info.status_))
      || OB_FAIL(dml.add_column("spare1", 0 /*unused now*/))
      || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/))
      || OB_FAIL(dml.add_column("spare3", ""/*unused now*/))
      || OB_FAIL(dml.add_column("comment", "" /*unused now*/))
      || OB_FAIL(dml.add_column("high_bound_val",
                                ObHexEscapeSqlStr(part_info.high_bound_val_)))
      || OB_FAIL(dml.add_column("b_high_bound_val",
                                part_info.b_high_bound_val_))
      || OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(part_info.list_val_)))
      || OB_FAIL(dml.add_column("b_list_val", part_info.b_list_val_))
      || OB_FAIL(dml.add_column("partition_type", partition_type))
      || OB_FAIL(dml.add_column("tablet_id", part_info.tablet_id_.id()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    //nothing todo
  }
  return ret;
}

int ObAddIncPartDMLGenerator::extract_part_info(PartInfo &part_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ori_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    int64_t sub_part_num = 0;
    if (PARTITION_LEVEL_TWO == ori_table_->get_part_level()) {
      sub_part_num = part_.get_sub_part_num();
    }
    part_info.tenant_id_ = ori_table_->get_tenant_id();
    part_info.table_id_ = ori_table_->get_table_id();
    part_info.part_id_ = part_.get_part_id();
    part_info.schema_version_ = schema_version_;
    part_info.sub_part_num_ = sub_part_num;
    part_info.status_ = PARTITION_STATUS_INVALID;
    part_info.part_idx_ = part_.get_part_idx();
    part_info.partition_type_ = part_.get_partition_type();
    part_info.tablet_id_ = part_.get_tablet_id();

    bool is_oracle_mode = false;
    if (FAILEDx(ori_table_->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle compat mode", KR(ret), KPC_(ori_table));
    }

    if (OB_FAIL(ret)) {
    } else if (!ori_table_->is_interval_part() || !part_.get_part_name().empty()) {
      part_info.part_name_ = part_.get_part_name();
    } else if (OB_FAIL(gen_interval_part_name(part_info.part_id_, part_info.part_name_))) {
      LOG_WARN("fail to gen_interval_part_name", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (ori_table_->is_range_part()) {
      if (OB_FAIL(gen_high_bound_val_str(is_oracle_mode,
                                         part_.get_high_bound_val(),
                                         part_info.high_bound_val_,
                                         part_info.b_high_bound_val_,
                                         part_info.tenant_id_))) {
        LOG_WARN("generate high bound val failed", K(ret));
      }
    } else if (ori_table_->is_list_part()) {
      if (OB_FAIL(gen_list_val_str(is_oracle_mode,
                                   part_.get_list_row_values(),
                                   part_info.list_val_,
                                   part_info.b_list_val_,
                                   part_info.tenant_id_))) {
        LOG_WARN("generate listval failed", K(ret));
      }
    } else if (is_hash_like_part(ori_table_->get_part_option().get_part_func_type())) {
      part_info.part_idx_ = part_.get_part_idx();
    }
  }

  return ret;
}

int ObDropIncSubPartDMLGenerator::convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t deleted = true;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, part_info.tenant_id_)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, part_info.table_id_)))
      || OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_))
      || OB_FAIL(dml.add_pk_column("sub_part_id", part_info.sub_part_id_))
      || OB_FAIL(dml.add_column("is_deleted", deleted))
      || OB_FAIL(dml.add_column("schema_version", part_info.schema_version_))) {
    LOG_WARN("dml drop part info failed", K(ret));
  }
  return ret;
}

int ObDropIncSubPartDMLGenerator::extract_part_info(PartInfo &part_info)
{
  int ret = OB_SUCCESS;

  part_info.tenant_id_ = sub_part_.get_tenant_id();
  part_info.table_id_ = sub_part_.get_table_id();
  part_info.part_id_ = sub_part_.get_part_id();
  part_info.sub_part_id_ = sub_part_.get_sub_part_id();
  part_info.schema_version_ = schema_version_;

  return ret;
}

int ObDropIncPartDMLGenerator::convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, part_info.tenant_id_)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, part_info.table_id_)))
      || OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_))
      || OB_FAIL(dml.add_column("schema_version", part_info.schema_version_))) {
    LOG_WARN("dml drop part info failed", K(ret));
  }
  return ret;
}

int ObDropIncPartDMLGenerator::extract_part_info(PartInfo &part_info)
{
  int ret = OB_SUCCESS;

  part_info.tenant_id_ = part_.get_tenant_id();
  part_info.table_id_ = part_.get_table_id();
  part_info.part_id_ = part_.get_part_id();
  part_info.schema_version_ = schema_version_;

  return ret;
}

int ObAddIncPartHelper::add_partition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_user_partition_table()) {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer history_dml;
    ObDMLSqlSplicer sub_dml;
    ObDMLSqlSplicer history_sub_dml;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    ObPartition **part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    const int64_t deleted = false;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      ObPartition *part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part array is null", K(ret), K(i), K(inc_table_));
      } else {
        HEAP_VAR(ObAddIncPartDMLGenerator, part_dml_gen,
                 ori_table_, *part, inc_part_num, i, schema_version_) {
          if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
            LOG_WARN("gen dml failed", K(ret));
          } else if (OB_FAIL(dml.finish_row())) {
            LOG_WARN("failed to finish row", K(ret));
          } else if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
            LOG_WARN("gen dml history failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(history_dml.add_column("is_deleted", deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(history_dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (PARTITION_LEVEL_TWO != ori_table_->get_part_level()) {
          // skip
        } else if (OB_ISNULL(part->get_subpart_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart array is null", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
            inc_sub_part_num++;
            HEAP_VAR(ObAddIncSubPartDMLGenerator, sub_part_dml_gen,
                     ori_table_, *part, *part->get_subpart_array()[j], inc_part_num, i, j, schema_version_) {
              if (OB_FAIL(sub_part_dml_gen.gen_dml(sub_dml))) {
                LOG_WARN("gen sub dml column failed", K(ret));
              } else if (OB_FAIL(sub_dml.finish_row())) {
                LOG_WARN("failed to finish row", K(ret));
              } else if (OB_FAIL(sub_part_dml_gen.gen_dml(history_sub_dml))) {
                LOG_WARN("gen dml history failed", K(ret));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(history_sub_dml.add_column("is_deleted", deleted))) {
              LOG_WARN("add column failed", K(ret));
            } else if (OB_FAIL(history_sub_dml.finish_row())) {
              LOG_WARN("failed to finish row", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_dml.splice_batch_insert_sql(share::OB_ALL_PART_HISTORY_TNAME,
                                                      part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret)) {
      ObSqlString part_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(dml.splice_batch_insert_sql(share::OB_ALL_PART_TNAME, part_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_sql));
      } else if (affected_rows != inc_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret) && inc_sub_part_num > 0) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_sub_dml.splice_batch_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME,
                                                      part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret) && inc_sub_part_num> 0) {
      ObSqlString part_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(sub_dml.splice_batch_insert_sql(share::OB_ALL_SUB_PART_TNAME, part_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObAddIncSubPartHelper::add_subpartition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer history_dml;
    ObDMLSqlSplicer sub_dml;
    ObDMLSqlSplicer history_sub_dml;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    ObPartition **part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    const int64_t deleted = false;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      ObPartition *part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part array is null", K(ret), K(i), K(inc_table_));
      } else {
        if (OB_ISNULL(part->get_subpart_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart array is null", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
            inc_sub_part_num++;
            HEAP_VAR(ObAddIncSubPartDMLGenerator, sub_part_dml_gen,
                     ori_table_, *part, *part->get_subpart_array()[j], inc_part_num, i, j, schema_version_) {
              if (OB_FAIL(sub_part_dml_gen.gen_dml(sub_dml))) {
                LOG_WARN("gen sub dml column failed", K(ret));
              } else if (OB_FAIL(sub_dml.finish_row())) {
                LOG_WARN("failed to finish row", K(ret));
              } else if (OB_FAIL(sub_part_dml_gen.gen_dml(history_sub_dml))) {
                LOG_WARN("gen dml history failed", K(ret));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(history_sub_dml.add_column("is_deleted", deleted))) {
              LOG_WARN("add column failed", K(ret));
            } else if (OB_FAIL(history_sub_dml.finish_row())) {
              LOG_WARN("failed to finish row", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_sub_dml.splice_batch_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME,
                                                      part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret)) {
      ObSqlString part_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(sub_dml.splice_batch_insert_sql(share::OB_ALL_SUB_PART_TNAME, part_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDropIncPartHelper::drop_partition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_user_partition_table()) {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer sub_dml;
    ObSqlString part_history_sql;
    ObSqlString sub_part_history_sql;
    ObSqlString value_str;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    ObPartition **part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      dml.reset();
      ObPartition *part = part_array[i];
      // delete __all_part_history
      HEAP_VAR(ObDropIncPartDMLGenerator, part_dml_gen, *part, schema_version_) {
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part is null", KR(ret), KP(part));
        } else if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
          LOG_WARN("gen dml column failed", K(ret));
        } else {
          const int64_t deleted = true;
          if (OB_FAIL(dml.add_column("is_deleted", deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (0 == i) {
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
              LOG_WARN("splice_insert_sql failed", K(ret));
            }
          } else {
            value_str.reset();
            if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }
      } // end HEAP_VAR

      // delete __all_sub_part_history
      for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
        sub_dml.reset();
        inc_sub_part_num++;
        HEAP_VAR(ObDropIncSubPartDMLGenerator, sub_part_dml_gen,
                 *part->get_subpart_array()[j], schema_version_) {
          if (OB_FAIL(sub_part_dml_gen.gen_dml(sub_dml))) {
            LOG_WARN("gen sub dml column failed", K(ret));
          } else if (0 == i && 0 == j) {
            if (OB_FAIL(sub_dml.splice_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME, sub_part_history_sql))) {
              LOG_WARN("splice_insert_sql failed", K(ret));
            }
          } else {
            value_str.reset();
            if (OB_FAIL(sub_dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(sub_part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        } // end HEAP_VAR
      }
    } // end for
    if (OB_SUCC(ret) && inc_part_num > 0) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
    if (OB_SUCC(ret) && inc_sub_part_num > 0) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, sub_part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sub_part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDropIncSubPartHelper::drop_subpartition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_user_partition_table()) {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer sub_dml;
    ObSqlString part_history_sql;
    ObSqlString sub_part_history_sql;
    ObSqlString value_str;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    ObPartition **part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      ObPartition *part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_array[i] is null", K(ret), K(i));
      } else {
        int64_t subpart_num = part->get_subpartition_num();
        ObSubPartition **subpart_array = part->get_subpart_array();
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_num; j++) {
          sub_dml.reset();
          inc_sub_part_num++;
          HEAP_VAR(ObDropIncSubPartDMLGenerator, sub_part_dml_gen,
                   *subpart_array[j], schema_version_) {
            if (OB_FAIL(sub_part_dml_gen.gen_dml(sub_dml))) {
              LOG_WARN("gen sub dml column failed", K(ret));
            } else if (0 == i && 0 == j) {
              if (OB_FAIL(sub_dml.splice_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME, sub_part_history_sql))) {
                LOG_WARN("splice_insert_sql failed", K(ret));
              }
            } else {
              value_str.reset();
              if (OB_FAIL(sub_dml.splice_values(value_str))) {
                LOG_WARN("splice_values failed", K(ret));
              } else if (OB_FAIL(sub_part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
                LOG_WARN("append_fmt failed", K(value_str), K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && inc_sub_part_num > 0) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, sub_part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sub_part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObRenameIncPartHelper::rename_partition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", KR(ret), KP(ori_table_), KP(inc_table_));
  } else if (!ori_table_->is_user_partition_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user partition table", KR(ret), KPC(ori_table_));
  } else {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t table_id = ori_table_->get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString part_sql;
    ObPartition **part_array = inc_table_->get_part_array();
    ObPartition *inc_part = nullptr;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    int64_t affected_rows = 0;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc table part_array is null", KR(ret), KP(inc_table_));
    } else if (OB_UNLIKELY(1 != inc_part_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc part num should be 1", KR(ret), K(inc_part_num));
    } else if (OB_ISNULL(inc_part = part_array[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc_part is null", KR(ret));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))
          || OB_FAIL(dml.add_pk_column("part_id", inc_part->get_part_id()))
          || OB_FAIL(dml.add_column("schema_version", schema_version_))
          || OB_FAIL(dml.add_column("part_name", inc_part->get_part_name().ptr()))) {
      LOG_WARN("dml add column failed", KR(ret));
    } else if (OB_FAIL(dml.splice_update_sql(share::OB_ALL_PART_TNAME, part_sql))) {
      LOG_WARN("dml splice update sql failed", KR(ret));
    } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql",KR(ret), K(tenant_id), K(part_sql));
    } else if (OB_UNLIKELY(inc_part_num != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", KR(ret), K(inc_part_num), K(affected_rows));
    } else {
      ObDMLSqlSplicer history_dml;
      ObSqlString part_history_sql;
      affected_rows = 0;
      HEAP_VAR(ObAddIncPartDMLGenerator, part_dml_gen,
                ori_table_, *inc_part, inc_part_num, inc_part->get_part_idx(), schema_version_) {
        if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
          LOG_WARN("gen dml failed", KR(ret));
        } else if (OB_FAIL(history_dml.add_column("is_deleted", false))) {
          LOG_WARN("add column failed", KR(ret));
        } else if (OB_FAIL(history_dml.splice_insert_sql(share::OB_ALL_PART_HISTORY_TNAME,
                                                        part_history_sql))) {
          LOG_WARN("failed to splice batch insert sql", KR(ret), K(part_history_sql));
        } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", KR(ret), K(part_history_sql), KPC(inc_part));
        } else if (OB_UNLIKELY(inc_part_num != affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("history affected_rows is unexpected", KR(ret), K(inc_part_num), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int ObRenameIncSubpartHelper::rename_subpartition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", KR(ret), KP(ori_table_), KP(inc_table_));
  } else if (!ori_table_->is_user_subpartition_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user subpartition table", KR(ret), KPC(ori_table_));
  } else {
    int64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t table_id = ori_table_->get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString subpart_sql;
    ObPartition **part_array = inc_table_->get_part_array();
    ObPartition *inc_part = nullptr;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", KR(ret), KP(inc_table_));
    } else if (OB_UNLIKELY(1 != inc_part_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc part num should be 1", KR(ret), K(inc_part_num));
    } else if (OB_ISNULL(inc_part = part_array[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc part is null", KR(ret));
    } else {
      ObSubPartition **subpart_array = inc_part->get_subpart_array();
      ObSubPartition *inc_subpart = nullptr;
      const int64_t inc_subpart_num = inc_part->get_subpartition_num();
      int64_t affected_rows = 0;
      if (OB_ISNULL(subpart_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpart_array is null", KR(ret));
      } else if (OB_UNLIKELY(1 != inc_subpart_num)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc subpart num should be 1", KR(ret), K(inc_subpart_num));
      } else if (OB_ISNULL(inc_subpart = subpart_array[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_subpart is null", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
              ||OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))
              ||OB_FAIL(dml.add_pk_column("part_id", inc_part->get_part_id()))
              ||OB_FAIL(dml.add_pk_column("sub_part_id", inc_subpart->get_sub_part_id()))
              ||OB_FAIL(dml.add_column("schema_version", schema_version_))
              ||OB_FAIL(dml.add_column("sub_part_name", inc_subpart->get_part_name().ptr()))) {
        LOG_WARN("dml add column failed", KR(ret));
      } else if (OB_FAIL(dml.splice_update_sql(share::OB_ALL_SUB_PART_TNAME, subpart_sql))) {
        LOG_WARN("dml splice update sql failed", KR(ret));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, subpart_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql",KR(ret), K(tenant_id), K(subpart_sql));
      } else if (OB_UNLIKELY(inc_subpart_num != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret), K(inc_subpart_num), K(affected_rows));
      } else {
        ObDMLSqlSplicer history_sub_dml;
        ObSqlString subpart_history_sql;
        affected_rows = 0;
        HEAP_VAR(ObAddIncSubPartDMLGenerator, sub_part_dml_gen,
                ori_table_, *inc_part, *inc_subpart, inc_part_num, inc_part->get_part_idx(),
                inc_subpart->get_sub_part_idx(), schema_version_) {
          if (OB_FAIL(sub_part_dml_gen.gen_dml(history_sub_dml))) {
            LOG_WARN("gen dml history failed", KR(ret));
          } else if (OB_FAIL(history_sub_dml.add_column("is_deleted", false))) {
            LOG_WARN("add column failed", KR(ret));
          } else if (OB_FAIL(history_sub_dml.splice_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME,
                                                              subpart_history_sql))) {
            LOG_WARN("failed to splice insert sql", KR(ret), K(subpart_history_sql));
          } else if (OB_FAIL(sql_client_.write(exec_tenant_id,subpart_history_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", KR(ret), K(subpart_history_sql));
          } else if (OB_UNLIKELY(inc_subpart_num != affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("history affected_rows is unexpected", KR(ret), K(inc_part_num), K(affected_rows));
          }
        }
      }
    }
  }
  return ret;
}
} //end of schema
} //end of share
} //end of oceanbase
