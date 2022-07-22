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

namespace oceanbase {
using namespace obrpc;
namespace share {
namespace schema {
using namespace common;

int ObPartDMLGenerator::add_part_name_column(
    const ObPartitionSchema* table, const ObPartition& part, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    ObString part_name;
    char name_buf[OB_MAX_PARTITION_NAME_LENGTH];
    // add column will copy part_name
    MEMSET(name_buf, 0, OB_MAX_PARTITION_NAME_LENGTH);
    if (OB_FAIL(table->get_part_name(part.get_part_id(), name_buf, OB_MAX_PARTITION_NAME_LENGTH, NULL))) {
      LOG_WARN("get part name failed", K(part.get_part_id()));
    } else {
      part_name = ObString(strlen(name_buf), name_buf);
      if (OB_FAIL(dml.add_column("part_name", part_name))) {
        LOG_WARN("dml add part info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPartDMLGenerator::gen_dml(ObDMLSqlSplicer& dml)
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
    const ObRowkey& high_bound_val, ObString& high_bound_val_str, ObString& b_high_bound_val_str, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  // TODO: add session timezone_info is better
  ObTimeZoneInfo tz_info;
  tz_info.set_offset(0);
  if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {
    LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
                 high_bound_val, high_bound_val_, OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos, false, &tz_info))) {
    LOG_WARN("Failed to convert rowkey to sql text", K(ret));
  } else {
    high_bound_val_str.assign_ptr(high_bound_val_, static_cast<int32_t>(pos));
  }
  if (OB_SUCC(ret)) {
    pos = 0;
    if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(
            high_bound_val, b_high_bound_val_, OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else {
      b_high_bound_val_str.assign_ptr(b_high_bound_val_, static_cast<int32_t>(pos));
    }
  }

  return ret;
}
int ObPartDMLGenerator::gen_list_val_str(const common::ObIArray<common::ObNewRow>& list_value,
    common::ObString& list_val_str, common::ObString& b_list_val_str, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // TODO: add session timezone_info is better
  ObTimeZoneInfo tz_info;
  tz_info.set_offset(0);
  if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {
    LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
                 list_value, list_val_, OB_MAX_B_PARTITION_EXPR_LENGTH, pos, false, &tz_info))) {
    LOG_WARN("failed to convert row to sql test", K(ret));
  } else {
    list_val_str.assign_ptr(list_val_, static_cast<int32_t>(pos));
  }
  pos = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(
                 ObPartitionUtils::convert_rows_to_hex(list_value, b_list_val_, OB_MAX_B_PARTITION_EXPR_LENGTH, pos))) {
    LOG_WARN("failed to convert rowkey to hex", K(ret));
  } else {
    b_list_val_str.assign_ptr(b_list_val_, static_cast<int32_t>(pos));
  }
  return ret;
}

bool ObPartSqlHelper::is_tablegroup_def()
{
  return is_tablegroup_def_;
}

int ObPartSqlHelper::iterate_all_part(const bool only_history, const bool deal_with_delay_delete_parts /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table_->is_user_partition_table()) {
    //  } else if (table_->is_user_partition_table()
    //      && (is_tablegroup_def_ || (table_->is_range_part() || table_->is_list_part()))) { // for debug
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString part_sql;
    ObSqlString part_history_sql;
    ObSqlString value_str;
    const ObPartitionOption& part_expr = table_->get_part_option();
    int64_t part_num = part_expr.get_part_num();
    ObPartition** part_array = table_->get_part_array();
    int64_t count = 0;
    int64_t max_used_part_id = part_expr.get_max_used_part_id();
    if (deal_with_delay_delete_parts) {
      part_num = table_->get_dropped_partition_num();
      part_array = table_->get_dropped_part_array();
    }
    if (max_used_part_id < 0) {
      max_used_part_id = part_num - 1;
    } else if (max_used_part_id < part_num - 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max_used_part_id is invalid", K(ret), K(max_used_part_id), K(part_num));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
      dml.reset();
      // for compatible
      ObPartition* part = NULL;
      ObPartition mock_part;
      if (part_array != NULL) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(part_array[i]));
        } else {
          part = part_array[i];
        }
      } else {
        mock_part.set_part_id(max_used_part_id - part_num + 1 + i);
        mock_part.set_part_idx(i);
        part = &mock_part;
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

int ObPartSqlHelper::iterate_all_sub_part(
    const bool only_history, bool deal_with_delay_delete_parts, bool deal_with_delay_delete_subparts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table_->is_sub_part_template()) {
    if (!deal_with_delay_delete_parts && !deal_with_delay_delete_subparts &&
        OB_FAIL(iterate_all_sub_part_for_template(only_history))) {
      LOG_WARN("fail to iterate all sub part for template", KR(ret));
    }
  } else {
    if (OB_FAIL(iterate_all_sub_part_for_nontemplate(
            only_history, deal_with_delay_delete_parts, deal_with_delay_delete_subparts))) {
      LOG_WARN("fail to iterate all sub part for nontemplate", KR(ret));
    }
  }
  return ret;
}

int ObPartSqlHelper::iterate_all_sub_part_for_template(const bool only_history)
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
    const int64_t part_num = table_->get_part_option().get_part_num();
    ObPartition** part_array = table_->get_part_array();
    ObSubPartition** def_subpart_array = table_->get_def_subpart_array();
    ObSubPartition* subpart = NULL;
    const int64_t def_sub_part_num = table_->get_sub_part_option().get_part_num();
    int64_t count = 0;
    int64_t max_used_part_id = table_->get_part_option().get_max_used_part_id();
    if (max_used_part_id < 0) {
      max_used_part_id = part_num - 1;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < def_sub_part_num; j++) {
        dml.reset();
        int64_t part_id = max_used_part_id - part_num + 1 + i;
        if (NULL != part_array && NULL != part_array[i]) {
          part_id = part_array[i]->get_part_id();
        }
        if (NULL != def_subpart_array && NULL != def_subpart_array[j]) {
          subpart = def_subpart_array[j];
        }
        if (OB_SUCC(ret)) {
          if (part_id < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part_id is invalid", K(ret), K(part_id));
          } else if (OB_FAIL(add_subpart_dml_column(exec_tenant_id, table_, part_id, j, subpart, dml))) {
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
          if (count >= MAX_DML_NUM || (i == part_num - 1 && j == def_sub_part_num - 1)) {
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

int ObPartSqlHelper::iterate_all_sub_part_for_nontemplate(const bool only_history,
    const bool deal_with_delay_delete_parts, /* = false */
    const bool deal_with_delay_delete_subparts /* = false */)
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
    ObPartition** part_array = table_->get_part_array();
    if (deal_with_delay_delete_parts) {
      part_num = table_->get_dropped_partition_num();
      part_array = table_->get_dropped_part_array();
    }
    ObSubPartition** subpart_array = NULL;
    ObSubPartition* subpart = NULL;
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
        if (deal_with_delay_delete_subparts) {
          subpart_array = part_array[i]->get_dropped_subpart_array();
          sub_part_num = part_array[i]->get_dropped_subpartition_num();
        }
        part_id = part_array[i]->get_part_id();
        if (part_id < 0 || sub_part_num < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_id or sub_part_num is invalid", K(ret), K(part_id), K(sub_part_num));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < sub_part_num; j++) {
        dml.reset();
        int sub_part_id = -1;
        if (OB_ISNULL(subpart_array) || OB_ISNULL(subpart_array[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart_array is invalid", KR(ret));
        } else {
          subpart = subpart_array[j];
          sub_part_id = subpart->get_sub_part_id();
          if (sub_part_id < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub_part_id or is invalid", K(ret), K(sub_part_id));
          } else if (OB_FAIL(add_subpart_dml_column(exec_tenant_id, table_, part_id, sub_part_id, subpart, dml))) {
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
  } else if (!table_->is_sub_part_template()) {
  } else if (table_->is_user_subpartition_table()) {
    //  } else if (table_->is_user_subpartition_table()
    //      && (is_tablegroup_def_ || (table_->is_range_subpart() || table_->is_list_subpart()))) { // for debug
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString value_str;
    ObSqlString part_sql;
    ObSqlString part_history_sql;
    const int64_t def_sub_part_num = table_->get_sub_part_option().get_part_num();
    ObSubPartition** def_subpart_array = table_->get_def_subpart_array();
    ObSubPartition* subpart = NULL;
    int64_t count = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < def_sub_part_num; j++) {
      dml.reset();
      int64_t mapping_pg_sub_part_id = -1;
      if (NULL != def_subpart_array && NULL != def_subpart_array[j]) {
        subpart = def_subpart_array[j];
      }
      if (nullptr != def_subpart_array) {
        if (nullptr == def_subpart_array[j]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null ptr", K(ret), KP(def_subpart_array[j]));
        } else {
          mapping_pg_sub_part_id = def_subpart_array[j]->get_mapping_pg_sub_part_id();
        }
      } else {
        mapping_pg_sub_part_id = -1;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(add_def_subpart_dml_column(exec_tenant_id, table_, j, mapping_pg_sub_part_id, subpart, dml))) {
        LOG_WARN("add dml column failed", K(ret));
      }
      if (OB_SUCC(ret)) {
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
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_DEF_SUB_PART_HISTORY_TNAME, part_history_sql))) {
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

int ObAddPartInfoHelper::add_partition_info_for_gc()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (!table_->has_self_partition()) {
    // skip
  } else {
    const uint64_t tenant_id = table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObArray<int64_t> part_ids;
    ObPartIdsGenerator gen(*table_);
    ObDMLSqlSplicer dml;
    const int64_t inc_part_num = table_->get_part_option().get_part_num();
    if (OB_FAIL(gen.gen(part_ids))) {
      LOG_WARN("generate part ids failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id))) ||
          OB_FAIL(
              dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(tenant_id, table_->get_table_id()))) ||
          OB_FAIL(dml.add_pk_column("partition_id", part_ids.at(i)))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), K(i), K(part_ids));
      }
    }  // end for
    if (OB_SUCC(ret)) {
      ObSqlString part_gc_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_TENANT_GC_PARTITION_INFO_TNAME, part_gc_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_gc_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, part_gc_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_gc_sql));
      } else if (part_ids.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(part_ids), K(affected_rows));
      }
    }
  }

  return ret;
}

int ObAddPartInfoHelper::add_part_info_dml_column(
    const uint64_t exec_tenant_id, const ObPartitionSchema* table, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    const ObPartitionOption& part_option = table->get_part_option();
    const ObPartitionOption& subpart_option = table->get_sub_part_option();
    if (OB_FAIL(dml.add_pk_column(
            "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
        OB_FAIL(dml.add_pk_column(
            "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
        OB_FAIL(dml.add_column("part_type", part_option.get_part_func_type())) ||
        OB_FAIL(dml.add_column(OBJ_GET_K(part_option, part_num))) || OB_FAIL(dml.add_column("part_space", 0)) ||
        OB_FAIL(dml.add_column("part_expr", part_option.get_part_func_expr_str())) ||
        OB_FAIL(dml.add_column("sub_part_type", subpart_option.get_part_func_type())) ||
        OB_FAIL(dml.add_column("def_sub_part_num", subpart_option.get_part_num())) ||
        OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version())) ||
        OB_FAIL(dml.add_column("sub_part_expr", subpart_option.get_part_func_expr()))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_part_dml_column(
    const uint64_t exec_tenant_id, const ObPartitionSchema* table, const ObPartition& part, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    int64_t sub_part_num = 0;
    if (PARTITION_LEVEL_TWO == table->get_part_level()) {
      if (table->is_sub_part_template()) {
        sub_part_num = table->get_sub_part_option().get_part_num();
      } else {
        sub_part_num = part.get_sub_part_num();
      }
    }
    if (OB_FAIL(dml.add_pk_column(
            "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
        OB_FAIL(dml.add_pk_column(
            "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
        OB_FAIL(dml.add_pk_column("part_id", part.get_part_id())) ||
        OB_FAIL(dml.add_pk_column("part_idx", part.get_part_idx())) ||
        OB_FAIL(dml.add_column("schema_version", table->get_schema_version())) ||
        OB_FAIL(dml.add_column("sub_part_num", sub_part_num)) ||
        OB_FAIL(dml.add_column("sub_part_space", 0 /*unused now*/)) || OB_FAIL(dml.add_column("new_sub_part_num", 0)) ||
        OB_FAIL(dml.add_column("new_sub_part_space", 0 /*unused now*/)) ||
        OB_FAIL(dml.add_column("status", PARTITION_STATUS_INVALID)) ||
        OB_FAIL(dml.add_column("spare1", 0 /*unused now*/)) || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/)) ||
        OB_FAIL(dml.add_column("spare3", "" /*unused now*/)) || OB_FAIL(dml.add_column("comment", "" /*unused now*/)) ||
        OB_FAIL(dml.add_column("mapping_pg_part_id", part.get_mapping_pg_part_id())) ||
        OB_FAIL(dml.add_column("drop_schema_version", part.get_drop_schema_version())) ||
        OB_FAIL(dml.add_column("max_used_sub_part_id", part.get_max_used_sub_part_id()))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (OB_FAIL(ObPartDMLGenerator::add_part_name_column(table, part, dml))) {
      LOG_WARN("add part name failed", K(ret), K(table), K(part));
    } else if (OB_FAIL(add_part_high_bound_val_column(table, part, dml))) {
      LOG_WARN("add part high bound failed", K(ret), K(table), K(part));
    } else if (OB_FAIL(add_part_list_val_column(table, part, dml))) {
      LOG_WARN("add list val failed", K(ret), K(table), K(part));
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
    const int64_t part_idx, const int64_t subpart_idx, const ObSubPartition* subpart, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    if (OB_FAIL(dml.add_pk_column(
            "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
        OB_FAIL(dml.add_pk_column(
            "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
        OB_FAIL(dml.add_pk_column("part_id", part_idx)) || OB_FAIL(dml.add_pk_column("sub_part_id", subpart_idx)) ||
        OB_FAIL(dml.add_column("schema_version", table->get_schema_version())) ||
        OB_FAIL(dml.add_column("status", PARTITION_STATUS_INVALID)) ||
        OB_FAIL(dml.add_column("spare1", 0 /*unused now*/)) || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/)) ||
        OB_FAIL(dml.add_column("spare3", "" /*unused now*/)) || OB_FAIL(dml.add_column("comment", "")) ||
        OB_FAIL(dml.add_column("sub_part_idx", subpart_idx)) || OB_FAIL(dml.add_column("source_partition_id", -1)) ||
        OB_FAIL(dml.add_column(
            "mapping_pg_sub_part_id", NULL == subpart ? OB_INVALID_ID : subpart->get_mapping_pg_sub_part_id())) ||
        OB_FAIL(dml.add_column(
            "drop_schema_version", NULL == subpart ? OB_INVALID_ID : subpart->get_drop_schema_version()))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (OB_FAIL(add_subpart_name_column(table,
                   part_idx,
                   subpart_idx,
                   false,  // is_def_subpart
                   dml))) {
      LOG_WARN("add subpart name failed", K(ret), KPC(table), K(part_idx), K(subpart_idx));
    } else if (OB_FAIL(add_subpart_high_bound_val_column(table, *subpart, dml))) {
      LOG_WARN("add part high bound failed", K(ret), KPC(table), K(subpart_idx));
    } else if (OB_FAIL(add_subpart_list_val_column(table, *subpart, dml))) {
      LOG_WARN("add list value failed", K(ret), K(table), K(subpart_idx));
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_def_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
    const int64_t def_subpart_idx, const int64_t mapping_pg_sub_part_id, const ObSubPartition* subpart,
    ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    // For sub-partitions, dynamic addition and deletion of partitions and partition splits are currently not supported,
    // so sub_part_idx and sub_part_id are equal
    if (OB_FAIL(dml.add_pk_column(
            "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
        OB_FAIL(dml.add_pk_column(
            "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
        OB_FAIL(dml.add_pk_column("sub_part_id", def_subpart_idx)) ||
        OB_FAIL(dml.add_column("schema_version", table->get_schema_version())) ||
        OB_FAIL(dml.add_column("spare1", 0 /*unused now*/)) || OB_FAIL(dml.add_column("spare2", 0 /*unused now*/)) ||
        OB_FAIL(dml.add_column("spare3", "" /*unused now*/)) || OB_FAIL(dml.add_column("comment", "")) ||
        OB_FAIL(dml.add_column("sub_part_idx", def_subpart_idx)) ||
        OB_FAIL(dml.add_column("mapping_pg_sub_part_id", mapping_pg_sub_part_id))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (OB_FAIL(add_subpart_name_column(table,
                   ObSubPartition::TEMPLATE_PART_ID,
                   def_subpart_idx,
                   true,  // is_def_subpart
                   dml))) {
      LOG_WARN("add subpart name failed", K(ret), KPC(table), K(def_subpart_idx));
    } else if (OB_FAIL(add_subpart_high_bound_val_column(table, *subpart, dml))) {
      LOG_WARN("add part high bound failed", K(ret), KPC(table), K(def_subpart_idx));
    } else if (OB_FAIL(add_subpart_list_val_column(table, *subpart, dml))) {
      LOG_WARN("add list value failed", K(ret), K(table), K(def_subpart_idx));
    }
  }

  return ret;
}

int ObAddPartInfoHelper::add_subpart_name_column(const ObPartitionSchema* table, const int64_t part_idx,
    const int64_t subpart_idx, const bool is_def_subpart, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    ObString sub_part_name;
    char name_buf[OB_MAX_PARTITION_NAME_LENGTH];
    MEMSET(name_buf, 0, OB_MAX_PARTITION_NAME_LENGTH);
    if (OB_FAIL(table->get_subpart_name(
            part_idx, subpart_idx, is_def_subpart, name_buf, OB_MAX_PARTITION_NAME_LENGTH, NULL))) {
    } else {
      sub_part_name = ObString(strlen(name_buf), name_buf);
      if (OB_FAIL(dml.add_column("sub_part_name", sub_part_name))) {
        LOG_WARN("dml add part info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAddPartInfoHelper::add_part_high_bound_val_column(
    const ObPartitionSchema* table, const ObBasePartition& part, ObDMLSqlSplicer& dml)
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

int ObAddPartInfoHelper::add_part_list_val_column(
    const ObPartitionSchema* table, const ObBasePartition& part, ObDMLSqlSplicer& dml)
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

int ObAddPartInfoHelper::add_subpart_high_bound_val_column(
    const ObPartitionSchema* table, const ObBasePartition& part, ObDMLSqlSplicer& dml)
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

int ObAddPartInfoHelper::add_subpart_list_val_column(
    const ObPartitionSchema* table, const ObBasePartition& part, ObDMLSqlSplicer& dml)
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

template <typename P>
int ObAddPartInfoHelper::add_high_bound_val_column(const P& part_option, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (high_bound_val_ == NULL) {
    high_bound_val_ = static_cast<char*>(allocator_.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
    if (OB_ISNULL(high_bound_val_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("high_bound_val is null", K(ret), K(high_bound_val_));
    }
  }
  // determine if it is a list partition
  if (OB_SUCC(ret)) {
    MEMSET(high_bound_val_, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    int64_t pos = 0;
    // TODO: add session timezone_info is better
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    if (OB_FAIL(OTTZ_MGR.get_tenant_tz(table_->get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(table_->get_tenant_id()));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(part_option.get_high_bound_val(),
                   high_bound_val_,
                   OB_MAX_B_HIGH_BOUND_VAL_LENGTH,
                   pos,
                   false,
                   &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(ret));
    } else if (OB_FAIL(dml.add_column("high_bound_val", ObHexEscapeSqlStr(ObString(pos, high_bound_val_))))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(
                   part_option.get_high_bound_val(), high_bound_val_, OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else if (OB_FAIL(dml.add_column("b_high_bound_val", ObString(pos, high_bound_val_)))) {
      LOG_WARN("Failed to add column b_high_bound_val", K(ret));
    } else {
      LOG_DEBUG("high bound info", "high_bound_val", ObString(pos, high_bound_val_).ptr(), K(pos));
    }  // do nothing
  }
  return ret;
}

template <class P>
int ObAddPartInfoHelper::add_list_val_column(const P& part_option, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (list_val_ == NULL) {
    list_val_ = static_cast<char*>(allocator_.alloc(OB_MAX_B_PARTITION_EXPR_LENGTH));
    if (OB_ISNULL(list_val_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("list_val is null", K(ret), K(list_val_));
    }
  }
  // determine if it is a list partition, if it is a list partition
  if (OB_SUCC(ret)) {
    MEMSET(list_val_, 0, OB_MAX_B_PARTITION_EXPR_LENGTH);
    int64_t pos = 0;
    // TODO: add session timezone_info is better
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    if (OB_FAIL(OTTZ_MGR.get_tenant_tz(table_->get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(table_->get_tenant_id()));
    } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(part_option.get_list_row_values(),
                   list_val_,
                   OB_MAX_B_PARTITION_EXPR_LENGTH,
                   pos,
                   false,
                   &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(ret));
    } else if (OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(ObString(pos, list_val_))))) {
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
  const bool deal_with_delay_delete_parts = false;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(iterate_part_info(is_only_history))) {
    LOG_WARN("drop part info failed", K(ret));
  } else if (OB_FAIL(iterate_all_part(is_only_history, deal_with_delay_delete_parts))) {
    LOG_WARN("drop all part failed", K(ret));
  } else if (OB_FAIL(iterate_all_sub_part(is_only_history))) {
    LOG_WARN("drop all sub part failed", K(ret));
  } else if (OB_FAIL(iterate_all_def_sub_part(is_only_history))) {
    LOG_WARN("drop all def sub part failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::delete_dropped_partition_info()
{
  int ret = OB_SUCCESS;
  const bool is_only_history = true;
  const bool deal_with_delay_delete_parts = true;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(iterate_all_part(is_only_history, deal_with_delay_delete_parts))) {
    LOG_WARN("drop all part failed", K(ret));
  } else if (table_->is_sub_part_template()) {
  } else if (OB_FAIL(iterate_all_sub_part(is_only_history, true, false))) {
    LOG_WARN("drop all sub part failed", K(ret));
  } else if (OB_FAIL(iterate_all_sub_part(is_only_history, true, true))) {
    LOG_WARN("drop all sub part failed", K(ret));
  } else if (OB_FAIL(iterate_all_sub_part(is_only_history, false, true))) {
    LOG_WARN("drop all sub part failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_part_info_dml_column(
    const uint64_t exec_tenant_id, const ObPartitionSchema* table, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
             OB_FAIL(dml.add_pk_column(
                 "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
             OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_part_dml_column(
    const uint64_t exec_tenant_id, const ObPartitionSchema* table, const ObPartition& part, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
             OB_FAIL(dml.add_pk_column(
                 "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
             OB_FAIL(dml.add_pk_column("part_id", part.get_part_id())) ||
             OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
    const int64_t part_idx, const int64_t subpart_idx, const ObSubPartition* subpart, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  UNUSED(subpart);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
             OB_FAIL(dml.add_pk_column(
                 "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
             OB_FAIL(dml.add_pk_column("part_id", part_idx)) ||
             OB_FAIL(dml.add_pk_column("sub_part_id", subpart_idx)) ||
             OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObDropPartInfoHelper::add_def_subpart_dml_column(const uint64_t exec_tenant_id, const ObPartitionSchema* table,
    const int64_t subpart_idx, const int64_t mapping_pg_sub_part_id, const ObSubPartition* subpart,
    share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  UNUSED(mapping_pg_sub_part_id);
  UNUSED(subpart);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table->get_tenant_id()))) ||
             OB_FAIL(dml.add_pk_column(
                 "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table->get_table_id()))) ||
             OB_FAIL(dml.add_pk_column("sub_part_id", subpart_idx)) ||
             OB_FAIL(dml.add_pk_column("schema_version", table->get_schema_version()))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  return ret;
}

int ObAddIncSubPartDMLGenerator::convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(
          dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, part_info.table_id_))) ||
      OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_)) ||
      OB_FAIL(dml.add_pk_column("sub_part_id", part_info.sub_part_id_)) ||
      OB_FAIL(dml.add_pk_column("sub_part_idx", part_info.sub_part_idx_)) ||
      OB_FAIL(dml.add_column("sub_part_name", part_info.part_name_)) ||
      OB_FAIL(dml.add_column("schema_version", part_info.schema_version_)) ||
      OB_FAIL(dml.add_column("status", part_info.status_)) || OB_FAIL(dml.add_column("spare1", 0)) ||
      OB_FAIL(dml.add_column("spare2", 0)) || OB_FAIL(dml.add_column("spare3", "")) ||
      OB_FAIL(dml.add_column("comment", "")) ||
      OB_FAIL(dml.add_column("high_bound_val", ObHexEscapeSqlStr(part_info.high_bound_val_))) ||
      OB_FAIL(dml.add_column("b_high_bound_val", part_info.b_high_bound_val_)) ||
      OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(part_info.list_val_))) ||
      OB_FAIL(dml.add_column("b_list_val", part_info.b_list_val_)) ||
      OB_FAIL(dml.add_column("mapping_pg_sub_part_id", part_info.mapping_pg_sub_part_id_)) ||
      OB_FAIL(dml.add_column("drop_schema_version", part_info.drop_schema_version_))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  }
  return ret;
}

int ObAddIncSubPartDMLGenerator::extract_part_info(PartInfo& part_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ori_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("template cannot add subpart", K(ret));
  } else if (part_idx_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_idx", K(part_idx_), K(ret));
  } else {
    part_info.tenant_id_ = ori_table_->get_tenant_id();
    part_info.table_id_ = ori_table_->get_table_id();
    // When adding the entire first-level partition, the first-level partition id needs to be generated by rs;
    // When the secondary partition is added separately, the primary partition id is specified by the sql layer.
    if (OB_INVALID_ID != part_.get_part_id()) {
      part_info.part_id_ = part_.get_part_id();
    } else {
      part_info.part_id_ = ori_table_->get_part_option().get_max_used_part_id() - inc_part_num_ + part_idx_ + 1;
    }

    part_info.sub_part_id_ = part_.get_max_used_sub_part_id() - part_.get_subpartition_num() + subpart_idx_ + 1;
    part_info.part_name_ = sub_part_.get_part_name();
    part_info.schema_version_ = schema_version_;
    part_info.status_ = PARTITION_STATUS_INVALID;
    part_info.spare1_ = 0;
    part_info.spare2_ = 0;
    part_info.spare3_ = "";
    part_info.comment_ = "";
    part_info.mapping_pg_sub_part_id_ = sub_part_.get_mapping_pg_sub_part_id();
    part_info.sub_part_idx_ = -1;
    part_info.drop_schema_version_ = OB_INVALID_VERSION;

    if (ori_table_->is_range_subpart()) {
      if (OB_FAIL(gen_high_bound_val_str(sub_part_.get_high_bound_val(),
              part_info.high_bound_val_,
              part_info.b_high_bound_val_,
              part_info.tenant_id_))) {
        LOG_WARN("generate high bound val failed", K(ret));
      }
    } else if (ori_table_->is_list_subpart()) {
      if (OB_FAIL(gen_list_val_str(
              sub_part_.get_list_row_values(), part_info.list_val_, part_info.b_list_val_, part_info.tenant_id_))) {
        LOG_WARN("generate listval failed", K(ret));
      }
    } else if (is_hash_like_part(ori_table_->get_sub_part_option().get_part_func_type())) {
      part_info.sub_part_idx_ = subpart_idx_;
    }
  }

  return ret;
}

int ObAddIncSubPartDMLGenerator::extract_part_info_for_delay_delete(PartInfo& part_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(extract_part_info(part_info))) {
    LOG_WARN("extract part info failed", K(ret));
  } else {
    part_info.sub_part_id_ = sub_part_.get_sub_part_id();
    part_info.drop_schema_version_ = schema_version_;
  }

  return ret;
}

int ObAddIncSubPartDMLGenerator::gen_dml_for_delay_delete(ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  PartInfo part_info;
  if (OB_FAIL(extract_part_info_for_delay_delete(part_info))) {
    LOG_WARN("extract part info for delay delete failed", K(ret));
  } else if (OB_FAIL(convert_to_dml(part_info, dml))) {
    LOG_WARN("convert to dml failed", K(ret));
  }
  return ret;
}

int ObAddIncPartDMLGenerator::gen_dml_for_delay_delete(ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  PartInfo part_info;
  if (OB_FAIL(extract_part_info_for_delay_delete(part_info))) {
    LOG_WARN("extract part info for delay delete failed", K(ret));
  } else if (OB_FAIL(convert_to_dml(part_info, dml))) {
    LOG_WARN("convert to dml failed", K(ret));
  }
  return ret;
}

int ObAddIncPartDMLGenerator::convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(
          dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, part_info.table_id_))) ||
      OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_)) ||
      OB_FAIL(dml.add_pk_column("part_idx", part_info.part_idx_)) ||
      OB_FAIL(dml.add_pk_column("part_name", part_info.part_name_)) ||
      OB_FAIL(dml.add_column("schema_version", part_info.schema_version_)) ||
      OB_FAIL(dml.add_column("sub_part_num", part_info.sub_part_num_)) ||
      OB_FAIL(dml.add_column("sub_part_space", part_info.sub_part_space_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("new_sub_part_num", part_info.new_sub_part_num_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("new_sub_part_space", part_info.new_sub_part_space_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("status", part_info.status_)) ||
      OB_FAIL(dml.add_column("spare1", part_info.spare1_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("spare2", part_info.spare2_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("spare3", part_info.spare3_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("comment", part_info.comment_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("high_bound_val", ObHexEscapeSqlStr(part_info.high_bound_val_))) ||
      OB_FAIL(dml.add_column("b_high_bound_val", part_info.b_high_bound_val_)) ||
      OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(part_info.list_val_))) ||
      OB_FAIL(dml.add_column("b_list_val", part_info.b_list_val_)) ||
      OB_FAIL(dml.add_column("mapping_pg_part_id", part_info.mapping_pg_part_id_)) ||
      OB_FAIL(dml.add_column("drop_schema_version", part_info.drop_schema_version_)) ||
      OB_FAIL(dml.add_column("max_used_sub_part_id", part_info.max_used_sub_part_id_))) {
    LOG_WARN("dml add part info failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  }
  return ret;
}

int ObAddIncPartDMLGenerator::extract_part_info(PartInfo& part_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ori_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else {
    int64_t sub_part_num = 0;
    if (PARTITION_LEVEL_TWO == ori_table_->get_part_level() && !ori_table_->is_sub_part_template()) {
      sub_part_num = part_.get_sub_part_num();
    } else if (PARTITION_LEVEL_TWO == ori_table_->get_part_level() && ori_table_->is_sub_part_template()) {
      sub_part_num = ori_table_->get_sub_part_option().get_part_num();
    }
    part_info.tenant_id_ = ori_table_->get_tenant_id();
    part_info.table_id_ = ori_table_->get_table_id();
    // When adding the entire first-level partition, part_id is generated by rs,
    // But when adding a secondary partition separately, part_id is determined by sql
    // But need to write __all_part_history table, also need to generate additional items through this function
    if (OB_INVALID_VERSION != part_idx_) {
      part_info.part_id_ = ori_table_->get_part_option().get_max_used_part_id() - inc_part_num_ + part_idx_ + 1;
    } else {
      part_info.part_id_ = part_.get_part_id();
    }
    part_info.part_name_ = part_.get_part_name();
    part_info.schema_version_ = schema_version_;
    part_info.sub_part_num_ = sub_part_num;
    part_info.sub_part_space_ = 0;
    part_info.new_sub_part_num_ = 0;
    part_info.new_sub_part_space_ = 0;
    part_info.status_ = PARTITION_STATUS_INVALID;
    part_info.spare1_ = 0;
    part_info.spare2_ = 0;
    part_info.spare3_ = "";
    part_info.comment_ = "";
    part_info.mapping_pg_part_id_ = part_.get_mapping_pg_part_id();
    part_info.part_idx_ = -1;
    part_info.drop_schema_version_ = OB_INVALID_VERSION;
    part_info.max_used_sub_part_id_ = part_.get_max_used_sub_part_id();

    if (ori_table_->is_range_part()) {
      if (OB_FAIL(gen_high_bound_val_str(part_.get_high_bound_val(),
              part_info.high_bound_val_,
              part_info.b_high_bound_val_,
              part_info.tenant_id_))) {
        LOG_WARN("generate high bound val failed", K(ret));
      }
    } else if (ori_table_->is_list_part()) {
      if (OB_FAIL(gen_list_val_str(
              part_.get_list_row_values(), part_info.list_val_, part_info.b_list_val_, part_info.tenant_id_))) {
        LOG_WARN("generate listval failed", K(ret));
      }
    } else if (is_hash_like_part(ori_table_->get_part_option().get_part_func_type())) {
      part_info.part_idx_ = part_.get_part_idx();
    }
  }

  return ret;
}

int ObAddIncPartDMLGenerator::extract_part_info_for_delay_delete(PartInfo& part_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(extract_part_info(part_info))) {
    LOG_WARN("extract part info failed", K(ret));
  } else {
    part_info.part_id_ = part_.get_part_id();
    part_info.drop_schema_version_ = schema_version_;
  }

  return ret;
}

int ObDropIncSubPartDMLGenerator::convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t deleted = true;
  if (OB_FAIL(
          dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, part_info.tenant_id_))) ||
      OB_FAIL(
          dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, part_info.table_id_))) ||
      OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_)) ||
      OB_FAIL(dml.add_pk_column("sub_part_id", part_info.sub_part_id_)) ||
      OB_FAIL(dml.add_column("is_deleted", deleted)) ||
      OB_FAIL(dml.add_column("schema_version", part_info.schema_version_))) {
    LOG_WARN("dml drop part info failed", K(ret));
  }
  return ret;
}

int ObDropIncSubPartDMLGenerator::extract_part_info(PartInfo& part_info)
{
  int ret = OB_SUCCESS;

  part_info.tenant_id_ = sub_part_.get_tenant_id();
  part_info.table_id_ = sub_part_.get_table_id();
  part_info.part_id_ = sub_part_.get_part_id();
  part_info.sub_part_id_ = sub_part_.get_sub_part_id();
  part_info.schema_version_ = schema_version_;

  return ret;
}

int ObDropIncPartDMLGenerator::convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(
          dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, part_info.tenant_id_))) ||
      OB_FAIL(
          dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, part_info.table_id_))) ||
      OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_)) ||
      OB_FAIL(dml.add_column("schema_version", part_info.schema_version_))) {
    LOG_WARN("dml drop part info failed", K(ret));
  }
  return ret;
}

int ObDropIncPartDMLGenerator::extract_part_info(PartInfo& part_info)
{
  int ret = OB_SUCCESS;

  part_info.tenant_id_ = part_.get_tenant_id();
  part_info.table_id_ = part_.get_table_id();
  part_info.part_id_ = part_.get_part_id();
  part_info.schema_version_ = schema_version_;

  return ret;
}

int ObAddIncPartHelper::add_partition_info_for_gc()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (!ori_table_->has_self_partition()) {
    // skip
  } else {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    ObDMLSqlSplicer dml;
    ObArray<int64_t> part_ids;
    ObPartIdsGeneratorForAdd<ObPartitionSchema> gen(*ori_table_, *inc_table_);
    if (OB_FAIL(gen.gen(part_ids))) {
      LOG_WARN("fail to gen partition ids", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      if (OB_FAIL(dml.add_pk_column(
              "tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, ori_table_->get_tenant_id()))) ||
          OB_FAIL(dml.add_pk_column(
              "table_id", ObSchemaUtils::get_extract_schema_id(tenant_id, ori_table_->get_table_id()))) ||
          OB_FAIL(dml.add_pk_column("partition_id", part_ids.at(i)))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), K(i), K(part_ids));
      }
    }  // end for
    if (OB_SUCC(ret)) {
      ObSqlString part_gc_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_TENANT_GC_PARTITION_INFO_TNAME, part_gc_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_gc_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, part_gc_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_gc_sql));
      } else if (part_ids.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(part_ids), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObAddIncPartHelper::add_partition_info(const bool is_delay_delete)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (PARTITION_LEVEL_TWO == ori_table_->get_part_level() && !ori_table_->is_sub_part_template()) {
    if (OB_FAIL(add_partition_info_for_nontemplate(is_delay_delete))) {
      LOG_WARN("fail to add partition for nontemplate", K(ret));
    }
  } else {
    if (OB_FAIL(add_partition_info_for_template(is_delay_delete))) {
      LOG_WARN("fail to add partition for template", K(ret));
    }
  }
  return ret;
}

int ObAddIncPartHelper::add_partition_info_for_template(const bool is_delay_delete)
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
    const ObPartitionOption& part_expr = inc_table_->get_part_option();
    ObPartition** part_array = inc_table_->get_part_array();
    const int64_t inc_part_num = part_expr.get_part_num();
    const bool only_history = is_delay_delete;
    const int64_t deleted = false;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      if (OB_ISNULL(part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part array is null", K(ret), K(i), K(inc_table_));
      } else {
        ObAddIncPartDMLGenerator part_dml_gen(ori_table_, *part_array[i], inc_part_num, i, schema_version_);
        if (only_history) {
        } else if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
          LOG_WARN("gen dml failed", K(ret));
        } else if (OB_FAIL(dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (is_delay_delete) {
          if (OB_FAIL(part_dml_gen.gen_dml_for_delay_delete(history_dml))) {
            LOG_WARN("gen dml history failed", K(ret));
          }
        } else {
          if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
            LOG_WARN("gen dml history failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(history_dml.add_column("is_deleted", deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(history_dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_dml.splice_batch_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
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
      if (!only_history) {
        if (OB_FAIL(dml.splice_batch_insert_sql(share::OB_ALL_PART_TNAME, part_sql))) {
          LOG_WARN("failed to splice batch insert sql", K(ret), K(part_sql));
        } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(ret), K(part_sql));
        } else if (affected_rows != inc_part_num) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int ObAddIncPartHelper::add_partition_info_for_nontemplate(const bool is_delay_delete)
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
    ObPartition** part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    const bool only_history = is_delay_delete;
    const int64_t deleted = false;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      ObPartition* part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part array is null", K(ret), K(i), K(inc_table_));
      } else {
        ObAddIncPartDMLGenerator part_dml_gen(ori_table_, *part, inc_part_num, i, schema_version_);
        if (only_history) {
        } else if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
          LOG_WARN("gen dml failed", K(ret));
        } else if (OB_FAIL(dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (is_delay_delete) {
          if (OB_FAIL(part_dml_gen.gen_dml_for_delay_delete(history_dml))) {
            LOG_WARN("gen dml history failed", K(ret));
          }
        } else {
          if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
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
        } else if (OB_ISNULL(part->get_subpart_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart array is null", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
            inc_sub_part_num++;
            ObAddIncSubPartDMLGenerator sub_part_dml_gen(
                ori_table_, *part, *part->get_subpart_array()[j], inc_part_num, i, j, schema_version_);

            if (only_history) {
            } else if (OB_FAIL(sub_part_dml_gen.gen_dml(sub_dml))) {
              LOG_WARN("gen sub dml column failed", K(ret));
            } else if (OB_FAIL(sub_dml.finish_row())) {
              LOG_WARN("failed to finish row", K(ret));
            }

            if (OB_FAIL(ret)) {
            } else if (is_delay_delete) {
              if (OB_FAIL(sub_part_dml_gen.gen_dml_for_delay_delete(history_sub_dml))) {
                LOG_WARN("gen dml history failed", K(ret));
              }
            } else {
              if (OB_FAIL(sub_part_dml_gen.gen_dml(history_sub_dml))) {
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
      if (OB_FAIL(history_dml.splice_batch_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret) && !only_history) {
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

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_sub_dml.splice_batch_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME, part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret) && !only_history) {
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

int ObAddIncSubPartHelper::add_subpartition_info_for_gc()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (!ori_table_->has_self_partition()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ori_table has not self partition", K(ret));
  } else {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    ObDMLSqlSplicer dml;
    ObArray<int64_t> part_ids;
    ObPartIdsGeneratorForAdd<ObPartitionSchema> gen(*ori_table_, *inc_table_);
    if (OB_FAIL(gen.gen(part_ids))) {
      LOG_WARN("fail to gen partition ids", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      if (OB_FAIL(dml.add_pk_column(
              "tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, ori_table_->get_tenant_id()))) ||
          OB_FAIL(dml.add_pk_column(
              "table_id", ObSchemaUtils::get_extract_schema_id(tenant_id, ori_table_->get_table_id()))) ||
          OB_FAIL(dml.add_pk_column("partition_id", part_ids.at(i)))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), K(i), K(part_ids));
      }
    }  // end for
    if (OB_SUCC(ret)) {
      ObSqlString part_gc_sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_TENANT_GC_PARTITION_INFO_TNAME, part_gc_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_gc_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, part_gc_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_gc_sql));
      } else if (part_ids.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(part_ids), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObAddIncSubPartHelper::add_subpartition_info(const bool is_delay_delete)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("template cannot add subpartition", K(ret));
  } else if (OB_FAIL(add_subpartition_info_for_nontemplate(is_delay_delete))) {
    LOG_WARN("fail to add partition info for nontemplate", K(ret));
  }
  return ret;
}

int ObAddIncSubPartHelper::add_subpartition_info_for_nontemplate(const bool is_delay_delete)
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
    ObPartition** part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    const bool only_history = is_delay_delete;
    const int64_t deleted = false;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(inc_table_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      ObPartition* part = part_array[i];
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
            ObAddIncSubPartDMLGenerator sub_part_dml_gen(
                ori_table_, *part, *part->get_subpart_array()[j], inc_part_num, i, j, schema_version_);

            if (only_history) {
            } else if (OB_FAIL(sub_part_dml_gen.gen_dml(sub_dml))) {
              LOG_WARN("gen sub dml column failed", K(ret));
            } else if (OB_FAIL(sub_dml.finish_row())) {
              LOG_WARN("failed to finish row", K(ret));
            }

            if (OB_FAIL(ret)) {
            } else if (is_delay_delete) {
              if (OB_FAIL(sub_part_dml_gen.gen_dml_for_delay_delete(history_sub_dml))) {
                LOG_WARN("gen dml history failed", K(ret));
              }
            } else {
              if (OB_FAIL(sub_part_dml_gen.gen_dml(history_sub_dml))) {
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
      if (OB_FAIL(history_sub_dml.splice_batch_insert_sql(share::OB_ALL_SUB_PART_HISTORY_TNAME, part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }

    if (OB_SUCC(ret) && !only_history) {
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
  } else if (PARTITION_LEVEL_TWO == ori_table_->get_part_level() && !ori_table_->is_sub_part_template()) {
    if (OB_FAIL(drop_partition_info_for_nontemplate())) {
      LOG_WARN("fail to drop partition info for nontemplate", K(ret));
    }
  } else {
    if (OB_FAIL(drop_partition_info_for_template())) {
      LOG_WARN("fail to drop partition info for template", K(ret));
    }
  }
  return ret;
}

int ObDropIncPartHelper::drop_partition_info_for_template()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_user_partition_table()) {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString part_history_sql;
    ObSqlString value_str;
    const ObPartitionOption& part_expr = inc_table_->get_part_option();
    ObPartition** part_array = inc_table_->get_part_array();
    const int64_t inc_part_num = part_expr.get_part_num();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      dml.reset();
      ObDropIncPartDMLGenerator part_dml_gen(ori_table_, *part_array[i], schema_version_);
      if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
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
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != inc_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}
int ObDropIncPartHelper::drop_partition_info_for_nontemplate()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("template cannot drop subpart", K(ret));
  } else if (ori_table_->is_user_partition_table()) {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer sub_dml;
    ObSqlString part_history_sql;
    ObSqlString sub_part_history_sql;
    ObSqlString value_str;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    ObPartition** part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      dml.reset();
      ObPartition* part = part_array[i];
      ObDropIncPartDMLGenerator part_dml_gen(ori_table_, *part, schema_version_);
      if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
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

      if (OB_FAIL(ret)) {
      } else {
        for (int64 j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
          sub_dml.reset();
          inc_sub_part_num++;
          ObDropIncSubPartDMLGenerator sub_part_dml_gen(ori_table_, *part->get_subpart_array()[j], schema_version_);

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
    if (OB_SUCC(ret) && inc_part_num > 0) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
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
  } else if (ori_table_->is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("template cannot drop subpart", K(ret));
  } else if (OB_FAIL(drop_subpartition_info_for_nontemplate())) {
    LOG_WARN("fail to drop partition info for nontemplate", K(ret));
  }
  return ret;
}

int ObDropIncSubPartHelper::drop_dropped_subpartition_array()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("template cannot drop subpart", K(ret));
  } else if (OB_FAIL(drop_subpartition_info_for_nontemplate(true))) {
    LOG_WARN("fail to drop partition info for nontemplate", K(ret));
  }
  return ret;
}

int ObDropIncSubPartHelper::drop_subpartition_info_for_nontemplate(const bool deal_with_delay_delete_parts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (ori_table_->is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("template cannot drop subpart", K(ret));
  } else if (ori_table_->is_user_partition_table()) {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer sub_dml;
    ObSqlString part_history_sql;
    ObSqlString sub_part_history_sql;
    ObSqlString value_str;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    ObPartition** part_array = inc_table_->get_part_array();
    int64_t inc_sub_part_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      ObPartition* part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_array[i] is null", K(ret), K(i));
      } else {
        int64_t subpart_num = part->get_subpartition_num();
        ObSubPartition** subpart_array = part->get_subpart_array();
        if (deal_with_delay_delete_parts) {
          subpart_num = part->get_dropped_subpartition_num();
          subpart_array = part->get_dropped_subpart_array();
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_num; j++) {
          sub_dml.reset();
          inc_sub_part_num++;
          ObDropIncSubPartDMLGenerator sub_part_dml_gen(ori_table_, *subpart_array[j], schema_version_);

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
    if (OB_SUCC(ret) && inc_sub_part_num > 0) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, sub_part_history_sql.ptr(), affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(ret), K(sub_part_history_sql));
      } else if (affected_rows != inc_sub_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObSplitPartHelperV2::split_partition()
{
  int ret = OB_SUCCESS;
  const bool only_history = false;
  bool sql_init = false;
  if (schema_version_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner stat", K(ret), K(new_schema_), K(split_info_), K(schema_version_));
  } else {
    const uint64_t tenant_id = new_schema_.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer history_dml;
    ObPartition** part_array = new_schema_.get_part_array();
    int64_t inc_part_num = 0;
    ObPartition* partition = NULL;
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K_(new_schema));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_schema_.get_partition_num(); ++i) {
      partition = part_array[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid part array", K(ret), K(i), K_(new_schema));
      } else if (1 != partition->get_source_part_ids().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("split table every partition must have source partition id", K(partition));
      } else if (partition->get_source_part_ids().at(0) != split_info_.source_part_ids_.at(0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("split table every partition must have source partition id",
            K(ret),
            "partition",
            *partition,
            K(split_info_));
      } else {
        inc_part_num++;
        if (1 == new_schema_.get_partition_num()) {
          partition->get_source_part_ids().reuse();
          if (OB_FAIL(partition->get_source_part_ids().push_back(-1))) {
            LOG_WARN("failed to push back part id", K(ret), K(partition));
          }
        }
      }
      ObSplitPartDMLGeneratorV2 part_dml_gen(new_schema_, *partition, schema_version_);
      if (OB_FAIL(ret)) {
      } else if (only_history) {
      } else if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
        LOG_WARN("gen dml column failed", K(ret), "partition", *partition);
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), "partition", *partition);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
        LOG_WARN("gen history dml failed", K(ret), "partition", *partition);
      } else {
        const int64_t deleted = false;
        if (OB_FAIL(history_dml.add_column("is_deleted", deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(history_dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret), "partition", *partition);
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString sql;
      if (!only_history) {
        if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_PART_TNAME, sql))) {
          LOG_WARN("failed to splice batch insert sql", K(ret), K(sql));
        } else if (OB_FAIL(sql_client_.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(ret), K(sql));
        } else if (affected_rows != inc_part_num) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
        }
      }
      if (OB_SUCC(ret)) {
        sql.reset();
        affected_rows = 0;
        if (OB_FAIL(history_dml.splice_batch_insert_sql(OB_ALL_PART_HISTORY_TNAME, sql))) {
          LOG_WARN("failed to splice batch insert sql", K(ret), K(sql));
        } else if (OB_FAIL(sql_client_.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("execute sql failed", K(ret), K(sql));
        } else if (affected_rows != inc_part_num) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("history affected_rows is unexpected", K(ret), K(inc_part_num), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int ObSplitPartDMLGeneratorV2::convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  ObString part_ids(OB_MAX_CHAR_LENGTH, part_info.source_part_ids_str_);
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(
          dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, part_info.tenant_id_))) ||
      OB_FAIL(
          dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, part_info.table_id_))) ||
      OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_)) ||
      OB_FAIL(dml.add_column("schema_version", part_info.schema_version_)) ||
      OB_FAIL(dml.add_column("sub_part_num", part_info.sub_part_num_)) ||
      OB_FAIL(dml.add_column("sub_part_space", part_info.sub_part_space_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("new_sub_part_num", part_info.new_sub_part_num_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("new_sub_part_space", part_info.new_sub_part_space_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("status", part_info.status_)) ||
      OB_FAIL(dml.add_column("spare1", part_info.spare1_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("spare2", part_info.spare2_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("spare3", part_info.spare3_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("comment", part_info.comment_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("high_bound_val", ObHexEscapeSqlStr(part_info.high_bound_val_))) ||
      OB_FAIL(dml.add_column("b_high_bound_val", part_info.b_high_bound_val_)) ||
      OB_FAIL(dml.add_column("source_partition_id", part_ids)) ||
      OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(part_info.list_val_))) ||
      OB_FAIL(dml.add_column("b_list_val", part_info.b_list_val_)) ||
      OB_FAIL(dml.add_column("part_name", part_info.part_name_)) ||
      OB_FAIL(dml.add_column("part_idx", part_info.part_idx_)) ||
      OB_FAIL(dml.add_column("mapping_pg_part_id", part_info.mapping_pg_part_id_))) {

    LOG_WARN("dml add part info failed", K(ret), K(part_ids));
    //} else if (OB_FAIL(ObPartDMLGenerator::add_part_name_column(inc_table_, part_, dml))) {
    //  LOG_WARN("fail to add part name column", K(ret), K(inc_table_), K(part_));
  }
  return ret;
}

// FIXME:Support non-templated secondary partition
int ObSplitPartDMLGeneratorV2::extract_part_info(PartInfo& part_info)
{
  int ret = OB_SUCCESS;
  if (schema_version_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner stat", K(ret), K(schema_version_));
  } else if (OB_FAIL(part_.get_source_part_ids_str(part_info.source_part_ids_str_, OB_MAX_CHAR_LENGTH))) {
    LOG_WARN("fail to get source part ids", K(ret));
  } else {
    int64_t sub_part_num = 0;
    if (PARTITION_LEVEL_TWO == new_schema_.get_part_level()) {
      sub_part_num = new_schema_.get_sub_part_option().get_part_num();
    }
    part_info.tenant_id_ = new_schema_.get_tenant_id();
    part_info.table_id_ = new_schema_.get_table_id();
    part_info.part_id_ = part_.get_part_id();
    part_info.part_idx_ = part_.get_part_idx();
    part_info.part_name_ = part_.get_part_name();
    part_info.schema_version_ = schema_version_;
    part_info.sub_part_num_ = sub_part_num;
    part_info.sub_part_space_ = 0;
    part_info.new_sub_part_num_ = 0;
    part_info.new_sub_part_space_ = 0;
    part_info.status_ = PARTITION_STATUS_INVALID;
    part_info.spare1_ = 0;
    part_info.spare2_ = 0;
    part_info.spare3_ = "";
    part_info.comment_ = "";
    part_info.mapping_pg_part_id_ = part_.get_mapping_pg_part_id();
    if (new_schema_.is_range_part()) {
      if (OB_FAIL(gen_high_bound_val_str(part_.get_high_bound_val(),
              part_info.high_bound_val_,
              part_info.b_high_bound_val_,
              part_info.tenant_id_))) {
        LOG_WARN("generate high bound val failed", K(ret));
      }
    } else if (new_schema_.is_list_part()) {
      if (OB_FAIL(gen_list_val_str(
              part_.get_list_row_values(), part_info.list_val_, part_info.b_list_val_, part_info.tenant_id_))) {
        LOG_WARN("generate list value failed", K(ret));
      }
    }
  }
  LOG_DEBUG("extrace part info", K(ret), K(part_info.source_part_ids_str_));
  return ret;
}

/////////////////////////////////////
/////////////////////////////////////
int ObUpdatePartHisInfoHelper::update_partition_info()
{
  int ret = OB_SUCCESS;
  if (arg_.split_info_.get_spp_array().count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    ;
    LOG_WARN("invalid argument", K(ret), K(arg_));
  } else {
    const uint64_t tenant_id = table_schema_.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    ObSqlString part_history_sql;
    ObSqlString value_str;
    const ObPartition* partition = NULL;
    for (int64_t i = 0; i < arg_.split_info_.get_spp_array().count() && OB_SUCC(ret); i++) {
      const ObSplitPartitionPair& pair = arg_.split_info_.get_spp_array().at(i);
      for (int64_t j = 0; j < pair.get_dest_array().count() && OB_SUCC(ret); j++) {
        // Since secondary partition split is not supported, here partition_id is equivalent to part_id,
        // which needs to be modified later
        int64_t partition_id = pair.get_dest_array().at(j).get_partition_id();
        partition = NULL;
        bool check_dropped_partition = false;
        if (OB_FAIL(table_schema_.get_partition_by_part_id(partition_id, check_dropped_partition, partition))) {
          LOG_WARN("fail to get partition", K(ret), K(partition_id));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid partition", K(ret), K(partition));
        } else {
          dml.reset();
          ObUpdatePartHisDMLGenerator part_dml_gen(*partition, schema_version_, table_schema_);
          if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
            LOG_WARN("fail to gen dml", K(ret));
          } else if (0 == i && 0 == j) {
            if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
              LOG_WARN("fail to splice insert sql", K(ret));
            }
          } else {
            value_str.reset();
            if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("fail to splice values", K(ret));
            } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      }
    }
  }
  return ret;
}
///////////////////////////////////////////
int ObUpdatePartHisInfoHelperV2::update_partition_info()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObPartition** partition_array = schema_.get_part_array();
  int64_t partition_num = schema_.get_partition_num();
  ObDMLSqlSplicer dml;
  ObSqlString part_history_sql;
  ObSqlString value_str;
  if (OB_ISNULL(partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema error", K(ret), K(schema_));
  }
  bool first_row = true;
  for (int64_t i = 0; i < partition_num && OB_SUCC(ret); i++) {
    if (OB_ISNULL(partition_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid schema", K(ret), K(i), K(schema_));
    } else if (partition_array[i]->get_source_part_ids().count() > 0 &&
               partition_array[i]->get_source_part_ids().at(0) >= 0) {
      dml.reset();
      ObUpdatePartHisDMLGenerator part_dml_gen(*partition_array[i], schema_.get_schema_version(), schema_);
      if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
        LOG_WARN("fail to gen dml", K(ret));
      } else if (first_row) {
        if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
          LOG_WARN("fail to splice insert sql", K(ret));
        } else {
          first_row = false;
        }
      } else {
        value_str.reset();
        if (OB_FAIL(dml.splice_values(value_str))) {
          LOG_WARN("fail to splice values", K(ret));
        } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
          LOG_WARN("append_fmt failed", K(value_str), K(ret));
        }
      }
    } else {
      // nothing todo
    }
  }
  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_client_.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
    }
  }
  return ret;
}
///////////////////////////////////////////
ObUpdatePartHisDMLGenerator::ObUpdatePartHisDMLGenerator(
    const ObPartition& part, const int64_t schema_version, const ObPartitionSchema& table_schema)
    : part_(part), schema_version_(schema_version), table_schema_(table_schema)
{}

int ObUpdatePartHisDMLGenerator::convert_to_dml(const PartInfo& part_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  ObString part_ids(OB_MAX_CHAR_LENGTH, part_info.source_part_ids_str_);
  const uint64_t tenant_id = part_info.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(
          dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, part_info.tenant_id_))) ||
      OB_FAIL(
          dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, part_info.table_id_))) ||
      OB_FAIL(dml.add_pk_column("part_id", part_info.part_id_)) ||
      OB_FAIL(dml.add_pk_column("part_name", part_info.part_name_)) ||
      OB_FAIL(dml.add_column("schema_version", part_info.schema_version_)) ||
      OB_FAIL(dml.add_column("sub_part_num", part_info.sub_part_num_)) ||
      OB_FAIL(dml.add_column("sub_part_space", part_info.sub_part_space_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("new_sub_part_num", part_info.new_sub_part_num_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("new_sub_part_space", part_info.new_sub_part_space_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("status", part_info.status_)) ||
      OB_FAIL(dml.add_column("spare1", part_info.spare1_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("spare2", part_info.spare2_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("spare3", part_info.spare3_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("comment", part_info.comment_ /*unused now*/)) ||
      OB_FAIL(dml.add_column("source_partition_id", part_ids)) ||
      OB_FAIL(dml.add_column("part_idx", part_info.part_idx_)) || OB_FAIL(dml.add_column("is_deleted", false)) ||
      OB_FAIL(dml.add_column("high_bound_val", ObHexEscapeSqlStr(part_info.high_bound_val_))) ||
      OB_FAIL(dml.add_column("b_high_bound_val", part_info.b_high_bound_val_)) ||
      OB_FAIL(dml.add_column("list_val", ObHexEscapeSqlStr(part_info.list_val_))) ||
      OB_FAIL(dml.add_column("b_list_val", part_info.b_list_val_)) ||
      OB_FAIL(dml.add_column("mapping_pg_part_id", part_info.mapping_pg_part_id_))) {

    LOG_WARN("dml add part info failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  }
  return ret;
}

// FIXME: Support non-templated secondary partition
int ObUpdatePartHisDMLGenerator::extract_part_info(PartInfo& part_info)
{
  int ret = OB_SUCCESS;
  int64_t sub_part_num = 0;
  if (PARTITION_LEVEL_TWO == table_schema_.get_part_level()) {
    sub_part_num = table_schema_.get_sub_part_option().get_part_num();
  }
  part_info.tenant_id_ = table_schema_.get_tenant_id();
  part_info.table_id_ = table_schema_.get_table_id();
  part_info.part_id_ = part_.get_part_id();
  part_info.part_name_ = part_.get_part_name();
  part_info.schema_version_ = schema_version_;
  part_info.sub_part_num_ = sub_part_num;
  part_info.sub_part_space_ = 0;
  part_info.new_sub_part_num_ = 0;
  part_info.new_sub_part_space_ = 0;
  part_info.status_ = PARTITION_STATUS_INVALID;
  part_info.spare1_ = 0;
  part_info.spare2_ = 0;
  part_info.spare3_ = "";
  part_info.comment_ = "";
  MEMSET(part_info.source_part_ids_str_, 0, OB_MAX_CHAR_LENGTH);
  part_info.mapping_pg_part_id_ = part_.get_mapping_pg_part_id();
  part_info.part_idx_ = part_.get_part_idx();
  if (table_schema_.is_range_part()) {
    if (OB_FAIL(gen_high_bound_val_str(part_.get_high_bound_val(),
            part_info.high_bound_val_,
            part_info.b_high_bound_val_,
            part_info.tenant_id_))) {
      LOG_WARN("generate high bound val failed", K(ret));
    }
  } else if (table_schema_.is_list_part()) {
    if (OB_FAIL(gen_list_val_str(
            part_.get_list_row_values(), part_info.list_val_, part_info.b_list_val_, part_info.tenant_id_))) {
      LOG_WARN("generate list value failed", K(ret));
    }
  }
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
