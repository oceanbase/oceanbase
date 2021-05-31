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

#include "ob_all_virtual_proxy_partition_info.h"

#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/parser/ob_parser_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/json/ob_json_print_utils.h"
#include "common/ob_smart_var.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {
ObAllVirtualProxyPartitionInfo::ObAllVirtualProxyPartitionInfo()
    : ObAllVirtualProxyBaseIterator(), next_table_idx_(0), next_part_key_idx_(0), table_schemas_()
{}

ObAllVirtualProxyPartitionInfo::~ObAllVirtualProxyPartitionInfo()
{}

int ObAllVirtualProxyPartitionInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", KP_(schema_service), K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(full_schema_guard_))) {
    SERVER_LOG(WARN, "fail to get schema guard", K(ret));
  } else {
    const int64_t ROW_KEY_COUNT = 1;
    ObRowkey start_key;
    ObRowkey end_key;
    const ObObj* start_key_obj_ptr = NULL;
    const ObObj* end_key_obj_ptr = NULL;
    uint64_t table_id = OB_INVALID_ID;
    const ObTableSchema* table_schema = NULL;

    // maybe proxy specify multiple table id
    for (int64_t i = 0; (OB_SUCC(ret)) && (i < key_ranges_.count()); ++i) {
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      start_key_obj_ptr = start_key.get_obj_ptr();
      end_key_obj_ptr = end_key.get_obj_ptr();

      if (ROW_KEY_COUNT != start_key.get_obj_cnt() || ROW_KEY_COUNT != end_key.get_obj_cnt() ||
          OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr) || !start_key_obj_ptr[0].is_int() ||
          !end_key_obj_ptr[0].is_int() || start_key_obj_ptr[0] != end_key_obj_ptr[0]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "table_id must be specified");
      } else {
        table_id = static_cast<uint64_t>(start_key_obj_ptr[0].get_int());
        if (OB_FAIL(get_table_schema(full_schema_guard_, table_id, table_schema))) {
          SERVER_LOG(WARN, "fail to get table schema", K(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          // skip
        } else if (OB_FAIL(table_schemas_.push_back(table_schema))) {
          SERVER_LOG(WARN, "fail to push back table_schema", K(table_schema), K(ret));
        }
      }
    }  // end of for
  }
  return ret;
}

int ObAllVirtualProxyPartitionInfo::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", KP_(schema_service), K(ret));
  } else if (next_table_idx_ >= table_schemas_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(table_schema = table_schemas_.at(next_table_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table schema is NULL", K_(next_table_idx), K(ret));
  } else if (!table_schema->is_partitioned_table()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fill_cells(*table_schema))) {
    SERVER_LOG(WARN, "fail to fill cells ", KPC(table_schema), K_(next_table_idx), K(ret));
  } else {
    ++next_part_key_idx_;
    if (next_part_key_idx_ >=
        table_schema->get_partition_key_column_num() + table_schema->get_subpartition_key_column_num()) {
      next_part_key_idx_ = 0;
      ++next_table_idx_;
    }
  }
  return ret;
}

int ObAllVirtualProxyPartitionInfo::fill_cells(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  // part option
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  int64_t all_part_num = table_schema.get_all_part_num();
  const ObPartitionOption& part_option = table_schema.get_part_option();
  const ObPartitionOption& sub_part_option = table_schema.get_sub_part_option();

  // part key column
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2* column_schema = NULL;
  int64_t first_part_key_num = table_schema.get_partition_key_column_num();
  int64_t sub_first_part_key_num = table_schema.get_subpartition_key_column_num();
  int64_t all_part_key_num = first_part_key_num + sub_first_part_key_num;
  const ObPartitionKeyInfo* partition_key_info = &table_schema.get_partition_key_info();
  int64_t part_key_idx = next_part_key_idx_;
  int64_t part_level = static_cast<int64_t>(PARTITION_LEVEL_ONE);
  if (part_key_idx >= first_part_key_num) {
    part_key_idx -= first_part_key_num;  // sub part key
    partition_key_info = &table_schema.get_subpartition_key_info();
    part_level = static_cast<int64_t>(PARTITION_LEVEL_TWO);
  }

  // get part key column
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(partition_key_info->get_column_id(part_key_idx, column_id))) {
    SERVER_LOG(WARN, "fail to get column id", K(part_key_idx), K(column_id), K(ret));
  } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
    ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(WARN, "fail to get column schema", K_(next_part_key_idx), K(column_id), K(ret));
  } else {
    // do nothing
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TABLE_ID:
        cells[i].set_int(static_cast<int64_t>(table_schema.get_table_id()));
        break;
      case TENANT_ID:
        cells[i].set_int(static_cast<int64_t>(table_schema.get_tenant_id()));
        break;

      case PART_LEVEL:
        cells[i].set_int(static_cast<int64_t>(table_schema.get_part_level()));
        break;
      case ALL_PART_NUM:
        cells[i].set_int(all_part_num);
        break;
      case TEMPLATE_NUM:  // The non-templated secondary partition table is 0, and the rest is 1
        cells[i].set_int(table_schema.is_sub_part_template() ? 1 : 0);
        break;
      case PART_ID_RULE_VER:
        cells[i].set_int(0);
        break;

      case PART_TYPE:
        cells[i].set_int(static_cast<int64_t>(part_option.get_part_func_type()));
        break;
      case PART_NUM:
        cells[i].set_int(static_cast<int64_t>(part_option.get_part_num()));
        break;
      case IS_COLUMN_TYPE:
        cells[i].set_bool(part_option.is_columns());
        break;
      case PART_SPACE:
        cells[i].set_int(part_option.get_part_space());
        break;
      case PART_EXPR:
        cells[i].set_varchar(part_option.get_part_func_expr_str());
        cells[i].set_collation_type(coll_type);
        break;
      case PART_EXPR_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case PART_RANGE_TYPE:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        if (table_schema.get_partition_num() > 0 && NULL != table_schema.get_part_array() &&
            NULL != table_schema.get_part_array()[0]) {
          if (OB_FAIL(get_rowkey_type_str(table_schema.get_part_array()[0]->get_high_bound_val(), cells[i]))) {
            SERVER_LOG(WARN, "fail to get rowkey type str", K(ret));
          } else {
            // do nothing here
          }
        }
        break;
      case PART_INTERVAL:
        cells[i].set_varchar(part_option.get_part_intervel_str());
        cells[i].set_collation_type(coll_type);
        break;
      case PART_INTERVAL_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case INTERVAL_START:
        cells[i].set_varchar(part_option.get_intervel_start_str());
        cells[i].set_collation_type(coll_type);
        break;
      case INTERVAL_START_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SUB_PART_TYPE:
        cells[i].set_int(static_cast<int64_t>(sub_part_option.get_part_func_type()));
        break;
      case SUB_PART_NUM: {
        int64_t sub_part_num = OB_INVALID_ID;
        if (table_schema.is_sub_part_template()) {
          sub_part_num = sub_part_option.get_part_num();
        } else {
          // for Non-templated secondary partition table, this value is meaningless.
          // but in order to avoid the proxy core when the old proxy and new ob, fill the sub_part_num of the first
          // partition here
          if (table_schema.get_partition_num() <= 0 || OB_ISNULL(table_schema.get_part_array()[0])) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "partition_num is 0 or partition is null", KR(ret), K(table_schema));
          } else {
            sub_part_num = table_schema.get_part_array()[0]->get_sub_part_num();
          }
        }
        if (OB_SUCC(ret)) {
          cells[i].set_int(sub_part_num);
        }
        break;
      }
      case IS_SUB_COLUMN_TYPE:
        cells[i].set_bool(sub_part_option.is_columns());
        break;
      case SUB_PART_SPACE:
        cells[i].set_int(sub_part_option.get_part_space());
        break;
      case SUB_PART_EXPR:
        cells[i].set_varchar(sub_part_option.get_part_func_expr_str());
        cells[i].set_collation_type(coll_type);
        break;
      case SUB_PART_EXPR_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SUB_PART_RANGE_TYPE:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        {
          ObSubPartition* subpartition = NULL;
          if (PARTITION_LEVEL_TWO != table_schema.get_part_level()) {
            // skip
          } else if (table_schema.is_sub_part_template()) {
            if (table_schema.get_def_subpartition_num() > 0 && OB_NOT_NULL(table_schema.get_def_subpart_array()) &&
                OB_NOT_NULL(table_schema.get_def_subpart_array()[0])) {
              subpartition = table_schema.get_def_subpart_array()[0];
            }
          } else {
            if (table_schema.get_partition_num() > 0 && OB_NOT_NULL(table_schema.get_part_array()) &&
                OB_NOT_NULL(table_schema.get_part_array()[0])) {
              ObPartition* partition = table_schema.get_part_array()[0];
              if (OB_NOT_NULL(partition) && partition->get_subpartition_num() > 0 &&
                  OB_NOT_NULL(partition->get_subpart_array()) && OB_NOT_NULL(partition->get_subpart_array()[0])) {
                subpartition = partition->get_subpart_array()[0];
              }
            }
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(subpartition) &&
              OB_FAIL(get_rowkey_type_str(subpartition->get_high_bound_val(), cells[i]))) {
            SERVER_LOG(WARN, "fail to get rowkey type str", K(ret));
          } else {
            // do nothing here
          }
        }
        break;
      case DEF_SUB_PART_INTERVAL:
        cells[i].set_varchar(sub_part_option.get_part_intervel_str());
        cells[i].set_collation_type(coll_type);
        break;
      case DEF_SUB_PART_INTERVAL_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case DEF_SUB_INTERVAL_START:
        cells[i].set_varchar(sub_part_option.get_intervel_start_str());
        cells[i].set_collation_type(coll_type);
        break;
      case DEF_SUB_INTERVAL_START_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;

      case PART_KEY_NUM:
        cells[i].set_int(all_part_key_num);
        break;
      case PART_KEY_NAME:
        cells[i].set_varchar(column_schema->get_column_name());
        cells[i].set_collation_type(coll_type);
        break;
      case PART_KEY_TYPE:
        cells[i].set_int(static_cast<int64_t>(column_schema->get_data_type()));
        break;
      case PART_KEY_IDX:
        cells[i].set_int(table_schema.get_column_idx(column_id, true));
        break;
      case PART_KEY_LEVEL:
        cells[i].set_int(part_level);
        break;
      case PART_KEY_EXTRA:
        if (column_schema->is_generated_column()) {
          if (!column_schema->get_orig_default_value().is_string_type()) {
            SERVER_LOG(
                WARN, "default value should be string type ", K(i), K(column_schema->get_orig_default_value()), K(ret));
            cells[i].set_varchar("");
          } else {
            cells[i].set_varchar(column_schema->get_orig_default_value().get_string());
          }
        } else {
          cells[i].set_varchar("");
        }
        cells[i].set_collation_type(coll_type);
        break;

      case SPARE1:
        // spare1 use as collation type
        cells[i].set_int(static_cast<int64_t>(column_schema->get_collation_type()));
        break;
      case SPARE2: {
        int64_t idx = -1;
        const ObRowkeyInfo& info = table_schema.get_rowkey_info();
        if (OB_FAIL(info.get_index(column_id, idx))) {
          // The generated column of the primary key table can also be used as the partition key,
          // not necessarily in the rowkey info
          if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret && !table_schema.is_no_pk_table() &&
                        column_schema->is_generated_column())) {
            idx = info.get_size() + next_part_key_idx_;
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "part key column must be pk", K(ret), K(column_id), K(info));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(0 > idx)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected index", K(ret), K(idx), K(column_id));
          } else {
            cells[i].set_int(idx);
          }
        }
        break;
      }
      case SPARE3:
        cells[i].set_int(0);
        break;
      case SPARE4: {
        ObString proxy_check_partition_str;
        if (OB_FAIL(gen_proxy_part_pruning_str(table_schema, column_schema, proxy_check_partition_str))) {
          SERVER_LOG(WARN, "fail to gen proxy part pruning str", K(ret));
        } else {
          cells[i].set_varchar(proxy_check_partition_str);
          cells[i].set_collation_type(coll_type);
        }
      } break;
      case SPARE5:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SPARE6:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(i), K(ret));
        break;
    }
  }
  return ret;
}

// In order to support the use of expressions with partition key constraints for partition reduction calculations.
// The support schemes are as follows:
// add an expression with the equivalent constraint of the partition key in the spare4 field in the
// __all_virtual_proxy_partition_info table. If there are multiple such constraints, separate them with semicolons;
//
// PS: part_key_extra field, still record the expression that the partition key is the generated column (existing now)
//
// When the proxy is partitioned, it will first analyze the expressions of the columns generated in part_key_extra.
// If not, then look at each expression in the spare4 field after each segment is divided by a semicolon.
// Whether the formula can be used for the partition reduction of the filter condition.
//
// such as:
// create table t1 (c1 varchar(20 char) primary key, c2 varchar(20 char), c3 varchar(20 char),
// partition_id varchar2(2 char) GENERATED ALWAYS AS (substr(c1,19,2)) VIRTUAL,
// CONSTRAINT cst_pmt_apply_fd_dtl_00 CHECK ((partition_id = substr(c2,19,2)),
// CONSTRAINT cst_pmt_apply_fd_dtl_01 CHECK ((partition_id = substr(c3,19,2))))
// partition by list(partition_id)
// (partition p0 values('00'), partition p1 values('11'));
//
// Previously, the part_key_extra field would be added: substr("C1",19,2); spare4 field is empty
// Now the part_key_extra field will be added: substr("C1",19,2); spare4 field will be added: substr("C2",19,2);
// substr("C3",19,2)
int ObAllVirtualProxyPartitionInfo::gen_proxy_part_pruning_str(const share::schema::ObTableSchema& table_schema,
    const ObColumnSchemaV2* column_schema, ObString& proxy_check_partition_str)
{
  int ret = OB_SUCCESS;
  ObWorker::CompatMode tenant_mode = ObWorker::CompatMode::MYSQL;
  if (OB_ISNULL(allocator_) || OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(allocator_), K(column_schema), K(ret));
  } else if (OB_FAIL(full_schema_guard_.get_tenant_compat_mode(table_schema.get_tenant_id(), tenant_mode))) {
    SERVER_LOG(WARN, "fail to get tenant compat mode", K(table_schema.get_tenant_id()), K(ret));
  } else {
    SMART_VAR(char[OB_MAX_SQL_LENGTH], expr_str_buf)
    {
      MEMSET(expr_str_buf, 0, OB_MAX_SQL_LENGTH);
      int64_t pos = 0;
      {
        // need to switch to the mode of the tenant where the table is located
        // Because the constraint check expr str is in different modes, the text string is stored in different ways,
        // When parsing and printing, need to use the interface of the corresponding mode.
        // For example, in MySQL mode: substr(`c1`,19,2), in Oracle mode: substr("C1",19,2)
        CompatModeGuard tmp_mode(tenant_mode);
        for (ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
             OB_SUCC(ret) && iter != table_schema.constraint_end();
             ++iter) {
          sql::ObRawExpr* check_expr = NULL;
          if (OB_ISNULL(iter) || (OB_ISNULL(*iter))) {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "invalid argument", K(ret), K(iter));
          } else if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
            continue;
          } else if (OB_FAIL(build_check_str_to_raw_expr((*iter)->get_check_expr_str(), table_schema, check_expr))) {
            SERVER_LOG(WARN, "build check expr failed", K(ret), K(check_expr));
          } else if (OB_ISNULL(check_expr)) {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "build check expr failed", K(ret), K(check_expr));
          } else if (T_OP_EQ == check_expr->get_expr_type()) {
            sql::ObOpRawExpr* op_check_expr = static_cast<sql::ObOpRawExpr*>(check_expr);
            if (2 != op_check_expr->get_param_count() || OB_ISNULL(op_check_expr->get_param_expr(0)) ||
                OB_ISNULL(op_check_expr->get_param_expr(1))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid param count", K(op_check_expr->get_param_count()));
            } else {
              // For constraint check: left_expr = right_expr,
              // If two children have a child column_ref and the same as the part key,
              // Then convert another child into an expression text string
              ObString part_col_name = column_schema->get_column_name_str();
              sql::ObRawExpr* left = op_check_expr->get_param_expr(0);
              sql::ObRawExpr* right = op_check_expr->get_param_expr(1);
              sql::ObColumnRefRawExpr* col_ref_raw_expr = NULL;
              sql::ObRawExpr* part_pruning_expr = NULL;
              if (left->is_column_ref_expr()) {
                col_ref_raw_expr = static_cast<sql::ObColumnRefRawExpr*>(left);
                if (part_col_name == col_ref_raw_expr->get_column_name()) {
                  part_pruning_expr = right;
                }
              } else if (right->is_column_ref_expr()) {
                col_ref_raw_expr = static_cast<sql::ObColumnRefRawExpr*>(right);
                if (part_col_name == col_ref_raw_expr->get_column_name()) {
                  part_pruning_expr = left;
                }
              }
              if (OB_NOT_NULL(part_pruning_expr)) {
                int64_t tmp_pos = 0;
                sql::ObRawExprPrinter expr_printer(
                    expr_str_buf + pos, OB_MAX_SQL_LENGTH - pos, &tmp_pos, session_->get_timezone_info());
                if (OB_FAIL(expr_printer.do_print(part_pruning_expr, sql::T_NONE_SCOPE, true))) {
                  SERVER_LOG(WARN, "print expr definition failed", K(ret));
                } else {
                  pos += tmp_pos;
                  if (pos < OB_MAX_SQL_LENGTH) {
                    expr_str_buf[pos] = ';';
                    pos++;
                  } else {
                    // do nothing
                  }
                }
              }
            }
          }
        }  // for end;
        if (OB_SUCC(ret)) {
          // Remove the tail';'
          if (pos - 1 > 0 && pos - 1 < OB_MAX_SQL_LENGTH && ';' == expr_str_buf[pos - 1]) {
            pos -= 1;
          }
          if (OB_FAIL(ob_write_string(*allocator_, ObString(pos, expr_str_buf), proxy_check_partition_str))) {
            SERVER_LOG(WARN, "fail to copy check string", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObAllVirtualProxyPartitionInfo::build_check_str_to_raw_expr(
    const ObString& check_expr_str, const share::schema::ObTableSchema& table_schema, sql::ObRawExpr*& check_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(allocator_), K(ret));
  } else {
    sql::ObRawExprFactory expr_factory(*allocator_);
    const ParseNode* expr_node = NULL;
    ObArray<sql::ObQualifiedName> columns;
    ObArray<std::pair<sql::ObRawExpr*, sql::ObRawExpr*>> ref_sys_exprs;
    if (OB_FAIL(sql::ObRawExprUtils::parse_bool_expr_node_from_str(check_expr_str, *allocator_, expr_node))) {
      SERVER_LOG(WARN, "parse expr node from str failed", K(ret), K(check_expr_str));
    } else if (OB_ISNULL(expr_node)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid argument", K(session_), K(ret));
    } else if (OB_FAIL(sql::ObRawExprUtils::build_check_constraint_expr(
                   expr_factory, *session_, *expr_node, check_expr, columns))) {
      SERVER_LOG(WARN, "build constraint expr failed", K(ret));
    } else if (OB_ISNULL(check_expr)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "build constraint expr failed", K(ret), K(check_expr));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const sql::ObQualifiedName& q_name = columns.at(i);
      const ObColumnSchemaV2* col_schema = NULL;
      if (q_name.is_sys_func()) {
        sql::ObRawExpr* sys_func = q_name.access_idents_.at(0).sys_func_expr_;
        if (OB_ISNULL(sys_func)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "sys_func is null", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_sys_exprs.count(); ++i) {
          if (OB_FAIL(sql::ObRawExprUtils::replace_ref_column(
                  sys_func, ref_sys_exprs.at(i).first, ref_sys_exprs.at(i).second))) {
            SERVER_LOG(WARN, "replace ref_column failed", K(ret), KPC(sys_func));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(q_name.access_idents_.at(0).sys_func_expr_->check_param_num())) {
          SERVER_LOG(WARN, "check param_num failed", K(ret));
        } else if (OB_FAIL(sql::ObRawExprUtils::replace_ref_column(check_expr, q_name.ref_expr_, sys_func))) {
          SERVER_LOG(WARN, "replace ref_column failed", K(ret));
        } else if (OB_FAIL(ref_sys_exprs.push_back(
                       std::pair<sql::ObRawExpr*, sql::ObRawExpr*>(q_name.ref_expr_, sys_func)))) {
          SERVER_LOG(WARN, "push_back to ref_sys_exprs failed", K(ret));
        }
      } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0 || OB_ISNULL(q_name.ref_expr_)) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid check expr", K(ret), K(check_expr_str));
      } else if (NULL == (col_schema = table_schema.get_column_schema(q_name.col_name_)) || col_schema->is_hidden()) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        ObString scope_name = "constraint column function";
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
            q_name.col_name_.length(),
            q_name.col_name_.ptr(),
            scope_name.length(),
            scope_name.ptr());
      } else if (OB_FAIL(sql::ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
        SERVER_LOG(WARN, "init column expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      SERVER_LOG(DEBUG, "constraint check expr", K(check_expr_str));
      SERVER_LOG(DEBUG, "constraint check expr parse_node: ", K(sql::ObParserResultPrintWrapper(*expr_node)));
      SERVER_LOG(DEBUG, "constraint check exp raw_expr: ", K(*check_expr));
    }
  }

  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
