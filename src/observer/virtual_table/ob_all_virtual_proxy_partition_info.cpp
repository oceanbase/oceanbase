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

#define USING_LOG_PREFIX SERVER

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

namespace oceanbase
{
namespace observer
{
ObAllVirtualProxyPartitionInfo::ObAllVirtualProxyPartitionInfo()
    : ObAllVirtualProxyBaseIterator(),
      next_table_idx_(0),
      next_part_key_idx_(0),
      table_schemas_()
{
}

ObAllVirtualProxyPartitionInfo::~ObAllVirtualProxyPartitionInfo()
{
}

int ObAllVirtualProxyPartitionInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", KP_(schema_service), KR(ret));
  } else {
    const int64_t ROW_KEY_COUNT = 2;
    ObRowkey start_key;
    ObRowkey end_key;
    const ObObj *start_key_obj_ptr = NULL;
    const ObObj *end_key_obj_ptr = NULL;
    ObString tenant_name;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    uint64_t table_id = OB_INVALID_ID;
    const ObTenantSchema *tenant_schema = NULL;
    const ObTableSchema *table_schema = NULL;
    ObSEArray<uint64_t, 1> input_table_ids;

    // maybe proxy specify multiple tenant_name and table_id
    ARRAY_FOREACH_N(key_ranges_, i, cnt) {
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      start_key_obj_ptr = start_key.get_obj_ptr();
      end_key_obj_ptr = end_key.get_obj_ptr();
      if (OB_UNLIKELY((ROW_KEY_COUNT != start_key.get_obj_cnt()) 
          || (ROW_KEY_COUNT != end_key.get_obj_cnt()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "row key count not match");
        LOG_WARN("row key count not match", KR(ret), K(ROW_KEY_COUNT),
            "start key count", start_key.get_obj_cnt(), "end key count", end_key.get_obj_cnt());
      } else if (OB_UNLIKELY(!start_key_obj_ptr[0].is_varchar_or_char()
          || !end_key_obj_ptr[0].is_varchar_or_char()
          || (start_key_obj_ptr[0] != end_key_obj_ptr[0]))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name and table_id (must all be specified)");
        LOG_WARN("invalid tenant_name", KR(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (OB_UNLIKELY(!start_key_obj_ptr[1].is_int()
          || !end_key_obj_ptr[1].is_int()
          || start_key_obj_ptr[1] != end_key_obj_ptr[1])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name and table_id (must all be specified)");
        LOG_WARN("invalid table_id", KR(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else {
        tenant_name = end_key_obj_ptr[0].get_string();
        table_id = static_cast<uint64_t>(end_key_obj_ptr[1].get_int());
        // check tenant_name in batch query
        if (!input_tenant_name_.empty()
            && OB_UNLIKELY(0 != input_tenant_name_.case_compare(tenant_name))) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "different tenant_names in a batch query");
          LOG_WARN("unexpected different tenant_names", KR(ret), K(tenant_name));
        } else if (OB_FAIL(input_table_ids.push_back(table_id))) {
          LOG_WARN("fail to push back", KR(ret), K(tenant_name), K(table_id));
        } else {
          input_tenant_name_ = tenant_name;
        }
      }
    } // end of for key_ranges_

    if (OB_FAIL(ret) || input_table_ids.empty()) {
      // skip
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
        OB_SYS_TENANT_ID, tenant_schema_guard_))) {
      LOG_WARN("fail to get sys tenant schema guard", KR(ret), K(OB_SYS_TENANT_ID));
    } else if (OB_FAIL(tenant_schema_guard_.get_tenant_info(input_tenant_name_, tenant_schema))) {
      LOG_WARN("fail to get tenant info", KR(ret), K_(input_tenant_name));
    } else if (OB_ISNULL(tenant_schema)) {
      LOG_TRACE("tenant not exist", K_(input_tenant_name)); // skip
    } else {
      tenant_id = tenant_schema->get_tenant_id();
      if (OB_UNLIKELY(!is_valid_tenant_id(effective_tenant_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid effective_tenant_id", KR(ret), K_(effective_tenant_id));
      } else if (!is_sys_tenant(effective_tenant_id_) && (tenant_id != effective_tenant_id_)) {
        LOG_TRACE("unprivileged tenant", K(tenant_id), K_(effective_tenant_id)); // skip
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, tenant_schema_guard_))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else {
        ARRAY_FOREACH_N(input_table_ids, idx, cnt) {
          uint64_t current_table_id = input_table_ids.at(idx);
          if (OB_FAIL(tenant_schema_guard_.get_table_schema(
                      tenant_id, current_table_id, table_schema))) {
            LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(current_table_id));
          } else if (OB_ISNULL(table_schema)) {
            if (OB_FAIL(check_schema_version(tenant_schema_guard_, tenant_id))) {
              LOG_WARN("fail to check schema version", KR(ret), K(tenant_id));
            } else {
              LOG_TRACE("table not exist", K(tenant_id), K(current_table_id)); // skip
            }
          } else if (!table_schema->is_partitioned_table()) {
            LOG_TRACE("is not partitioned table", K(current_table_id)); // skip
          } else if (OB_FAIL(table_schemas_.push_back(table_schema))) {
            LOG_WARN("fail to push back table_schema", K(table_schema), KR(ret));
          }
        } // end for input_table_ids
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyPartitionInfo::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", KR(ret), KP_(schema_service));
  } else if (next_table_idx_ >= table_schemas_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(table_schema = table_schemas_.at(next_table_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", KR(ret), K_(next_table_idx));
  } else if (OB_FAIL(fill_row_(*table_schema))) {
    LOG_WARN("fail to fill cells ", KR(ret), KPC(table_schema), K_(next_table_idx));
  } else {
    ++next_part_key_idx_;
    if (next_part_key_idx_ >= table_schema->get_partition_key_column_num()
                              + table_schema->get_subpartition_key_column_num()) {
      next_part_key_idx_ = 0;
      ++next_table_idx_;
    }
  }
  return ret;
}

int ObAllVirtualProxyPartitionInfo::fill_row_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  // part option
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t all_part_num = table_schema.get_all_part_num();
  const ObPartitionOption &part_option = table_schema.get_part_option();
  const ObPartitionOption &sub_part_option = table_schema.get_sub_part_option();

  // part key column
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *column_schema = NULL;
  int64_t first_part_key_num = table_schema.get_partition_key_column_num();
  int64_t sub_first_part_key_num = table_schema.get_subpartition_key_column_num();
  int64_t all_part_key_num = first_part_key_num + sub_first_part_key_num;
  const ObPartitionKeyInfo *partition_key_info = &table_schema.get_partition_key_info();
  int64_t part_key_idx = next_part_key_idx_;
  int64_t part_level = static_cast<int64_t>(PARTITION_LEVEL_ONE);
  ObAccuracy accuracy;
  if (part_key_idx >= first_part_key_num) {
    part_key_idx -= first_part_key_num; // sub part key
    partition_key_info = &table_schema.get_subpartition_key_info();
    part_level = static_cast<int64_t>(PARTITION_LEVEL_TWO);
  }

  // get part key column
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", KR(ret));
  } else if (OB_FAIL(partition_key_info->get_column_id(part_key_idx, column_id))) {
    LOG_WARN("fail to get column id", KR(ret), K(part_key_idx), K(column_id));
  } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get column schema", KR(ret), K_(next_part_key_idx), K(column_id));
  } else {
    accuracy = column_schema->get_accuracy();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
    case TENANT_NAME: {
        cells[i].set_varchar(input_tenant_name_);
        cells[i].set_collation_type(coll_type);
        break;
      }
    case TABLE_ID: {
        cells[i].set_int(static_cast<int64_t>(table_schema.get_table_id()));
        break;
      }
    case PART_LEVEL: {
        cells[i].set_int(static_cast<int64_t>(table_schema.get_part_level()));
        break;
      }
    case ALL_PART_NUM: {
        cells[i].set_int(all_part_num);
        break;
      }
    case TEMPLATE_NUM: {// set 0 means non-templated secondary partition for proxy
        cells[i].set_int(0);
        break;
      }
    case PART_ID_RULE_VER: {
        cells[i].set_int(0);
        break;
      }
    case PART_TYPE: {
        cells[i].set_int(static_cast<int64_t>(part_option.get_part_func_type()));
        break;
      }
    case PART_NUM: {
        cells[i].set_int(static_cast<int64_t>(part_option.get_part_num()));
        break;
      }
    case IS_COLUMN_TYPE: {
        cells[i].set_bool(false); // not used
        break;
      }
    case PART_SPACE: {
        cells[i].set_int(0); // not used
        break;
      }
    case PART_EXPR: {
        cells[i].set_varchar(part_option.get_part_func_expr_str());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_EXPR_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_RANGE_TYPE: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        if (table_schema.get_partition_num() > 0
            && NULL != table_schema.get_part_array()
            && NULL != table_schema.get_part_array()[0]) {
          if (OB_FAIL(get_rowkey_type_str(
              table_schema.get_part_array()[0]->get_high_bound_val(),
              cells[i]))) {
            LOG_WARN("fail to get rowkey type str", KR(ret));
          } else {
            // do nothing here
          }
        }
        break;
      }
    case PART_INTERVAL: {
        cells[i].set_varchar(part_option.get_part_intervel_str());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_INTERVAL_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case INTERVAL_START: {
        cells[i].set_varchar(part_option.get_intervel_start_str());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case INTERVAL_START_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SUB_PART_TYPE: {
        cells[i].set_int(static_cast<int64_t>(sub_part_option.get_part_func_type()));
        break;
      }
    case SUB_PART_NUM: { // meaningless for 4.0
        cells[i].set_int(0);
        break;
      }
    case IS_SUB_COLUMN_TYPE: {
        cells[i].set_bool(false); // not used
        break;
      }
    case SUB_PART_SPACE: {
        cells[i].set_int(0); // not used
        break;
      }
    case SUB_PART_EXPR: {
        cells[i].set_varchar(sub_part_option.get_part_func_expr_str());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SUB_PART_EXPR_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SUB_PART_RANGE_TYPE: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        if (PARTITION_LEVEL_TWO != table_schema.get_part_level()) {
          // skip
        } else {
          ObSubPartition *subpartition = NULL;
          if (table_schema.get_partition_num() > 0
              && OB_NOT_NULL(table_schema.get_part_array())
              && OB_NOT_NULL(table_schema.get_part_array()[0])) {
            ObPartition *partition = table_schema.get_part_array()[0];
            if (OB_NOT_NULL(partition)
                && partition->get_subpartition_num() > 0
                && OB_NOT_NULL(partition->get_subpart_array())
                && OB_NOT_NULL(partition->get_subpart_array()[0])) {
              subpartition = partition->get_subpart_array()[0];
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(subpartition)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("subpartition not found", KR(ret), K(table_schema));
            } else if (OB_FAIL(get_rowkey_type_str(subpartition->get_high_bound_val(), cells[i]))) {
              LOG_WARN("fail to get rowkey type str", KR(ret), K(table_schema));
            }
          }
        }
        break;
      }
    case DEF_SUB_PART_INTERVAL: {
        cells[i].set_varchar(sub_part_option.get_part_intervel_str());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case DEF_SUB_PART_INTERVAL_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case DEF_SUB_INTERVAL_START: {
        cells[i].set_varchar(sub_part_option.get_intervel_start_str());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case DEF_SUB_INTERVAL_START_BIN: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_KEY_NUM: {
        cells[i].set_int(all_part_key_num);
        break;
      }
    case PART_KEY_NAME: {
        cells[i].set_varchar(column_schema->get_column_name());
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_KEY_TYPE: {
        cells[i].set_int(static_cast<int64_t>(column_schema->get_data_type()));
        break;
      }
    case PART_KEY_IDX: {
        cells[i].set_int(table_schema.get_column_idx(column_id, true));
        break;
      }
    case PART_KEY_LEVEL: {
        cells[i].set_int(part_level);
        break;
      }
    case PART_KEY_EXTRA: {
        if (column_schema->is_generated_column()) {
          if (!column_schema->get_orig_default_value().is_string_type()) {
            LOG_WARN("default value should be string type ", KR(ret),
                K(i), K(column_schema->get_orig_default_value()));
            cells[i].set_varchar("");
          } else {
            cells[i].set_varchar(column_schema->get_orig_default_value().get_string());
          }
        } else {
          cells[i].set_varchar("");
        }
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_KEY_COLLATION_TYPE: {
        cells[i].set_int(static_cast<int64_t>(column_schema->get_collation_type()));
        break;
      }
    case PART_KEY_ROWKEY_IDX: {
        int64_t idx = -1;
        const ObRowkeyInfo &info = table_schema.get_rowkey_info();
        if (OB_FAIL(info.get_index(column_id, idx))) {
          // In the following scenarios, the partition key does not need to be included in the primary key
          // 1. The generated column of the primary key table can also be used as the partition key
          // 2. Virtual table
          // 3. No primary key table (heap table)
          if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
            if (!table_schema.is_heap_table() && column_schema->is_generated_column()) {
              idx = info.get_size() + next_part_key_idx_;
              ret = OB_SUCCESS;
            } else if (table_schema.is_vir_table() || table_schema.is_heap_table()) {
              idx = -1;
              ret = OB_SUCCESS;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("part key column must be pk", KR(ret), K(column_id), K(info));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get index from ObRowkeyInfo", KR(ret), K(column_id), K(info));
          }
        }
        if (OB_SUCC(ret)) {
          cells[i].set_int(idx);
        }
        break;
      }
    case PART_KEY_EXPR: {
        ObString proxy_check_partition_str;
        if (OB_FAIL(gen_proxy_part_pruning_str_(
            table_schema,
            column_schema,
            proxy_check_partition_str))) {
          LOG_WARN("fail to gen proxy part pruning str", KR(ret));
        } else {
          cells[i].set_varchar(proxy_check_partition_str);
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
    case PART_KEY_LENGTH: {
        cells[i].set_int(static_cast<int64_t>(accuracy.get_length()));
        break;
      }
    case PART_KEY_PRECISION: {
        cells[i].set_int(static_cast<int64_t>(accuracy.get_precision()));
        break;
      }
    case PART_KEY_SCALE: {
        cells[i].set_int(static_cast<int64_t>(accuracy.get_scale()));
        break;
      }
    case SPARE1: {// int, unused
        cells[i].set_int(0);
        break;
      }
    case SPARE2: {// int, unused
        cells[i].set_int(0);
        break;
      }
    case SPARE3: {// int, unused
        cells[i].set_int(0);
        break;
      }
    case SPARE4: {// varchar, unused
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SPARE5: {// varchar, unused
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case SPARE6: {// varchar, unused
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
    case PART_KEY_DEFAULT_VALUE: {
        if (column_schema->is_generated_column()
            || column_schema->is_identity_column()
            || column_schema->get_cur_default_value().is_null()) {
          cells[i].set_null();
        } else {
          char *buf = NULL;
          const ObObj &obj = column_schema->get_cur_default_value();
          const int64_t buf_len = obj.get_serialize_size();
          int64_t pos = 0;
          if (OB_ISNULL(allocator_)) {
            ret = OB_NOT_INIT;
            LOG_WARN("data member doesn't init", KR(ret));
          } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc tmp_buf fail", KR(ret));
          } else if (OB_FAIL(obj.serialize(buf, buf_len, pos))) {
            LOG_WARN("fail to serialize", KR(ret));
          } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
          } else {
            cur_row_.cells_[i].set_lob_value(ObLongTextType, buf, static_cast<int32_t>(pos));
          }
        }
        cells[i].set_collation_type(coll_type);
        break;
      }
    default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column id", K(i), KR(ret));
        break;
      }
    } // end switch
  }
  return ret;
}

// In order to support the use of expressions with partition key constraints for partition reduction
// calculations. The support schemes are as follows:
// The server will add an expression with one side as the equivalent constraint of the partition
// key in the spare4 field in the __all_virtual_proxy_partition_info table.If there are multiple
// such constraints, separate them with semicolons;
// part_key_extra field, still record the expression that the partition key is the generated
// column (existing now)
//
// When the proxy is partitioned, it will first analyze the expressions of the columns generated
// in part_key_extra to see if the filter conditions in the query can be partitioned and cut in
// this way. If not, then look at each expression in the spare4 field after it is split with a
// semicolon. Whether the formula can be used for the partition reduction of the filter condition.
//
// For example:
// create table t1 (c1 varchar(20 char) primary key, c2 varchar(20 char), c3 varchar(20 char),
// partition_id varchar2(2 char) GENERATED ALWAYS AS (substr(c1,19,2)) VIRTUAL,
// CONSTRAINT cst_pmt_apply_fd_dtl_00 CHECK ((partition_id = substr(c2,19,2)),
// CONSTRAINT cst_pmt_apply_fd_dtl_01 CHECK ((partition_id = substr(c3,19,2))))
// partition by list(partition_id)
// (partition p0 values('00'), partition p1 values('11'));
//
// Previously, the part_key_extra field would be added:
// substr("C1",19,2);
// spare4 field is empty
//
// Now the part_key_extra field will be added:
// substr("C1",19,2);
// spare4 field will be added:
// substr("C2",19,2); substr("C3",19,2)
int ObAllVirtualProxyPartitionInfo::gen_proxy_part_pruning_str_(
    const share::schema::ObTableSchema &table_schema,
    const ObColumnSchemaV2 *column_schema,
    ObString &proxy_check_partition_str)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(allocator_) || OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(allocator_), K(column_schema), KR(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to get compat mode", KR(ret), K(table_schema));
  } else {
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf) {
      MEMSET(expr_str_buf, 0, OB_MAX_DEFAULT_VALUE_LENGTH);
      int64_t pos = 0;
      {
        // Here switch to the mode of the tenant where the table is located, and cannot directly use the current system tenant MySQL mode for processing.
        // Because the constraint check expr str is in different modes, the text string is stored in different ways,
        // so use the interface of the corresponding mode when parsing and printing.
        // For example, the text string in MySQL mode: substr(`c1`,19,2), the text string in Oracle mode is: substr("C1",19,2)
        lib::CompatModeGuard tmp_mode(is_oracle_mode ?
                                 lib::Worker::CompatMode::ORACLE :
                                 lib::Worker::CompatMode::MYSQL);
        for (ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
             OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
          sql::ObRawExpr *check_expr = NULL;
          if (OB_ISNULL(iter) || (OB_ISNULL(*iter))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", KR(ret), K(iter));
          } else if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
            continue;
          } else if (OB_FAIL(build_check_str_to_raw_expr_(
              (*iter)->get_check_expr_str(),
              table_schema,
              check_expr))) {
            LOG_WARN("build check expr failed", KR(ret), K(check_expr));
          } else if (OB_ISNULL(check_expr)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("build check expr failed", KR(ret), K(check_expr));
          } else if (T_OP_EQ == check_expr->get_expr_type()) {
            sql::ObOpRawExpr *op_check_expr = static_cast<sql::ObOpRawExpr *>(check_expr);
            if (2 != op_check_expr->get_param_count()
                || OB_ISNULL(op_check_expr->get_param_expr(0))
                || OB_ISNULL(op_check_expr->get_param_expr(1))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid param count", K(op_check_expr->get_param_count()));
            } else {
              //for constraint check: left_expr = right_expr,
              //if one of the two childs is column_ref, and is equal to part key,
              //then convert the other one to expression text string.
              ObString part_col_name = column_schema->get_column_name_str();
              sql::ObRawExpr *left = op_check_expr->get_param_expr(0);
              sql::ObRawExpr *right = op_check_expr->get_param_expr(1);
              sql::ObColumnRefRawExpr *col_ref_raw_expr = NULL;
              sql::ObRawExpr *part_pruning_expr = NULL;
              if (left->is_column_ref_expr()) {
                col_ref_raw_expr = static_cast<sql::ObColumnRefRawExpr *>(left);
                if (part_col_name == col_ref_raw_expr->get_column_name()) {
                  part_pruning_expr = right;
                }
              } else if (right->is_column_ref_expr()) {
                col_ref_raw_expr = static_cast<sql::ObColumnRefRawExpr *>(right);
                if (part_col_name == col_ref_raw_expr->get_column_name()) {
                  part_pruning_expr = left;
                }
              }
              if (OB_NOT_NULL(part_pruning_expr)) {
                int64_t tmp_pos = 0;
                sql::ObRawExprPrinter expr_printer(expr_str_buf + pos,
                                                   OB_MAX_DEFAULT_VALUE_LENGTH - pos,
                                                   &tmp_pos,
                                                   &tenant_schema_guard_,
                                                   session_->get_timezone_info());
                if (OB_FAIL(expr_printer.do_print(part_pruning_expr, sql::T_NONE_SCOPE, true))) {
                  LOG_WARN("print expr definition failed", KR(ret));
                } else {
                  pos += tmp_pos;
                  if (pos < OB_MAX_DEFAULT_VALUE_LENGTH) {
                    expr_str_buf[pos] = ';';
                    pos++;
                  } else {
                    // do nothing
                  }
                }
              }
            }
          }
        } // for end;
        if (OB_SUCC(ret)) {
          //strip tail ';'
          if (pos - 1 > 0 && pos - 1 < OB_MAX_DEFAULT_VALUE_LENGTH && ';' == expr_str_buf[pos - 1]) {
            pos -= 1;
          }
          if (OB_FAIL(ob_write_string(
              *allocator_,
              ObString(pos, expr_str_buf),
              proxy_check_partition_str))) {
            LOG_WARN("fail to copy check string", KR(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObAllVirtualProxyPartitionInfo::build_check_str_to_raw_expr_(
    const ObString &check_expr_str,
    const share::schema::ObTableSchema &table_schema,
    sql::ObRawExpr *&check_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(allocator_), KR(ret));
  } else {
    sql::ObRawExprFactory expr_factory(*allocator_);
    const ParseNode *expr_node = NULL;
    ObArray<sql::ObQualifiedName> columns;
    ObArray<std::pair<sql::ObRawExpr*, sql::ObRawExpr*>> ref_sys_exprs;
    if (OB_FAIL(sql::ObRawExprUtils::parse_bool_expr_node_from_str(check_expr_str,
                                                                        *allocator_,
                                                                        expr_node))) {
      LOG_WARN("parse expr node from str failed", KR(ret), K(check_expr_str));
    } else if (OB_ISNULL(expr_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(session_), KR(ret));
    } else if (OB_FAIL(sql::ObRawExprUtils::build_check_constraint_expr(expr_factory,
                                                                        *session_,
                                                                        *expr_node,
                                                                        check_expr,
                                                                        columns))) {
      LOG_WARN("build constraint expr failed", KR(ret));
    } else if (OB_ISNULL(check_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("build constraint expr failed", KR(ret), K(check_expr));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const sql::ObQualifiedName &q_name = columns.at(i);
      const ObColumnSchemaV2 *col_schema = NULL;
      if (q_name.is_sys_func()) {
        sql::ObRawExpr *sys_func = q_name.access_idents_.at(0).sys_func_expr_;
        if (OB_ISNULL(sys_func)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys_func is null", KR(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_sys_exprs.count(); ++i) {
          if (OB_FAIL(sql::ObRawExprUtils::replace_ref_column(
              sys_func, ref_sys_exprs.at(i).first, ref_sys_exprs.at(i).second))) {
            LOG_WARN("replace ref_column failed", KR(ret), KPC(sys_func));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(q_name.access_idents_.at(0).sys_func_expr_->check_param_num())) {
          LOG_WARN("check param_num failed", KR(ret));
        } else if (OB_FAIL(sql::ObRawExprUtils::replace_ref_column(
                           check_expr, q_name.ref_expr_, sys_func))) {
          LOG_WARN("replace ref_column failed", KR(ret));
        } else if (OB_FAIL(ref_sys_exprs.push_back(
                   std::pair<sql::ObRawExpr*, sql::ObRawExpr*>(q_name.ref_expr_, sys_func)))) {
          LOG_WARN("push_back to ref_sys_exprs failed", KR(ret));
        }
      } else if (q_name.database_name_.length() > 0
                 || q_name.tbl_name_.length() > 0
                 || OB_ISNULL(q_name.ref_expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN( "invalid check expr", KR(ret), K(check_expr_str));
      } else if (NULL == (col_schema = table_schema.get_column_schema(q_name.col_name_))
                 || col_schema->is_hidden()) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        ObString scope_name = "constraint column function";
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, q_name.col_name_.length(), q_name.col_name_.ptr(),
                       scope_name.length(), scope_name.ptr());
      } else if (OB_FAIL(sql::ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
        LOG_WARN("init column expr failed", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("constraint check expr", K(check_expr_str));
      LOG_TRACE("constraint check expr parse_node: ",
                 K(sql::ObParserResultPrintWrapper(*expr_node)));
      LOG_TRACE("constraint check exp raw_expr: ", K(*check_expr));
    }
  }

  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
