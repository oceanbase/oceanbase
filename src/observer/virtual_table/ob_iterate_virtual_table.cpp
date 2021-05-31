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

#include "ob_iterate_virtual_table.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_i_data_access_service.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_result.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace share::schema;

namespace observer {

// convert effective_tenant_id() to bigint
static int ubigint2bigint(const ObObj& src, ObObj& dst, ObIAllocator&)
{
  // null type checked outside, no need to check again
  dst = src;
  dst.set_type(ObIntType);
  return OB_SUCCESS;
}

ObIterateVirtualTable::ObIterateVirtualTable()
    : tenant_idx_(-1), cur_tenant_id_(OB_INVALID_ID), record_real_tenant_id_(false), columns_with_tenant_id_()
{}

ObIterateVirtualTable::~ObIterateVirtualTable()
{}

int ObIterateVirtualTable::init(const uint64_t base_table_id, const bool record_real_tenant_id,
    const share::schema::ObTableSchema* index_table, const ObVTableScanParam& scan_param)
{
  record_real_tenant_id_ = record_real_tenant_id;
  return ObAgentTableBase::init(combine_id(OB_SYS_TENANT_ID, base_table_id), index_table, scan_param);
}

bool ObIterateVirtualTable::is_real_tenant_id() const
{
  return record_real_tenant_id_ || common::OB_SYS_TENANT_ID == cur_tenant_id_;
}

int ObIterateVirtualTable::set_column_name_with_tenant_id(const char* column_name)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t len = strlen(column_name);
  ObIAllocator* scan_allocator = NULL;
  // quote column name with ``
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->scan_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_param is null or scan_allocator is null", K(ret), KP(scan_param_));
  } else if (FALSE_IT(scan_allocator = scan_param_->scan_allocator_)) {
  } else if (OB_ISNULL(buf = static_cast<char*>(scan_allocator->alloc(len + 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    buf[0] = '`';
    MEMCPY(buf + 1, column_name, len);
    buf[len + 1] = '`';
    ObString column_name_with_tenant_id(len + 2, buf);
    if (OB_FAIL(columns_with_tenant_id_.push_back(column_name_with_tenant_id))) {
      LOG_WARN("fail to push back", K(ret), K(column_name_with_tenant_id));
    }
  }
  return ret;
}

int ObIterateVirtualTable::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAgentTableBase::do_open())) {
    LOG_WARN("base agent table open failed", K(ret));
  } else {
    tenant_idx_ = -1;
    cur_tenant_id_ = OB_INVALID_ID;
    int64_t match_cnt = 0;
    int64_t total_cnt = columns_with_tenant_id_.count();
    // tenant_id may be '0', replace to effective_tenant_id()
    FOREACH_X(item, mapping_, OB_SUCC(ret))
    {
      if (item->base_col_name_ == "`tenant_id`") {
        item->base_col_name_ = ObString::make_string("effective_tenant_id()");
        item->convert_func_ = ubigint2bigint;
      } else {
        // mark column with combine_tenant_id flag
        FOREACH_X(column_name, columns_with_tenant_id_, OB_SUCC(ret))
        {
          if (0 == column_name->case_compare(item->base_col_name_)) {
            item->combine_tenant_id_ = true;
            match_cnt++;
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !GCONF.in_upgrade_mode() && match_cnt != total_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_table_def maybe invalid", K(ret), K(match_cnt), K(total_cnt));
    }
  }

  if (OB_SUCC(ret)) {
    tenants_.set_block_allocator(ObWrapperAllocator(allocator_));
    ObArray<uint64_t, ObWrapperAllocator> all_tenants;
    all_tenants.set_block_allocator(ObWrapperAllocator(allocator_));
    if (OB_FAIL(schema_guard_->get_tenant_ids(all_tenants))) {
      LOG_WARN("get all tenant ids failed", K(ret));
    } else {
      FOREACH_X(id, all_tenants, OB_SUCC(ret))
      {
        bool in_range = scan_param_->key_ranges_.empty();
        FOREACH_CNT_X(range, scan_param_->key_ranges_, OB_SUCC(ret) && !in_range)
        {
          in_range = check_tenant_in_range(*id, *range);
        }
        if (in_range) {
          if (OB_FAIL(tenants_.push_back(*id))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (scan_flag_.is_reverse_scan()) {
        std::sort(tenants_.begin(), tenants_.end(), std::greater<uint64_t>());
      } else {
        std::sort(tenants_.begin(), tenants_.end());
      }
      LOG_DEBUG("tenant id array", K(tenants_));
    }

    // Iterate virtual table add tenant_id to the head of primary key/index,
    // set base rowkey offset to 1.
    base_rowkey_offset_ = 1;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_tenant())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("switch to next tenant failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObIterateVirtualTable::inner_close()
{
  int ret = OB_SUCCESS;
  tenants_.reset();
  if (OB_FAIL(ObAgentTableBase::inner_close())) {
    LOG_WARN("base agent table close failed", K(ret));
  }
  return ret;
}

bool ObIterateVirtualTable::check_tenant_in_range(const uint64_t tenant_id, const ObNewRange& r)
{
  bool in_range = !(r.start_key_.is_max_row() && r.end_key_.is_min_row());

  // tenant id always be the first column of rowkey
  const ObObj& start = r.start_key_.get_obj_ptr()[0];
  const ObObj& end = r.end_key_.get_obj_ptr()[0];

  if (in_range && r.start_key_.is_valid() && !start.is_ext() && tenant_id < start.get_uint64()) {
    in_range = false;
  }

  if (in_range && r.end_key_.is_valid() && !end.is_ext() && tenant_id > end.get_uint64()) {
    in_range = false;
  }
  return in_range;
}

int ObIterateVirtualTable::init_non_exist_map_item(MapItem& item, const schema::ObColumnSchemaV2& col)
{
  int ret = OB_SUCCESS;
  if (col.get_column_name_str() != ObString::make_string("tenant_id")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only tenant id may not exist in base table", K(ret), K(col));
  } else {
    item.base_col_name_ = ObString::make_string("effective_tenant_id()");
    item.convert_func_ = ubigint2bigint;
  }
  return ret;
}

int ObIterateVirtualTable::setup_inital_rowkey_condition(common::ObSqlString& cols, common::ObSqlString& vals)
{
  // When tenant_id is the first column of base table's primary key,
  // we should include tenant_id in rowkey condition. Set initial condition column
  // and value to "tenant_id" and "effective_tenant_id()", since we set $base_rowkey_offset_
  // to 1, the first rowkey column (tenant_id) was skipped in rowkey condition construct.
  //
  // e.g.: __all_tenant_meta_table's primary key is (tenant_id, table_id, ...)
  //
  int ret = OB_SUCCESS;

  if (index_table_->get_table_id() == table_schema_->get_table_id()) {
    const schema::ObColumnSchemaV2* col = NULL;
    const ObRowkeyInfo& info = base_table_->get_rowkey_info();
    if (OB_ISNULL(info.get_column(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty rowkey info", K(ret), K(info));
    } else if (OB_ISNULL(col = base_table_->get_column_schema(info.get_column(0)->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column not found", K(ret), K(info));
    } else {
      if (col->get_column_name_str() == ObString::make_string("tenant_id")) {
        if (OB_FAIL(cols.assign("tenant_id")) ||
            OB_FAIL(vals.assign(is_real_tenant_id() ? "effective_tenant_id()" : "0"))) {
          LOG_WARN("sql append failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObIterateVirtualTable::add_extra_condition(common::ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  if (NULL == base_table_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base table is NULL", K(ret));
  } else if (NULL != base_table_->get_column_schema("tenant_id")) {
    if (OB_FAIL(sql.append_fmt(" AND tenant_id = %s", is_real_tenant_id() ? "effective_tenant_id()" : "0"))) {
      LOG_WARN("append sql failed", K(ret));
    }
  }
  return ret;
}

int ObIterateVirtualTable::next_tenant()
{
  int ret = OB_SUCCESS;
  if (tenant_idx_ + 1 >= tenants_.count()) {
    ret = OB_ITER_END;
  } else {
    tenant_idx_ += 1;
    cur_tenant_id_ = tenants_.at(tenant_idx_);
    const uint64_t exec_tenant_id = cur_tenant_id_;
    ObSQLClientRetryWeak sql_client_retry_weak(GCTX.sql_proxy_, exec_tenant_id, base_table_id_);
    if (OB_ISNULL(sql_res_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql or sql result no init", K(ret), KP(sql_res_), K(sql_));
    } else if (OB_FAIL(sql_.set_length(0))) {
      LOG_WARN("reset sql failed", K(ret));
    } else if (OB_FAIL(construct_sql(sql_))) {
      LOG_WARN("construct sql failed", K(ret));
    } else {
      sql_res_->~ReadResult();
      inner_sql_res_ = NULL;
      new (sql_res_) ObMySQLProxy::MySQLResult();
      LOG_DEBUG("execute sql", K(cur_tenant_id_), K(sql_));
      if (OB_FAIL(sql_client_retry_weak.read(*sql_res_, exec_tenant_id, sql_.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql_));
      } else if (OB_ISNULL(sql_res_->get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL sql executing result", K(ret));
      } else {
        inner_sql_res_ = static_cast<ObInnerSQLResult*>(sql_res_->get_result());
      }
    }
  }
  return ret;
}

int ObIterateVirtualTable::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!opened_ && OB_FAIL(do_open())) {
    LOG_WARN("do open failed", K(ret));
  } else if (tenant_idx_ >= tenants_.count() || tenants_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(inner_sql_res_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL sql result", K(ret));
  } else {
    bool switched = false;
    do {
      switched = false;
      if (OB_FAIL(inner_sql_res_->next())) {
        if (OB_ITER_END == ret) {
          if (OB_FAIL(next_tenant())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("switch to next tenant failed", K(ret));
            }
          } else {
            switched = true;
          }
        } else {
          LOG_WARN("get next row from inner sql result failed", K(ret));
        }
      }
    } while (switched);
  }
  if (OB_SUCC(ret)) {
    const ObNewRow* r = inner_sql_res_->get_row();
    if (OB_ISNULL(r) || OB_ISNULL(r->cells_) || r->get_count() > cur_row_.count_ ||
        r->get_count() != scan_param_->column_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is NULL or column count mismatched", K(ret), KP(r), K(cur_row_.count_));
    } else {
      convert_alloc_.reuse();
      for (int64_t i = 0; i < r->get_count(); i++) {
        const ObObj& input = r->get_cell(i);
        const MapItem& item = mapping_[scan_param_->column_ids_.at(i)];
        ObObj& output = cur_row_.cells_[i];
        if (input.is_null() || NULL == item.convert_func_) {
          output = r->get_cell(i);
        } else {
          item.convert_func_(input, output, convert_alloc_);
        }
        // check column which should combine tenant_id
        bool decode = false;
        if (OB_FAIL(deal_with_column_with_tenant_id(item, convert_alloc_, decode, output))) {
          LOG_WARN("fail to change column value", K(ret));
        }
      }
      row = &cur_row_;
    }
  }
  return ret;
}

int ObIterateVirtualTable::change_column_value(const MapItem& item, ObIAllocator& allocator, ObObj& obj)
{
  bool decode = true;
  return deal_with_column_with_tenant_id(item, allocator, decode, obj);
}

int ObIterateVirtualTable::deal_with_column_with_tenant_id(
    const MapItem& item, ObIAllocator& allocator, bool decode, ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (!item.combine_tenant_id_ || obj.is_null()) {
    // do nothing
  } else if (OB_SYS_TENANT_ID == cur_tenant_id_ && OB_ALL_SEQUENCE_VALUE_TID != extract_pure_id(base_table_id_)) {
    // __all_sequence_value's sequence_id always filter tenant_id
  } else if ((OB_ALL_ROUTINE_PARAM_TID == extract_pure_id(base_table_id_) ||
                 OB_ALL_ROUTINE_PARAM_HISTORY_TID == extract_pure_id(base_table_id_)) &&
             0 == item.base_col_name_.case_compare("`type_owner`") && obj.is_int() && obj.get_int() > 0 &&
             OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // tenant_id of __all_routine_param's type_owner may be OB_SYS_TENANT_ID, do not filter it.
  } else if (OB_ALL_TENANT_DEPENDENCY_TID == extract_pure_id(base_table_id_) &&
             (0 == item.base_col_name_.case_compare("`ref_obj_id`") && obj.is_int() && obj.get_int() > 0) &&
             OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // ref_obj_id may contain sys tenant id, do not filter it.
  } else if ((OB_ALL_TYPE_ATTR_TID == extract_pure_id(base_table_id_) ||
                 OB_ALL_TYPE_ATTR_HISTORY_TID == extract_pure_id(base_table_id_)) &&
             0 == item.base_col_name_.case_compare("`type_attr_id`") && obj.is_int() && obj.get_int() > 0 &&
             OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // tenant_id of __all_type_attr's type_attr_id may be OB_SYS_TENANT_ID, do not filter it.
  } else if ((OB_ALL_COLL_TYPE_TID == extract_pure_id(base_table_id_) ||
                 OB_ALL_COLL_TYPE_HISTORY_TID == extract_pure_id(base_table_id_)) &&
             0 == item.base_col_name_.case_compare("`elem_type_id`") && obj.is_int() && obj.get_int() > 0 &&
             OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // tenant_id of __all_coll_type's elem_type_id may be OB_SYS_TENANT_ID, do not filter it.
  } else if (!obj.is_int() && !obj.is_uint64() && !obj.is_varchar_or_char()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported obj type", K(ret), K(obj));
  } else if (obj.is_int() && obj.get_int() > 0) {
    int64_t value =
        decode ? extract_pure_id(obj.get_int()) : static_cast<int64_t>(combine_id(cur_tenant_id_, obj.get_int()));
    obj.set_int_value(value);
  } else if (obj.is_uint64() && 0 != obj.get_uint64()) {
    uint64_t value =
        decode ? static_cast<uint64_t>(extract_pure_id(obj.get_int())) : combine_id(cur_tenant_id_, obj.get_int());
    obj.set_uint64_value(value);
  } else if (obj.is_varchar_or_char()) {
    // __all_sys_stat column `value`
    int64_t value = OB_INVALID_ID;
    if (OB_FAIL(ObSchemaUtils::str_to_int(obj.get_string(), value))) {
      LOG_WARN("fail to covert str to int", K(ret), K(obj));
    } else if (value > 0) {
      value = decode ? extract_pure_id(value) : static_cast<int64_t>(combine_id(cur_tenant_id_, value));
      int64_t len = OB_MAX_BIT_LENGTH;
      char* buf = static_cast<char*>(allocator.alloc(len));
      int64_t pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, len, pos, "%lu", value))) {
        LOG_WARN("fail to convert uint to str", K(ret), K(value));
      } else if (0 == pos || pos >= len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pos", K(ret), K(pos));
      } else {
        obj.set_char_value(buf, static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
