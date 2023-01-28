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
#include "share/ob_i_tablet_scan.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_result.h"
#include "observer/ob_sql_client_decorator.h"

#include "pl/ob_pl_stmt.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

namespace observer
{

// convert effective_tenant_id() to bigint
static int ubigint2bigint(const ObObj &src, ObObj &dst, ObIAllocator &)
{
  // null type checked outside, no need to check again
  dst = src;
  dst.set_type(ObIntType);
  return OB_SUCCESS;
}

ObIterateVirtualTable::ObIterateVirtualTable() :
    tenant_idx_(-1),
    cur_tenant_id_(OB_INVALID_ID)
{
}

ObIterateVirtualTable::~ObIterateVirtualTable()
{
}

int ObIterateVirtualTable::init(
    const uint64_t base_table_id,
    const share::schema::ObTableSchema *index_table,
    const ObVTableScanParam &scan_param)
{
  return ObAgentTableBase::init(OB_SYS_TENANT_ID,
                                base_table_id,
                                index_table,
                                scan_param);
}

int ObIterateVirtualTable::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAgentTableBase::do_open())) {
    LOG_WARN("base agent table open failed", KR(ret));
  } else {
    tenant_idx_ = -1;
    cur_tenant_id_ = OB_INVALID_ID;
    // tenant_id may be '0', replace to effective_tenant_id()
    FOREACH_X(item, mapping_, OB_SUCC(ret)) {
      if (item->base_col_name_ == "`tenant_id`") {
        item->base_col_name_ = ObString::make_string("effective_tenant_id()");
        item->convert_func_ = ubigint2bigint;
      }
    }
  }

  if (OB_SUCC(ret)) {
    tenants_.set_block_allocator(ObWrapperAllocator(allocator_));
    ObArray<uint64_t, ObWrapperAllocator> all_tenants;
    all_tenants.set_block_allocator(ObWrapperAllocator(allocator_));
    if (OB_FAIL(schema_guard_->get_available_tenant_ids(all_tenants))) {
      LOG_WARN("get all normal tenant ids failed", KR(ret));
    } else {
      FOREACH_X(id, all_tenants, OB_SUCC(ret)) {
        bool in_range = scan_param_->key_ranges_.empty();
        FOREACH_CNT_X(range, scan_param_->key_ranges_, OB_SUCC(ret) && !in_range) {
          in_range = check_tenant_in_range(*id, *range);
        }
        // user tenant can only see its own data while sys tenant can see all tenant's data.
        if (in_range
            && (is_sys_tenant(effective_tenant_id_)
                || *id == effective_tenant_id_)) {
          bool exist = false;
          if (OB_FAIL(schema_guard_->check_table_exist(*id, base_table_id_, exist))) {
            LOG_WARN("fail to check table exist",
                     KR(ret), "tenant_id", *id, K(base_table_id_));
          } else if (!exist) {
            // 1. in upgrade process
            // 2. restore tenant from low data version
            LOG_TRACE("table not exist in tenant, maybe inner schemas are old, just skip",
                      "tenant_id", *id, K(base_table_id_));
          } else if (OB_FAIL(tenants_.push_back(*id))) {
            LOG_WARN("array push back failed", KR(ret));
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
          LOG_WARN("switch to next tenant failed", KR(ret));
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

bool ObIterateVirtualTable::check_tenant_in_range(const uint64_t tenant_id, const ObNewRange &r)
{
  bool in_range = !(r.start_key_.is_max_row() && r.end_key_.is_min_row());

  // tenant id always be the first column of rowkey
  const ObObj &start = r.start_key_.get_obj_ptr()[0];
  const ObObj &end = r.end_key_.get_obj_ptr()[0];

  if (in_range && r.start_key_.is_valid() && !start.is_ext()
      && tenant_id < start.get_uint64()) {
    in_range = false;
  }

  if (in_range && r.end_key_.is_valid() && !end.is_ext()
      && tenant_id > end.get_uint64()) {
    in_range = false;
  }
  return in_range;
}

int ObIterateVirtualTable::init_non_exist_map_item(MapItem &item,
    const schema::ObColumnSchemaV2 &col)
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

int ObIterateVirtualTable::setup_inital_rowkey_condition(
    common::ObSqlString &cols, common::ObSqlString &vals)
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
    const schema::ObColumnSchemaV2 *col = NULL;
    const ObRowkeyInfo &info = base_table_->get_rowkey_info();
    if (OB_ISNULL(info.get_column(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty rowkey info", K(ret), K(info));
    } else if (OB_ISNULL(col = base_table_->get_column_schema(
        info.get_column(0)->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column not found", K(ret), K(info));
    } else {
      if (col->get_column_name_str() == ObString::make_string("tenant_id")) {
        if (OB_FAIL(cols.assign("tenant_id")) || OB_FAIL(vals.assign("0"))) {
          LOG_WARN("sql append failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObIterateVirtualTable::add_extra_condition(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (NULL == base_table_) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("base table is NULL", K(ret));
  } else if (NULL != base_table_->get_column_schema("tenant_id")) {
    if (OB_FAIL(sql.append_fmt(" AND tenant_id = 0"))) {
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
      LOG_WARN("sql or sql result no init", KR(ret), K(exec_tenant_id), KP(sql_res_), K(sql_));
    } else if (OB_FAIL(sql_.set_length(0))) {
      LOG_WARN("reset sql failed", KR(ret), K(exec_tenant_id), K(sql_));
    } else if (OB_FAIL(construct_sql(exec_tenant_id, sql_))) {
      LOG_WARN("construct sql failed", KR(ret), K(exec_tenant_id), K(sql_));
    } else {
      sql_res_->~ReadResult();
      inner_sql_res_ = NULL;
      new (sql_res_) ObMySQLProxy::MySQLResult();
      LOG_TRACE("execute sql", K(exec_tenant_id), K(sql_));
      if (OB_FAIL(sql_client_retry_weak.read(*sql_res_, exec_tenant_id, sql_.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(exec_tenant_id), K(sql_));
      } else if (OB_ISNULL(sql_res_->get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL sql executing result", KR(ret), K(exec_tenant_id), K(sql_));
      } else {
        inner_sql_res_ = static_cast<ObInnerSQLResult *>(sql_res_->get_result());
      }
    }
  }
  return ret;
}

int ObIterateVirtualTable::inner_get_next_row(ObNewRow *&row)
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
    const ObNewRow *r = inner_sql_res_->get_row();
    if (OB_ISNULL(r) || OB_ISNULL(r->cells_)
        || r->get_count() < cur_row_.get_count()
        || cur_row_.get_count() != scan_param_->column_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is NULL or column count mismatched", K(ret), KP(r), K(cur_row_.count_));
    } else {
      convert_alloc_.reuse();
      for (int64_t i = 0; i < cur_row_.count_; i++) {
        const ObObj &input = r->get_cell(i);
        const MapItem &item = mapping_[scan_param_->column_ids_.at(i)];
        ObObj &output = cur_row_.cells_[i];
        if (input.is_null() || NULL == item.convert_func_) {
          output = r->get_cell(i);
        } else {
          item.convert_func_(input, output, convert_alloc_);
        }
      }
      row = &cur_row_;
    }
  }
  return ret;
}

int ObIterateVirtualTable::change_column_value(
    const MapItem &item,
    ObIAllocator &allocator,
    ObObj &obj)
{
  UNUSEDx(item, allocator, obj);
  return OB_SUCCESS;
}

} // end namespace observer
} // end namespace oceanbase
