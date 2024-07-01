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

#include "lib/container/ob_array_iterator.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_result.h"
#include "observer/virtual_table/ob_iterate_private_virtual_table.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

namespace observer
{


ObIteratePrivateVirtualTable::ObIteratePrivateVirtualTable() :
    tenant_idx_(-1),
    cur_tenant_id_(OB_INVALID_ID),
    meta_record_in_sys_(false),
    tenants_(),
    sql_()
{
}

ObIteratePrivateVirtualTable::~ObIteratePrivateVirtualTable()
{
}

int ObIteratePrivateVirtualTable::init(
    const uint64_t base_table_id,
    const bool meta_record_in_sys,
    const share::schema::ObTableSchema *index_table,
    const ObVTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  // To avoid column mistach problem when cluster is upgrading:
  // 1. sys tenant's base table schema should be modified at last.
  // 2. Can't query related virtual table with `select *`.
  if (OB_FAIL(ObAgentTableBase::init(OB_SYS_TENANT_ID,
                                     base_table_id,
                                     index_table,
                                     scan_param))) {
    LOG_WARN("fail to init iterate private virtual table",
             KR(ret), K(base_table_id), K(meta_record_in_sys));
  } else {
    meta_record_in_sys_ = meta_record_in_sys;
  }
  return ret;
}

int ObIteratePrivateVirtualTable::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(schema_guard_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", KR(ret));
  } else if (OB_FAIL(ObAgentTableBase::do_open())) {
    LOG_WARN("base agent table open failed", KR(ret));
  } else {
    tenant_idx_ = -1;
    cur_tenant_id_ = OB_INVALID_ID;
    tenants_.set_block_allocator(ObWrapperAllocator(allocator_));
    ObArray<uint64_t, ObWrapperAllocator> all_tenants;
    all_tenants.set_block_allocator(ObWrapperAllocator(allocator_));
    if (OB_FAIL(schema_guard_->get_tenant_ids(all_tenants))) {
      LOG_WARN("get all normal tenant ids failed", KR(ret));
    } else {
      // To make sure that output data is ordered by tenant_id while meta_record_in_sys_ is true,
      // we only fetch single tenant's data in one inner sql
      // although user tenant doesn't have any cluster private table.
      FOREACH_X(id, all_tenants, OB_SUCC(ret)) {
        bool in_range = scan_param_->key_ranges_.empty();
        FOREACH_CNT_X(range, scan_param_->key_ranges_, OB_SUCC(ret) && !in_range) {
          in_range = check_tenant_in_range_(*id, *range);
          LOG_TRACE("check tenant in range", K(*id), K(*range));
        }
        // user tenant can only see its own data while sys tenant can see all tenant's data.
        if (in_range
            && (is_sys_tenant(effective_tenant_id_)
                || *id == effective_tenant_id_)) {
          const ObSimpleTenantSchema *simple_tenant_schema = NULL;
          const uint64_t exec_tenant_id = get_exec_tenant_id_(*id);
          bool exist = false;
          if (OB_FAIL(schema_guard_->get_tenant_info(exec_tenant_id, simple_tenant_schema))) {
            LOG_WARN("fail to get tenant schema", KR(ret), K(*id), K(exec_tenant_id));
          } else if (OB_ISNULL(simple_tenant_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant schema is nullptr", KR(ret), K(*id), K(exec_tenant_id));
          } else if (!simple_tenant_schema->is_normal()) {
            LOG_TRACE("tenant status is not normal, just skip", K(exec_tenant_id));
          } else if (OB_FAIL(schema_guard_->check_table_exist(
                     exec_tenant_id, base_table_id_, exist))) {
            LOG_WARN("fail to check table exist",
                     KR(ret), K(exec_tenant_id), K(base_table_id_));
          } else if (!exist) {
            LOG_TRACE("table not exist in tenant, maybe in upgrade process, just skip",
                      K(exec_tenant_id), K(base_table_id_));
          } else if (OB_FAIL(tenants_.push_back(*id))) {
            LOG_WARN("array push back failed", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (scan_flag_.is_reverse_scan()) {
        lib::ob_sort(tenants_.begin(), tenants_.end(), std::greater<uint64_t>());
      } else {
        lib::ob_sort(tenants_.begin(), tenants_.end());
      }
      LOG_TRACE("tenant id array", K(tenants_));
    }

    // Iterate virtual table's `tenant_id` to the head of primary key/index,
    // set base rowkey offset to 1.
    base_rowkey_offset_ = 1;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_tenant_())) {
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

int ObIteratePrivateVirtualTable::inner_close()
{
  int ret = OB_SUCCESS;
  tenants_.reset();
  if (OB_FAIL(ObAgentTableBase::inner_close())) {
    LOG_WARN("base agent table close failed", KR(ret));
  }
  return ret;
}

bool ObIteratePrivateVirtualTable::check_tenant_in_range_(const uint64_t tenant_id, const ObNewRange &r)
{
  bool in_range = !(r.start_key_.is_max_row() && r.end_key_.is_min_row());

  // tenant id always be the first column of rowkey
  const ObObj &start = r.start_key_.get_obj_ptr()[0];
  const ObObj &end = r.end_key_.get_obj_ptr()[0];

  if (in_range && r.start_key_.is_valid() && !start.is_ext()
      && ((!start.is_number() && tenant_id < start.get_uint64())
           || (start.is_number() && start.get_number().compare(tenant_id) < 0))) {
    in_range = false;
  }

  if (in_range && r.end_key_.is_valid() && !end.is_ext()
      && ((!end.is_number() && tenant_id > end.get_uint64())
           || (end.is_number() && end.get_number().compare(tenant_id) > 0))) {
    in_range = false;
  }
  return in_range;
}

int ObIteratePrivateVirtualTable::init_non_exist_map_item(MapItem &item,
    const schema::ObColumnSchemaV2 &col)
{
  return OB_ERR_UNEXPECTED;
}

int ObIteratePrivateVirtualTable::setup_inital_rowkey_condition(
    common::ObSqlString &cols, common::ObSqlString &vals)
{
  // When tenant_id is the first column of base table's primary key,
  // we should include tenant_id in rowkey condition. Set initial condition column
  // and value to cur_tenant_id_, since we set $base_rowkey_offset_
  // to 1, the first rowkey column (tenant_id) was skipped in rowkey condition construct.
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(index_table_)
                  || OB_ISNULL(table_schema_)
                  || OB_ISNULL(base_table_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index or table is null", KR(ret), KP_(index_table), KP_(table_schema));
  } else if (index_table_->get_table_id() == table_schema_->get_table_id()) {
    const schema::ObColumnSchemaV2 *col = NULL;
    const ObRowkeyInfo &info = base_table_->get_rowkey_info();
    if (OB_UNLIKELY(OB_ISNULL(info.get_column(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty rowkey info", KR(ret), K(info));
    } else if (OB_UNLIKELY(OB_ISNULL(col = base_table_->get_column_schema(
               info.get_column(0)->column_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column not found", KR(ret), K(info));
    } else if (col->get_column_name_str() == ObString::make_string("tenant_id")) {
      if (OB_FAIL(cols.assign("tenant_id"))
          || OB_FAIL(vals.assign_fmt("%lu", cur_tenant_id_))) {
        LOG_WARN("sql append failed", KR(ret), K_(cur_tenant_id));
      }
    }
  }

  return ret;
}

int ObIteratePrivateVirtualTable::add_extra_condition(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt(" AND tenant_id = %lu", cur_tenant_id_))) {
    LOG_WARN("append sql failed", KR(ret), K_(cur_tenant_id));
  }

  /*
  * add filter for sensitive data, do not let this to influence other condition or
  * be influenced by other
  */
  if (OB_SUCC(ret)) {
    if (!is_sys_tenant(effective_tenant_id_)) {
      if (OB_TENANT_PARAMETER_TID == base_table_id_) {
        if (OB_FAIL(sql.append_fmt(" AND name not in ('external_kms_info')"))) {
          LOG_WARN("append filter sql failed", KR(ret), K_(cur_tenant_id), K_(base_table_id));
        }
      }
    }
  }
  return ret;
}

int ObIteratePrivateVirtualTable::next_tenant_()
{
  int ret = OB_SUCCESS;
  if (tenant_idx_ + 1 >= tenants_.count()) {
    ret = OB_ITER_END;
  } else {
    tenant_idx_ += 1;
    cur_tenant_id_ = tenants_.at(tenant_idx_);
    const uint64_t exec_tenant_id = get_exec_tenant_id_(cur_tenant_id_);
    if (OB_UNLIKELY(OB_ISNULL(GCTX.sql_proxy_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", KR(ret));
    } else  if (OB_UNLIKELY(OB_ISNULL(sql_res_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql or sql result no init", KR(ret), KP(sql_res_), K(sql_));
    } else if (OB_FAIL(sql_.set_length(0))) {
      LOG_WARN("reset sql failed", KR(ret));
    } else if (OB_FAIL(construct_sql(exec_tenant_id, sql_))) {
      LOG_WARN("construct sql failed", KR(ret), K(exec_tenant_id));
    } else {
      LOG_TRACE("construct iterate private virtual table sql",
                K(exec_tenant_id), K_(cur_tenant_id), K_(sql));
      sql_res_->~ReadResult();
      inner_sql_res_ = NULL;
      new (sql_res_) ObMySQLProxy::MySQLResult();
      if (OB_FAIL(GCTX.sql_proxy_->read(*sql_res_, exec_tenant_id, sql_.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(exec_tenant_id), K_(cur_tenant_id), K(sql_));
      } else if (OB_ISNULL(sql_res_->get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL sql executing result", KR(ret), K(exec_tenant_id), K_(cur_tenant_id), K(sql_));
      } else {
        inner_sql_res_ = static_cast<ObInnerSQLResult *>(sql_res_->get_result());
      }
    }
  }
  return ret;
}

int ObIteratePrivateVirtualTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!opened_ && OB_FAIL(do_open())) {
    LOG_WARN("do open failed", KR(ret));
  } else if (tenant_idx_ >= tenants_.count() || tenants_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(OB_ISNULL(inner_sql_res_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL sql result", KR(ret));
  } else {
    bool switched = false;
    do {
      switched = false;
      if (OB_FAIL(inner_sql_res_->next())) {
        if (OB_ITER_END == ret) {
          if (OB_FAIL(next_tenant_())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("switch to next tenant failed", KR(ret));
            }
          } else {
            switched = true;
          }
        } else {
          LOG_WARN("get next row from inner sql result failed", KR(ret));
        }
      }
    } while (switched);
  }
  if (OB_SUCC(ret)) {
    const ObNewRow *r = inner_sql_res_->get_row();
    if (OB_UNLIKELY(OB_ISNULL(r)
        || OB_ISNULL(r->cells_)
        || r->get_count() < cur_row_.count_
        || cur_row_.count_ != scan_param_->column_ids_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is NULL or column count mismatched", KR(ret), KP(r), K(cur_row_.count_));
    } else if (OB_FAIL(try_convert_row(r, row))) {
      LOG_WARN("try_convert_row failed", KR(ret), KP(r));
    }
  }
  return ret;
}

int ObIteratePrivateVirtualTable::try_convert_row(const ObNewRow *input_row, ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  convert_alloc_.reuse();
  for (int64_t i = 0; i < cur_row_.count_; i++) {
    const ObObj &input = input_row->get_cell(i);
    const MapItem &item = mapping_[scan_param_->column_ids_.at(i)];
    ObObj &output = cur_row_.cells_[i];
    if (input.is_null() || NULL == item.convert_func_) {
      output = input_row->get_cell(i);
    } else {
      item.convert_func_(input, output, convert_alloc_);
    }
  }
  row = &cur_row_;

  return ret;
}

uint64_t ObIteratePrivateVirtualTable::get_exec_tenant_id_(const uint64_t tenant_id)
{
  uint64_t exec_tenant_id = OB_INVALID_TENANT_ID;
  if (is_sys_tenant(tenant_id)) {
    exec_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_meta_tenant(tenant_id)) {
    exec_tenant_id = meta_record_in_sys_ ? OB_SYS_TENANT_ID : tenant_id;
  } else if (is_user_tenant(tenant_id)) {
    exec_tenant_id = gen_meta_tenant_id(tenant_id);
  }
  return exec_tenant_id;
}

static int varchar_to_empty_string(const ObObj &src, ObObj &dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  dst = src;
  dst.set_varchar("");
  return ret;
}

int ObIteratePrivateVirtualTable::set_convert_func(convert_func_t &func,
            const schema::ObColumnSchemaV2 &col, const schema::ObColumnSchemaV2 &base_col)
{
  int ret = OB_SUCCESS;
  if (!is_sys_tenant(effective_tenant_id_)) {
    if (OB_ALL_RECOVER_TABLE_JOB_TID == base_table_id_ ||
        OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID == base_table_id_) {
      if (base_col.get_column_name_str() == ObString::make_string("external_kms_info")) {
        func = varchar_to_empty_string;
      }
    }
  }
  return ret;
}
} // end namespace observer
} // end namespace oceanbase
