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

#include "ob_agent_virtual_table.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_result.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

using namespace common;
using namespace share;

namespace observer
{

// convert mysql type to oracle type, only types used in inter table schema define are supported.
// see replace_agent_table_columns_def of generate_inner_table_schema.py.
static int int2number(const ObObj &src, ObObj &dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  number::ObNumber num;
  if (OB_FAIL(num.from(src.get_int(), allocator))) {
    LOG_WARN("convert int to number failed", K(ret), K(src));
  } else {
    dst.set_number(num);
  }
  return ret;
}

static int uint2number(const ObObj &src, ObObj &dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  number::ObNumber num;
  if (OB_FAIL(num.from(src.get_uint64(), allocator))) {
    LOG_WARN("convert int to number failed", K(ret), K(src));
  } else {
    dst.set_number(num);
  }
  return ret;
}

static int double2number(const ObObj &src, ObObj &dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  // copy from ob_obj_cast.cpp:
  static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;
  char buf[MAX_DOUBLE_PRINT_SIZE];
  MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
  snprintf(buf, MAX_DOUBLE_PRINT_SIZE, "%lf", src.get_double());
  number::ObNumber nmb;
  if (OB_FAIL(nmb.from(buf, allocator))) {
    LOG_WARN("convert double to number failed", K(ret), K(src));
  } else {
    dst.set_number(nmb);
  }
  return ret;
}

static int varchar2varchar(const ObObj &src, ObObj &dst, ObIAllocator &)
{
  if (src.val_len_ <= 0) {
    dst.set_null();
  } else {
    dst = src;
    //对于oracle租户内部表,从表、列到字符串类型确保所有collation为CS_TYPE_UTF8MB4_BIN
    dst.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  }
  return OB_SUCCESS;
}

static int number2number(const ObObj &src, ObObj &dst, ObIAllocator &)
{
  dst = src;
  return OB_SUCCESS;
}

static int longtext2longtext(const ObObj &src, ObObj &dst, ObIAllocator &)
{
  dst = src;
  return OB_SUCCESS;
}

static int timestamp2otimestamp(const ObObj &src, ObObj &dst, ObIAllocator &)
{
  ObOTimestampData td;
  td.time_us_ = src.get_timestamp();
  td.time_ctx_.tail_nsec_ = 0;
  dst.set_otimestamp_value(ObTimestampLTZType, td);
  return OB_SUCCESS;
}

ObAgentVirtualTable::ObAgentVirtualTable() : general_tenant_id_(OB_INVALID_TENANT_ID),
                                             mode_(lib::Worker::CompatMode::ORACLE)
{
}

ObAgentVirtualTable::~ObAgentVirtualTable()
{
}

// if base table is in tenant space, sys_tenant_base_table = true.
// if virtual table is sys agent table, only_sys_data = true
int ObAgentVirtualTable::init(
    const uint64_t pure_table_id,
    const bool sys_tenant_base_table,
    const share::schema::ObTableSchema *index_table,
    const ObVTableScanParam &scan_param,
    const bool only_sys_data,
    const lib::Worker::CompatMode &mode)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == pure_table_id
      || OB_ISNULL(index_table)
      || !scan_param.ObVTableScanParam::is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pure_table_id), KP(index_table), K(scan_param));
  } else {
    only_sys_data_ = only_sys_data;
    mode_ = mode;
    const uint64_t base_tenant_id = (sys_tenant_base_table || only_sys_data) ?
                                    OB_SYS_TENANT_ID : scan_param.tenant_id_;
    if (OB_FAIL(ObAgentTableBase::init(base_tenant_id, pure_table_id, index_table, scan_param))) {
      LOG_WARN("init base agent table failed", K(ret));
    } else if (OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID == pure_table_id) {
      general_tenant_id_ = scan_param.tenant_id_;
    }
  }

  return ret;
}

int ObAgentVirtualTable::do_open()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(ObAgentTableBase::do_open())) {
    LOG_WARN("base agent table open failed", KR(ret));
  } else if (OB_FAIL(construct_sql(base_tenant_id_, sql))) {
    LOG_WARN("construct sql failed", KR(ret), K(base_tenant_id_));
  } else if (OB_FAIL(GCTX.sql_proxy_->read(*sql_res_, base_tenant_id_, sql.ptr()))) {
    LOG_WARN("execute sql failed", KR(ret), K(base_tenant_id_), K(sql));
  } else if (OB_ISNULL(sql_res_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL sql executing result", KR(ret), K(base_tenant_id_), K(sql));
  } else {
    inner_sql_res_ = static_cast<ObInnerSQLResult *>(sql_res_->get_result());
    if (general_tenant_id_ != OB_INVALID_TENANT_ID) {
      inner_sql_res_->result_set().get_session().switch_tenant(general_tenant_id_);
    }
  }

  return ret;
}

int ObAgentVirtualTable::set_convert_func(convert_func_t &func,
      const schema::ObColumnSchemaV2 &col, const schema::ObColumnSchemaV2 &base_col)
{
  int ret = OB_SUCCESS;
  if (lib::Worker::CompatMode::MYSQL == mode_) {
    // do not set_convert_func
  } else {
    ObObjType from = base_col.get_data_type();
    ObObjType to = col.get_data_type();
    ObObjTypeClass fromclass = ob_obj_type_class(from);
    if (ObVarcharType == from && ObVarcharType == to) {
      func = varchar2varchar;
    } else if (ObNumberType == to && ObNumberType == from) {
      func = number2number;
    } else if (ObNumberType == to && ObIntTC == fromclass) {
      func = int2number;
    } else if (ObNumberType == to && ObUIntTC == fromclass) {
      func = uint2number;
    } else if (ObNumberType == to && ObDoubleTC == fromclass) {
      func = double2number;
    } else if (ObTimestampType == from && ObTimestampLTZType == to) {
      func = timestamp2otimestamp;
    } else if (ObLongTextType == from && ObLongTextType == to) {
      func = longtext2longtext;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported type convert",
          K(ret), K(from), K(to), K(col), K(base_col));
    }
  }

  return ret;
}

int ObAgentVirtualTable::init_non_exist_map_item(
    MapItem &, const schema::ObColumnSchemaV2 &col)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("base table's column not found", K(ret), K(col), K(*base_table_));
  return ret;
}

int ObAgentVirtualTable::add_extra_condition(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else {
    if (!only_sys_data_) {
      tenant_id = table_schema_->get_tenant_id();
    }
  }
  bool need_tenant_id = true;
  if (OB_FAIL(ret)) {
  } else if (lib::Worker::CompatMode::MYSQL == mode_ && is_sys_tenant(effective_tenant_id_)) {
    LOG_TRACE("do not add tenant_id filter", K_(mode), K_(effective_tenant_id));
  } else if (OB_FAIL(should_add_tenant_condition(need_tenant_id, tenant_id))) {
    LOG_WARN("failed to get is_add_tenant_condition flag", K(ret));
  } else if (need_tenant_id && NULL != table_schema_->get_column_schema("tenant_id")) {
    if (OB_FAIL(sql.append_fmt(" AND tenant_id = %ld", tenant_id))) {
      LOG_WARN("append sql failed", K(ret));
    }
  }
  return ret;
}

int ObAgentVirtualTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObNewRow *base_row = NULL;
  if (!opened_ && OB_FAIL(do_open())) {
    LOG_WARN("do open failed", K(ret));
  } else if (OB_ISNULL(inner_sql_res_) || OB_ISNULL(scan_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null scan parameter or result", K(ret));
  } else if (OB_FAIL(inner_sql_res_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("iterate inner sql result failed", K(ret));
    }
  } else if (OB_ISNULL(base_row = inner_sql_res_->get_row())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL row", K(ret));
  } else if (OB_ISNULL(cur_row_.cells_) || cur_row_.count_ < scan_param_->column_ids_.count()
      || base_row->get_count() < scan_param_->column_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count mismatch", K(ret), KP(cur_row_.cells_), K(cur_row_.count_),
        K(base_row->get_count()), K(scan_param_->column_ids_.count()));
  } else {
    convert_alloc_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param_->column_ids_.count(); i++) {
      const ObObj &input = base_row->get_cell(i);
      if (input.is_null()) {
        cur_row_.cells_[i].set_null();
      } else if (lib::Worker::CompatMode::MYSQL == mode_) {
        cur_row_.cells_[i] = input;
        LOG_INFO("mysql compat mode agent do not do convert", KR(ret), K(input));
      } else if (OB_FAIL(mapping_[scan_param_->column_ids_.at(i)].convert_func_(
          input, cur_row_.cells_[i], convert_alloc_))) {
        LOG_WARN("convert obj failed", K(ret), K(input),
            K(mapping_[scan_param_->column_ids_.at(i)]));
      }
    }

    if (OB_SUCC(ret)) {
      cur_row_.count_ = scan_param_->column_ids_.count();
      row = &cur_row_;
    }
  }

  return ret;
}

// 如果query range为 (tenant_id, cond1, cond2, ...) <= (v1, v2, v3, ...) and (tenant_id, cond1, cond2, ...) >= (v1', v2', v3', ...)
// 那么如果在最后的条件加上 and tenant_id = v，也就是
// (tenant_id, cond1, cond2, ...) <= (v1, v2, v3, ...) and (tenant_id, cond1, cond2, ...) >= (v1', v2', v3', ...) and tenant_id = v
// 优化器这时候抽不出来query range，只能得到 (tenant_id, min, min), (tenant_id, max, max)，
// 对于某些只支持get操作的表(比如plan_cache_plan_explain)，结果集总为空，所以需要加上这个判断条件
int ObAgentVirtualTable::should_add_tenant_condition(bool &need, const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  need = true;

  if (OB_ISNULL(scan_param_) || OB_ISNULL(index_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(scan_param_), K(index_table_), K(ret));
  } else {
    const ObRangeArray &ranges = scan_param_->key_ranges_;
    const ObRowkeyInfo &info = index_table_->get_rowkey_info();
    if (info.get_size() <= 0) {
      LOG_DEBUG("rowkeys is empty", K(ret), K(info.get_size()));
    } else {
      FOREACH_CNT_X(r, ranges, OB_SUCC(ret)) {
        if (r->table_id_ != index_table_->get_table_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index table id", K(*r), K(*index_table_));
        } else if (!r->start_key_.is_valid() || !r->end_key_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowkey is invalid", K(ret));
        } else {
          uint64_t cid = 0;
          if (OB_FAIL(info.get_column_id(base_rowkey_offset_, cid))) {
            LOG_WARN("failed to get column id", K(ret), K(base_rowkey_offset_), K(info));
          } else if (0 == mapping_.at(cid).base_col_name_.compare("`tenant_id`")) {
            // not need: rowkey start with tenant_id and all ranges' tenant_id equal to %tenant_id
            const int64_t idx = base_rowkey_offset_;
            const ObRowkey &s = r->start_key_;
            const ObRowkey &e = r->end_key_;
            if (s.get_obj_cnt() > idx
                && e.get_obj_cnt() > idx
                && s.get_obj_ptr()[idx] == e.get_obj_ptr()[idx]
                && s.get_obj_ptr()[idx].is_number()) {
              number::ObNumber nmb;
              s.get_obj_ptr()[idx].get_number(nmb);
              if (nmb.compare(tenant_id) == 0) {
                need = false;
                continue;
              }
            }
          }
        }
        need = true;
        break;
      }
    }
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase

