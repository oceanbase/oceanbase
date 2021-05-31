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

#include "ob_agent_table_base.h"
#include "share/ob_i_data_access_service.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace share::schema;

namespace observer {

ObAgentTableBase::ObAgentTableBase()
    : allocator_bak_(NULL),
      base_table_id_(OB_INVALID_ID),
      base_table_(NULL),
      index_table_(NULL),
      scan_param_(NULL),
      base_rowkey_offset_(0),
      sql_res_(NULL),
      inner_sql_res_(NULL),
      opened_(false)
{}

void ObAgentTableBase::destroy()
{
  opened_ = false;
  if (NULL != sql_res_) {
    sql_res_->~ReadResult();
    inner_sql_res_ = NULL;
    if (NULL != allocator_) {
      allocator_->free(sql_res_);
    }
    sql_res_ = NULL;
  }

  if (NULL != allocator_) {
    FOREACH_CNT(it, mapping_)
    {
      if (NULL != it->base_col_name_.ptr()) {
        allocator_->free(it->base_col_name_.ptr());
        it->base_col_name_.reset();
      }
    }
    mapping_.reset();
  }
  if (allocator_ == &inner_allocator_) {
    inner_allocator_.reuse();
    allocator_ = allocator_bak_;
  }
}

ObAgentTableBase::~ObAgentTableBase()
{
  destroy();
}

int ObAgentTableBase::inner_close()
{
  int ret = OB_SUCCESS;
  destroy();
  return ret;
}

int ObAgentTableBase::init(
    const uint64_t base_table_id, const share::schema::ObTableSchema* index_table, const ObVTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == base_table_id || OB_ISNULL(index_table) || !scan_param.ObVTableScanParam::is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(base_table_id), KP(index_table), K(scan_param));
  } else {
    base_table_id_ = base_table_id;
    index_table_ = index_table;
    scan_param_ = &scan_param;
    // for compatiable
    uint64_t tenant_id = extract_tenant_id(base_table_id);
    uint64_t pure_id = extract_pure_id(base_table_id);
    if (OB_ALL_TABLE_TID == pure_id && OB_FAIL(ObSchemaUtils::get_all_table_id(tenant_id, base_table_id_))) {
      LOG_WARN("fail to get all table tid", K(ret), K(base_table_id));
    } else if (OB_ALL_TABLE_HISTORY_TID == pure_id &&
               OB_FAIL(ObSchemaUtils::get_all_table_history_id(tenant_id, base_table_id_))) {
      LOG_WARN("fail to get all table tid", K(ret), K(base_table_id));
    }
  }
  return ret;
}

int ObAgentTableBase::do_open()
{
  int ret = OB_SUCCESS;
  // %table_schema_, %allocator_, %schema_guard_ are set after init() before open().
  if (OB_ISNULL(index_table_) || OB_ISNULL(table_schema_) || OB_ISNULL(scan_param_) || OB_ISNULL(allocator_) ||
      OB_ISNULL(schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        KP(index_table_),
        KP(table_schema_),
        KP(scan_param_),
        KP(allocator_),
        KP(schema_guard_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is NULL", K(ret));
  } else {
    allocator_bak_ = allocator_;
    allocator_ = &inner_allocator_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard_->get_table_schema(base_table_id_, base_table_))) {
    LOG_WARN("get table schema failed", K(ret), K(base_table_id_));
  } else if (OB_ISNULL(base_table_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not found", K(ret), K(base_table_id_));
  } else if (OB_ISNULL(sql_res_ = OB_NEWx(ObMySQLProxy::MySQLResult, (allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate result failed", K(ret));
  } else {
    convert_alloc_.set_tenant_id(table_schema_->get_tenant_id());
    mapping_.set_block_allocator(ObWrapperAllocator(allocator_));
    FOREACH_CNT_X(c, scan_param_->column_ids_, OB_SUCC(ret))
    {
      if (OB_ISNULL(table_schema_->get_column_schema(*c))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column id", K(ret), K(*c));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_base_table_mapping())) {
      LOG_WARN("build base table mapping failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    opened_ = true;
  }
  return ret;
}

int ObAgentTableBase::build_base_table_mapping()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_) || OB_ISNULL(base_table_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table is NULL", K(ret));
  } else if (OB_FAIL(mapping_.prepare_allocate(table_schema_->get_max_used_column_id() + 1))) {
    LOG_WARN("array prepare allocate failed", K(ret), K(table_schema_->get_max_used_column_id()));
  } else {
    CompatModeGuard g(ObWorker::CompatMode::MYSQL);
    for (auto col = table_schema_->column_begin(); OB_SUCC(ret) && col != table_schema_->column_end(); col++) {
      auto& item = mapping_.at((*col)->get_column_id());
      auto base_col = base_table_->get_column_schema((*col)->get_column_name());
      if (OB_ISNULL(base_col)) {
        if (OB_FAIL(init_non_exist_map_item(item, *(*col)))) {
          LOG_WARN("init non exist map item failed", K(ret), K(**col));
        }
      } else {
        const ObString& n = base_col->get_column_name();
        // quote column name with ``
        char* buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(n.length() + 2)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          buf[0] = '`';
          MEMCPY(buf + 1, n.ptr(), n.length());
          buf[n.length() + 1] = '`';
          item.base_col_name_ = ObString(n.length() + 2, buf);
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(set_convert_func(item.convert_func_, **col, *base_col))) {
          LOG_WARN("set object convert function failed", K(ret), K(**col), K(*base_col));
        }
      }
    }
  }
  return ret;
}

int ObAgentTableBase::construct_sql(common::ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_) || OB_ISNULL(base_table_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table is NULL", K(ret));
  } else {
    int64_t rest_time = scan_param_->timeout_ - ObTimeUtility::current_time();
    int64_t query_timeout = 1;
    for (; rest_time > 0;) {
      query_timeout = query_timeout * 10;
      rest_time = rest_time / 10;
    }

    if (OB_FAIL(sql.assign_fmt("SELECT /*+ query_timeout(%ld) */ ", query_timeout))) {
      LOG_WARN("append sql failed", K(ret));
    }
    FOREACH_CNT_X(c, scan_param_->column_ids_, OB_SUCC(ret))
    {
      const ObString& name = mapping_.at(*c).base_col_name_;
      if (OB_FAIL(sql.append_fmt("%s%.*s", 0 == __INNER_I__(c) ? "" : ", ", name.length(), name.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(" FROM `%s` WHERE 1=1", base_table_->get_table_name()))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(append_sql_condition(sql))) {
      LOG_WARN("append condition failed", K(ret));
    } else if (OB_FAIL(add_extra_condition(sql))) {
      LOG_WARN("append extra condition failed", K(ret));
    } else if (OB_FAIL(append_sql_orderby(sql))) {
      LOG_WARN("append order by failed", K(ret));
    }
  }
  return ret;
}

int ObAgentTableBase::append_sql_condition(common::ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  const ObRangeArray& ranges = scan_param_->key_ranges_;
  const ObRowkeyInfo& info = index_table_->get_rowkey_info();
  if (OB_ISNULL(index_table_) || OB_ISNULL(scan_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*index_table_), K(*scan_param_), K(info), K(sql));
  } else if (info.get_size() > 0) {
    // Reporting error, because descending index can not use row comparison,it is difficult
    // to convert the key range to sql condition for descending index,and we have no
    // descending indexed sys table right now.
    for (int64_t i = 0; OB_SUCC(ret) && i < info.get_size(); i++) {
      if (DESC == info.get_column(i)->order_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("descending index not supported", K(ret), K(info));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (info.get_size() <= 0) {
    // no keys, it will not add contitions. like __tenant_virtual_session_variable, no primary key
    LOG_DEBUG("no row keys", K(ret), K(info));
  } else if (ranges.count() == 1 && ranges.at(0).start_key_.is_max_row() && ranges.at(0).end_key_.is_min_row()) {
    if (OB_FAIL(sql.append(" AND 0=1 "))) {
      LOG_WARN("sql append failed", K(ret));
    }
  } else {
    ObSqlString cols;
    ObSqlString vals;
    bool cond_added = false;
    FOREACH_CNT_X(r, ranges, OB_SUCC(ret))
    {
      LOG_DEBUG("convert range to condition", K(*r));
      if (r->table_id_ != index_table_->get_table_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index table id", K(*r), K(*index_table_));
      } else if (!r->start_key_.is_valid() || !r->end_key_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey is invalid", K(ret));
      } else {
        const ObRowkey* rowkeys[] = {&r->start_key_, &r->end_key_};
        const ObObj& last_start_obj = r->start_key_.ptr()[r->start_key_.length() - 1];
        const ObObj& last_end_obj = r->end_key_.ptr()[r->end_key_.length() - 1];
        const bool full_start_key =
            r->start_key_.length() == info.get_size() && !(last_start_obj.is_ext() || last_start_obj.is_null());
        const bool full_end_key =
            r->end_key_.length() == info.get_size() && !(last_end_obj.is_ext() || last_end_obj.is_null());
        const char* ops[] = {full_start_key && !r->border_flag_.inclusive_start() ? ">" : ">=",
            full_end_key && !r->border_flag_.inclusive_end() ? "<" : "<="};
        static_assert(ARRAYSIZEOF(rowkeys) == ARRAYSIZEOF(ops), "array size mismatch");
        bool start_range_added = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(rowkeys); i++) {
          if (OB_FAIL(cols.set_length(0)) || OB_FAIL(vals.set_length(0))) {
            LOG_WARN("reset string failed", K(ret));
          } else if (OB_FAIL(setup_inital_rowkey_condition(cols, vals))) {
            LOG_WARN("setup initial rowkey condition failed", K(ret));
          } else if (OB_FAIL(rowkey2condition(cols, vals, *rowkeys[i], info))) {
            LOG_WARN("convert rowkey to scan condition failed", K(ret), K(rowkeys[i]));
          } else if (cols.empty()) {
            // min start key or max end key, do nothing.
          } else if (vals.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("empty condition values returned", K(ret));
          } else if (OB_FAIL(sql.append_fmt("%s(%.*s) %s (%.*s)",
                         cond_added ? (start_range_added ? " AND " : " OR ") : " AND (",
                         static_cast<int>(cols.length()),
                         cols.ptr(),
                         ops[i],
                         static_cast<int>(vals.length()),
                         vals.ptr()))) {
            LOG_WARN("sql append failed", K(ret));
          } else {
            start_range_added = 0 == i;
            cond_added = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && cond_added) {
      if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("sql append failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAgentTableBase::append_sql_orderby(common::ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_table_) || mapping_.empty() || sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(index_table_), K(mapping_.count()), K(sql));
  } else if (schema::USING_BTREE == index_table_->get_index_using_type()) {
    // Only btree index table are scanned in index ordering.
    const ObRowkeyInfo& info = index_table_->get_rowkey_info();
    ObRowkeyColumn col;
    for (int64_t i = base_rowkey_offset_; OB_SUCC(ret) && i < info.get_size(); i++) {
      if (OB_FAIL(info.get_column(i, col))) {
        LOG_WARN("get column failed", K(ret), K(i), K(info));
      } else {
        const ObString& name = mapping_[col.column_id_].base_col_name_;
        const ObOrderType scan_order = scan_flag_.is_reverse_scan() ? DESC : ASC;
        const char* order = scan_order == col.order_ ? "ASC" : "DESC";
        if (OB_FAIL(sql.append_fmt(
                "%s %.*s %s", base_rowkey_offset_ == i ? " ORDER BY" : ",", name.length(), name.ptr(), order))) {
          LOG_WARN("sql append failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAgentTableBase::rowkey2condition(common::ObSqlString& cols, common::ObSqlString& vals,
    const common::ObRowkey& rowkey, const common::ObRowkeyInfo& rowkey_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cols.set_length(0)) || OB_FAIL(vals.set_length(0))) {
    LOG_WARN("reuse sql string failed", K(ret));
  } else if (rowkey.get_obj_cnt() <= 0 || rowkey.get_obj_cnt() > rowkey_info.get_size() ||
             OB_ISNULL(rowkey.get_obj_ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey), K(rowkey_info));
  } else {
    for (int64_t i = base_rowkey_offset_; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); i++) {
      const ObObj& c = rowkey.ptr()[i];
      if (c.is_ext() || c.is_null()) {
        break;
      } else {
        uint64_t cid = 0;
        // 128 is enough for non-string type
        // string type need escape (* 2) and quoted (+ 2).
        const int64_t buf_len = std::max(128, c.is_string_type() ? (c.val_len_ * 2 + 2) : 2);
        if (!cols.empty()) {
          if (OB_FAIL(cols.append(", ")) || OB_FAIL(vals.append(", "))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
        convert_alloc_.reuse();
        int64_t val_pos = vals.length();
        ObObj new_value = c;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rowkey_info.get_column_id(i, cid))) {
          LOG_WARN("get column id failed", K(ret), K(i), K(rowkey_info));
        } else if (OB_FAIL(cols.append(mapping_[cid].base_col_name_))) {
          LOG_WARN("sql append failed", K(ret));
        } else if (OB_FAIL(vals.reserve(vals.length() + buf_len))) {
          LOG_WARN("reserve buffer failed", K(ret), K(buf_len));
        } else if (OB_FAIL(change_column_value(mapping_[cid], convert_alloc_, new_value))) {
          LOG_WARN("fail to change column value", K(ret), K(new_value));
        } else if (OB_FAIL(new_value.print_sql_literal(vals.ptr(), vals.capacity(), val_pos))) {
          LOG_WARN("print obj to sql literal failed", K(ret), K(c));
        } else if (OB_FAIL(vals.set_length(val_pos))) {
          LOG_WARN("set sql string length failed", K(ret), K(val_pos), K(vals));
        }
      }
    }
  }
  return ret;
}

int ObAgentTableBase::set_convert_func(
    convert_func_t& func, const share::schema::ObColumnSchemaV2&, const share::schema::ObColumnSchemaV2&)
{
  func = NULL;
  return OB_SUCCESS;
}

int ObAgentTableBase::add_extra_condition(common::ObSqlString&)
{
  return OB_SUCCESS;
}

int ObAgentTableBase::setup_inital_rowkey_condition(common::ObSqlString&, common::ObSqlString&)
{
  return OB_SUCCESS;
}

int ObAgentTableBase::change_column_value(const MapItem& item, ObIAllocator& allocator, ObObj& new_value)
{
  UNUSED(item);
  UNUSED(new_value);
  UNUSED(allocator);
  return OB_SUCCESS;
}

}  // end namespace observer
}  // end namespace oceanbase
