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

#include "ob_inner_sql_result.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/rc/context.h"
#include "lib/signal/ob_signal_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_req_time_service.h"
#include "omt/ob_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace sql;

namespace observer
{
inline int ObInnerSQLResult::check_extend_value(const common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (obj.is_null()) {
    ret = OB_ERR_NULL_VALUE;
  } else if (obj.is_min_value()) {
    ret = OB_ERR_MIN_VALUE;
  } else if (obj.is_max_value()) {
    ret = OB_ERR_MIN_VALUE;
  }
  return ret;
}

ObInnerSQLResult::ObInnerSQLResult(ObSQLSessionInfo &session)
    : column_map_created_(false), column_indexed_(false), column_map_(),
      mem_context_(nullptr),
      mem_context_destroy_guard_(mem_context_),
      sql_ctx_(), schema_guard_(share::schema::ObSchemaMgrItem::MOD_INNER_SQL_RESULT),
      opened_(false), session_(session),
      result_set_(nullptr), remote_result_set_(nullptr), row_(NULL),
      execute_start_ts_(0), execute_end_ts_(0),
      compat_mode_(ORACLE_MODE == session.get_compatibility_mode() ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL),
      is_inited_(false),
      store_first_row_(false),
      iter_end_(false),
      is_read_(true),
      has_tenant_resource_(true),
      tenant_(nullptr)

{
  sql_ctx_.exec_type_ = InnerSql;
}

int ObInnerSQLResult::init(bool has_tenant_resource)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(session_.get_effective_tenant_id(),
                     ObModIds::OB_RESULT_SET,
                     ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create memory entity failed", K(ret));
  } else if (has_tenant_resource) {
    if (OB_FAIL(GCTX.omt_->get_tenant_with_tenant_lock(session_.get_effective_tenant_id(), handle_, tenant_))) {
      if (OB_IN_STOP_STATE == ret) {
        ret = OB_TENANT_NOT_IN_SERVER;
      }
      LOG_WARN("get tenant lock fail", K(ret), K(session_.get_effective_tenant_id()));
    }
  }
  if (OB_SUCC(ret)) {
    set_has_tenant_resource(has_tenant_resource);
    if (!has_tenant_resource) {
      remote_result_set_ = new (buf_) ObRemoteResultSet(mem_context_->get_arena_allocator());
      remote_result_set_->reset_and_init_remote_resp_handler();
    } else {
      result_set_ = new (buf_) ObResultSet(session_, mem_context_->get_arena_allocator());
      result_set_->set_is_inner_result_set(true);
    }
    is_inited_ = true;
  }
  return ret;
}

int ObInnerSQLResult::init()
{
  return init(true);
}

ObInnerSQLResult::~ObInnerSQLResult()
{
  close();
  if (result_set_ != nullptr) {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    if (has_tenant_resource() && OB_FAIL(tenant_guard.switch_to(tenant_))) {
      LOG_WARN("switch tenant fail", K(ret));
    } else {
      result_set_->~ObResultSet();
    }
  }
  if (remote_result_set_ != nullptr) {
    remote_result_set_->~ObRemoteResultSet();
  }
  if (tenant_ != nullptr) {
    tenant_->unlock(handle_);
    tenant_ = nullptr;
  }
}

int ObInnerSQLResult::open()
{
  int ret = OB_SUCCESS;
  execute_start_ts_ = ObTimeUtility::current_time();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (has_tenant_resource()) {
    result_set().get_exec_context().set_plan_start_time(execute_start_ts_);
  }
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (has_tenant_resource() && OB_FAIL(tenant_guard.switch_to(tenant_))) {
    LOG_WARN("switch tenant failed", K(ret), K(session_.get_effective_tenant_id()));
  } else {
    lib::CompatModeGuard g(compat_mode_);
    SQL_INFO_GUARD(session_.get_current_query_string(), session_.get_cur_sql_id());
    bool is_select = has_tenant_resource() ?
           ObStmt::is_select_stmt(result_set_->get_stmt_type())
           : ObStmt::is_select_stmt(remote_result_set_->get_stmt_type());
    WITH_CONTEXT(mem_context_) {
      if (opened_) {
        ret = OB_INIT_TWICE;
        LOG_WARN("result set already open", K(ret));
      } else if (has_tenant_resource() && OB_FAIL(result_set_->open())) {
        result_set_->refresh_location_cache_by_errno(true, ret);
        LOG_WARN("open result set failed", K(ret));
        // move after precess_retry().
//        result_set_->close();
      } else if (is_read_&& is_select) {
        //prefetch 1 row for throwing error code and retry
        opened_ = true;
        if ((has_tenant_resource() && OB_FAIL(result_set_->get_next_row(row_)))
            || (!has_tenant_resource() && OB_FAIL(remote_result_set_->get_next_row(row_)))) {
          if (OB_ITER_END == ret) {
            iter_end_ = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get_next_row failed", K(ret), K(has_tenant_resource()));
          }
        } else {
          store_first_row_ = true;
        }
      } else {
        opened_ = true;
      }
    }
  }
  execute_end_ts_ = ObTimeUtility::current_time();
  return ret;
}

int ObInnerSQLResult::close()
{
  int ret = OB_SUCCESS;
  // close can be executed too if result set not open.
  if (opened_) {
    // opened=true imply is_inited=true
    if (OB_FAIL(inner_close())) {
      LOG_WARN("result set close failed", K(ret));
    }
  }
  column_map_.clear();
  column_indexed_ = false;
  return ret;
}

int ObInnerSQLResult::force_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_close())) {
    LOG_WARN("result set close failed", K(ret));
  }
  column_map_.clear();
  column_indexed_ = false;
  return ret;
}
int ObInnerSQLResult::inner_close()
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard g(compat_mode_);
  SQL_INFO_GUARD(session_.get_current_query_string(), session_.get_cur_sql_id());
  LOG_DEBUG("compat_mode_", K(ret), K(compat_mode_), K(lbt()));

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (has_tenant_resource() && OB_FAIL(tenant_guard.switch_to(tenant_))) {
    LOG_WARN("switch tenant failed", K(ret), K(session_.get_effective_tenant_id()));
  } else {
    WITH_CONTEXT(mem_context_) {
      if (has_tenant_resource() && OB_FAIL(result_set_->close())) {
        result_set_->refresh_location_cache_by_errno(true, ret);
        LOG_WARN("result set close failed", K(ret));
      } else if(!has_tenant_resource() && OB_FAIL(remote_result_set_->close())) {
        LOG_WARN("remote_result_set close failed", K(ret));
      }
    }
  }
  opened_ = false;
  store_first_row_ = false;
  iter_end_ = false;
  return ret;
}

int ObInnerSQLResult::next()
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  LOG_DEBUG("compat_mode_", K(ret), K(compat_mode_), K(lbt()));
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (has_tenant_resource() && OB_FAIL(tenant_guard.switch_to(tenant_))) {
    LOG_WARN("switch tenant failed", K(ret), K(session_.get_effective_tenant_id()));
  } else if (store_first_row_) {
    store_first_row_ = false;
  } else {
    row_ = NULL;
    lib::CompatModeGuard g(compat_mode_);
    SQL_INFO_GUARD(session_.get_current_query_string(), session_.get_cur_sql_id());
    WITH_CONTEXT(mem_context_) {
      if (has_tenant_resource() && OB_FAIL(result_set_->get_next_row(row_))) {
        if (OB_ITER_END != ret) {
          result_set_->refresh_location_cache_by_errno(true, ret);
          LOG_WARN("get next row failed", K(ret));
        }
      } else if (!has_tenant_resource() && OB_FAIL(remote_result_set_->get_next_row(row_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed",
                   K(ret), K(remote_result_set_->get_field_columns()->count()));
        }
      }

    }
  }
  return ret;
}

int ObInnerSQLResult::build_column_map() const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (!column_map_created_) {
    if (OB_FAIL(column_map_.create(COLUMN_MAP_BUCKET_NUM,
        ObModIds::OB_HASH_BUCKET_SQL_COLUMN_MAP, ObModIds::OB_HASH_NODE_SQL_COLUMN_MAP))) {
      LOG_WARN("create hash table failed", K(ret), LITERAL_K(COLUMN_MAP_BUCKET_NUM));
    } else {
      column_map_created_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    column_map_.clear();
    const ColumnsFieldIArray *fields = has_tenant_resource()
                                       ? result_set_->get_field_columns()
                                           : remote_result_set_->get_field_columns();
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(fields));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fields->count(); ++i) {
      if (nullptr == fields->at(i).cname_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("null field name", K(ret), "field", fields->at(i));
      } else {
        if (OB_FAIL(column_map_.set_refactored(fields->at(i).cname_, i))) {
          // ignore duplicate column name
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("add column name to column map failed", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    column_indexed_ = true;
  }

  return ret;
}

int ObInnerSQLResult::find_idx(const char *col_name, int64_t &idx) const
{
  idx = OB_INVALID_INDEX;
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (nullptr == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(col_name));
  } else {
    if (OB_UNLIKELY(!column_indexed_)) {
      if (OB_FAIL(build_column_map())) {
        LOG_WARN("build column map failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ret = column_map_.get_refactored(ObString::make_string(col_name), idx);
      if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get index from hash failed", K(ret), K(col_name));
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_INVALID_INDEX == idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index", K(ret), K(idx));
      }
    }
  }
  return ret;
}

#define DEF_GET_VALUE_BY_INDEX_IMPL(name, obj_name, type)                                     \
  int ObInnerSQLResult::name(const int64_t col_idx, type &val) const                          \
  {                                                                                           \
    int ret = OB_SUCCESS;                                                                     \
    if (!opened_) {                                                                           \
      ret = OB_NOT_INIT;                                                                      \
      LOG_WARN("not opened", K(ret));                                                         \
    } else if (NULL == row_) {                                                                \
      ret = OB_ERR_UNEXPECTED;                                                                \
      LOG_WARN("row is null", K(ret));                                                        \
    } else if (col_idx                                                                        \
        >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_)) {               \
      ret = OB_SIZE_OVERFLOW;                                                                 \
      LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);                    \
    } else {                                                                                  \
      const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);   \
      if (idx < 0 || idx >= row_->count_) {                                                   \
        ret = OB_ERR_UNEXPECTED;                                                              \
        LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);                \
      } else {                                                                                \
        const ObObj &obj = row_->cells_[idx];                                                 \
        if (OB_FAIL(check_extend_value(obj))) {                                               \
          LOG_DEBUG("check extend value failed", K(ret));                                     \
        } else if (OB_FAIL(obj.obj_name(val))) {                                              \
          LOG_WARN("get "#type" value from obj failed", K(ret), K(obj), K(obj.get_meta().get_type()), K(col_idx), K_(row)); \
        }                                                                                     \
      }                                                                                       \
    }                                                                                         \
    return ret;                                                                               \
  }

#define DEF_GET_VALUE_BY_INDEX(name, type) DEF_GET_VALUE_BY_INDEX_IMPL(name, name, type)

//DEF_GET_VALUE_BY_INDEX(get_int, int64_t);
DEF_GET_VALUE_BY_INDEX_IMPL(get_uint, get_uint64, uint64_t);
DEF_GET_VALUE_BY_INDEX(get_datetime, int64_t);
DEF_GET_VALUE_BY_INDEX(get_date, int32_t);
DEF_GET_VALUE_BY_INDEX(get_time, int64_t);
DEF_GET_VALUE_BY_INDEX(get_year, uint8_t);

int ObInnerSQLResult::get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo *tz_info,
    int64_t &val) const
{
  UNUSED(tz_info);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_UNLIKELY(NULL == row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (OB_UNLIKELY(col_idx >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= row_->count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      const ObObj &obj = row_->cells_[idx];
      if (OB_FAIL(check_extend_value(obj))) {
        LOG_DEBUG("check extend value failed", K(ret));
      } else {
        val = obj.get_timestamp();
      }
    }
  }
  return ret;
}

int ObInnerSQLResult::get_otimestamp_value(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    const common::ObObjType type, common::ObOTimestampData &otimestamp_val) const
{
  UNUSED(tz_info);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_UNLIKELY(NULL == row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (OB_UNLIKELY(col_idx >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= row_->count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      const ObObj &obj = row_->cells_[idx];
      if (OB_UNLIKELY(type != obj.get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is mismatch", K(type), K(obj), K(ret));
      } else if (OB_FAIL(check_extend_value(obj))) {
        LOG_DEBUG("check extend value failed", K(ret));
      } else {
        otimestamp_val = obj.get_otimestamp_value();
      }
    }
  }
  return ret;
}

int ObInnerSQLResult::get_timestamp_tz(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_idx, tz_info, ObTimestampTZType, otimestamp_val);
}

int ObInnerSQLResult::get_timestamp_ltz(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_idx, tz_info, ObTimestampLTZType, otimestamp_val);
}

int ObInnerSQLResult::get_timestamp_nano(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_idx, tz_info, ObTimestampNanoType, otimestamp_val);
}

DEF_GET_VALUE_BY_INDEX(get_interval_ym, ObIntervalYMValue);
DEF_GET_VALUE_BY_INDEX(get_interval_ds, ObIntervalDSValue);

int ObInnerSQLResult::get_bool(const int64_t col_idx, bool &bool_val) const
{
  int64_t v = 0;
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_INVALID_INDEX == col_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_idx));
  } else if (OB_FAIL(get_int(col_idx, v))) {
    LOG_WARN("get int value failed", K(ret), K(col_idx));
  } else {
    bool_val = v;
  }
  return ret;
}

DEF_GET_VALUE_BY_INDEX(get_varchar, ObString);
DEF_GET_VALUE_BY_INDEX(get_raw, ObString);
DEF_GET_VALUE_BY_INDEX(get_nvarchar2, ObString);
DEF_GET_VALUE_BY_INDEX(get_nchar, ObString);
DEF_GET_VALUE_BY_INDEX(get_float, float);
DEF_GET_VALUE_BY_INDEX(get_double, double);

int ObInnerSQLResult::get_int(const int64_t col_idx, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  const ObObj *obj = NULL;
  if (OB_FAIL(get_obj(col_idx, obj))) {
    LOG_WARN("get obj error", K(ret));
  } else if (OB_ISNULL(obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get a invalud obj", K(col_idx), K(obj), K(ret));
  } else if (OB_UNLIKELY(ObIntTC != obj->get_type_class())) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("invalid input type", K(ret), K(*obj));
  } else {
    int_val = obj->get_int();
  }
  return ret;
}

int ObInnerSQLResult::get_number_impl(const int64_t col_idx, number::ObNumber &ret_nmb) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (NULL == row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (col_idx >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (idx < 0 || idx >= row_->count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      const ObObj &obj = row_->cells_[idx];
      if (OB_FAIL(check_extend_value(obj))) {
        LOG_DEBUG("check extend value failed", K(ret));
      } else if (obj.is_decimal_int()) {
        if (OB_FAIL(wide::to_number(obj.get_decimal_int(), obj.get_int_bytes(), obj.get_scale(),
                                    mem_context_->get_arena_allocator(), ret_nmb))) {
          LOG_WARN("to_number failed", K(ret));
        }
      } else if (OB_FAIL(obj.get_number(ret_nmb))) {
        LOG_WARN("get number failed", K(ret));
      }
    }
  }
  return ret;
}

DEF_GET_VALUE_BY_INDEX_IMPL(get_urowid_impl, get_urowid, ObURowIDData);

#undef DEF_GET_VALUE_BY_INDEX
#undef DEF_GET_VALUE_BY_INDEX_IMPL

int ObInnerSQLResult::get_type(const int64_t col_idx, ObObjMeta &type) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (NULL == row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (col_idx
      >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (idx < 0 || idx >= row_->count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      const ObObj &obj = row_->cells_[idx];
      type = obj.get_meta();
    }
  }
  return ret;
}

int ObInnerSQLResult::get_col_meta(const int64_t col_idx, bool old_max_length,
                                   oceanbase::common::ObString &name, ObDataType &data_type) const
{
  UNUSEDx(col_idx, old_max_length, name, data_type);
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}

int64_t ObInnerSQLResult::get_column_count() const
{
  return -1;
}

int ObInnerSQLResult::get_obj(const int64_t col_idx, ObObj &obj,
                              const ObTimeZoneInfo *tz_info,
                              ObIAllocator *allocator) const
{
  UNUSED(tz_info);
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (NULL == row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (col_idx
      >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (idx < 0 || idx >= row_->count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      obj = row_->cells_[idx];
    }
  }
  return ret;
}

#define DEF_GET_VALUE_BY_NAME(name, type)                                        \
  int ObInnerSQLResult::name(const char *col_name, type &val) const              \
  {                                                                              \
    int ret = OB_SUCCESS;                                                        \
    int64_t idx = OB_INVALID_INDEX;                                              \
    if (!opened_) {                                                              \
      ret = OB_NOT_INIT;                                                         \
      LOG_WARN("not opened", K(ret));                                            \
    } else if (NULL == col_name) {                                               \
      ret = OB_INVALID_ARGUMENT;                                                 \
      LOG_WARN("column name can not be NULL", K(ret));                           \
    } else if (OB_FAIL(find_idx(col_name, idx))) {                               \
      if (OB_ENTRY_NOT_EXIST == ret) {                                           \
        ret = OB_ERR_COLUMN_NOT_FOUND;                                           \
      } else {                                                                   \
        LOG_WARN("find column index failed", K(ret), K(col_name));               \
      }                                                                          \
    } else if (OB_INVALID_INDEX == idx) {                                        \
      ret = OB_ERR_UNEXPECTED;                                                   \
      LOG_WARN("invalid index returned", K(ret), K(idx));                        \
    } else if (OB_FAIL(name(idx, val))) {                                        \
      if (OB_ERR_NULL_VALUE != ret) {                                            \
        LOG_WARN(#name" failed", K(ret), K(idx));                                \
      }                                                                          \
    }                                                                            \
    return ret;                                                                  \
  }

DEF_GET_VALUE_BY_NAME(get_int, int64_t);
DEF_GET_VALUE_BY_NAME(get_uint, uint64_t);
DEF_GET_VALUE_BY_NAME(get_datetime, int64_t);
DEF_GET_VALUE_BY_NAME(get_date, int32_t);
DEF_GET_VALUE_BY_NAME(get_time, int64_t);
DEF_GET_VALUE_BY_NAME(get_year, uint8_t);
DEF_GET_VALUE_BY_NAME(get_bool, bool);
DEF_GET_VALUE_BY_NAME(get_varchar, ObString);
DEF_GET_VALUE_BY_NAME(get_raw, ObString);
DEF_GET_VALUE_BY_NAME(get_float, float);
DEF_GET_VALUE_BY_NAME(get_double, double);
DEF_GET_VALUE_BY_NAME(get_number_impl, number::ObNumber);
DEF_GET_VALUE_BY_NAME(get_type, ObObjMeta);
DEF_GET_VALUE_BY_NAME(get_obj, ObObj);
DEF_GET_VALUE_BY_NAME(get_interval_ym, ObIntervalYMValue);
DEF_GET_VALUE_BY_NAME(get_interval_ds, ObIntervalDSValue);
DEF_GET_VALUE_BY_NAME(get_nvarchar2, ObString);
DEF_GET_VALUE_BY_NAME(get_nchar, ObString);
DEF_GET_VALUE_BY_NAME(get_urowid_impl, ObURowIDData);
#undef DEF_GET_VALUE_BY_NAME

int ObInnerSQLResult::get_timestamp(const char *col_name,  const common::ObTimeZoneInfo *tz_info,
    int64_t &val) const
{
  UNUSED(tz_info);
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_UNLIKELY(NULL == col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column name can not be NULL", K(ret));
  } else if (OB_FAIL(find_idx(col_name, idx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    } else {
      LOG_WARN("find column index failed", K(ret), K(col_name));
    }
  } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index returned", K(ret), K(idx));
  } else if (OB_FAIL(get_timestamp(idx, tz_info, val))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get_timestamp failed", K(ret), K(idx));
    }
  }
  return ret;
}

int ObInnerSQLResult::get_otimestamp_value(const char *col_name,  const common::ObTimeZoneInfo &tz_info,
    const common::ObObjType type, common::ObOTimestampData &otimestamp_val) const
{
  UNUSED(tz_info);
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_UNLIKELY(NULL == col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column name can not be NULL", K(ret));
  } else if (OB_FAIL(find_idx(col_name, idx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    } else {
      LOG_WARN("find column index failed", K(ret), K(col_name));
    }
  } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index returned", K(ret), K(idx));
  } else if (OB_FAIL(get_otimestamp_value(idx, tz_info, type, otimestamp_val))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get_otimestamp failed", K(ret), K(idx));
    }
  }
  return ret;
}

int ObInnerSQLResult::get_timestamp_tz(const char *col_name,  const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_name, tz_info, ObTimestampTZType, otimestamp_val);
}

int ObInnerSQLResult::get_timestamp_ltz(const char *col_name,  const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_name, tz_info, ObTimestampLTZType, otimestamp_val);
}

int ObInnerSQLResult::get_timestamp_nano(const char *col_name,  const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_name, tz_info, ObTimestampNanoType, otimestamp_val);
}

int ObInnerSQLResult::get_number(const int64_t col_idx,
                                 common::number::ObNumber &nmb_val) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_INVALID_INDEX == col_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_idx));
  } else if (OB_FAIL(get_number_impl(col_idx, nmb_val))) {
    LOG_WARN("get number impl failed", K(ret), K(col_idx));
  }
  return ret;
}

int ObInnerSQLResult::get_number(const char *col_name, common::number::ObNumber &nmb_val) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(col_name));
  } else if (OB_FAIL(get_number_impl(col_name, nmb_val))) {
    LOG_WARN("get number impl failed", K(ret), K(col_name));
  }
  return ret;
}

int ObInnerSQLResult::inner_get_number(const int64_t col_idx, number::ObNumber &nmb_val,
    IAllocator &allocator) const
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_INVALID_INDEX == col_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_idx));
  } else if (OB_FAIL(get_number_impl(col_idx, nmb_val))) {
    LOG_WARN("get number impl failed", K(ret), K(col_idx));
  }
  return ret;
}

int ObInnerSQLResult::inner_get_number(const char *col_name, number::ObNumber &nmb_val,
    IAllocator &allocator) const
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(col_name));
  } else if (OB_FAIL(get_number_impl(col_name, nmb_val))) {
    LOG_WARN("get number impl failed", K(ret), K(col_name));
  }
  return ret;
}

int ObInnerSQLResult::inner_get_urowid(const int64_t col_idx,
                                       ObURowIDData &urowid_data,
                                       ObIAllocator &/* not used */) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_FAIL(get_urowid_impl(col_idx, urowid_data))) {
    LOG_WARN("failed to get urowid", K(ret));
  }
  return ret;
}

int ObInnerSQLResult::inner_get_urowid(const char* col_name,
                                       ObURowIDData &urowid_data,
                                       ObIAllocator &/* no used */) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_FAIL(get_urowid_impl(col_name, urowid_data))) {
    LOG_WARN("failed to get urowid data", K(ret));
  }
  return ret;
}

int ObInnerSQLResult::get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_lob_locator_impl(col_idx, lob_locator))) {
    LOG_WARN("get lob locator impl failed", K(ret), K(col_idx));
  }
  return ret;
}

int ObInnerSQLResult::get_lob_locator(const char *col_name, ObLobLocator *&lob_locator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_lob_locator_impl(col_name, lob_locator))) {
    LOG_WARN("get lob locator impl failed", K(ret), K(col_name));
  }
  return ret;
}

int ObInnerSQLResult::inner_get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator,
      ObIAllocator &allocator) const
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_lob_locator_impl(col_idx, lob_locator))) {
    LOG_WARN("get lob locator impl failed", K(ret), K(col_idx));
  }
  return ret;
}

int ObInnerSQLResult::inner_get_lob_locator(const char *col_name, ObLobLocator *&lob_locator,
      ObIAllocator &allocator) const
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_lob_locator_impl(col_name, lob_locator))) {
    LOG_WARN("get lob locator impl failed", K(ret), K(col_name));
  }
  return ret;
}

int ObInnerSQLResult::get_lob_locator_impl(const int64_t col_idx, ObLobLocator *&lob_locator) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (OB_ISNULL(row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (OB_INVALID_INDEX == col_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_idx));
  } else if (col_idx >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (idx < 0 || idx >= row_->count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      const ObObj &obj = row_->cells_[idx];
      if (OB_FAIL(check_extend_value(obj))) {
        LOG_DEBUG("check extend value failed", K(ret));
      } else if (OB_FAIL(obj.get_lob_locator(lob_locator))) {
        LOG_WARN("get lob locator value from obj failed", K(ret), K(obj), K(col_idx), K_(row));
      }
    }
  }
  return ret;
}

int ObInnerSQLResult::get_lob_locator_impl(const char *col_name, ObLobLocator *&lob_locator) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(col_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column name can not be NULL", K(ret));
    } else if (OB_FAIL(find_idx(col_name, idx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
      } else {
        LOG_WARN("find column index failed", K(ret), K(col_name));
      }
    } else if (OB_INVALID_INDEX == idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index returned", K(ret), K(idx));
    } else if (OB_FAIL(get_lob_locator_impl(idx, lob_locator))) {
      if (OB_ERR_NULL_VALUE != ret) {
        LOG_WARN("inner get lob locator failed", K(ret), K(idx));
      }
    }
  }
  return ret;
}

int ObInnerSQLResult::get_obj(const int64_t col_idx, const common::ObObj *&result) const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else if (NULL == row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (col_idx
      >= (NULL == row_->projector_ ? row_->count_ : row_->projector_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("column index overflow", K(ret), K(col_idx), "row", *row_);
  } else {
    const int64_t idx = (NULL != row_->projector_ ? row_->projector_[col_idx] : col_idx);
    if (idx < 0 || idx >= row_->count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "cell count", row_->count_);
    } else {
      const ObObj &obj = row_->cells_[idx];
      if (OB_FAIL(check_extend_value(obj))) {
        LOG_DEBUG("check extend value failed", K(ret));
      } else {
        result = &obj;
      }
    }
  }
  return ret;
}

int ObInnerSQLResult::print_info() const
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not opened", K(ret));
  } else {
    LOG_INFO("result", K_(result_set));
  }
  return ret;
}
} // end of namespace observer
} // end of namespace oceanbase
