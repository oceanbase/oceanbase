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

#define USING_LOG_PREFIX RS

#include "rootserver/ob_schema_split_executor.h"
#include "common/object/ob_obj_compare.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver {
static int filter_tenant_id(const ObObj& src, ObObj& dst, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  dst = src;
  if (src.is_null()) {
    // do nothing
  } else if (!src.is_int() && !src.is_uint64()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported obj type", K(ret), K(src));
  } else if (src.is_int() && src.get_int() > 0) {
    dst.set_int_value(static_cast<int64_t>(OB_INVALID_TENANT_ID));
  } else if (src.is_uint64() && 0 != src.get_uint64()) {
    dst.set_uint64_value(OB_INVALID_TENANT_ID);
  }
  return ret;
}

static int filter_column_tenant_id(const ObObj& src, ObObj& dst, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  dst = src;
  if (src.is_null()) {
    // do nothing
  } else if (!src.is_int() && !src.is_uint64() && !src.is_varchar_or_char()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported obj type", K(ret), K(src));
  } else if (src.is_int() && src.get_int() > 0) {
    dst.set_int_value(static_cast<int64_t>(extract_pure_id(src.get_int())));
  } else if (src.is_uint64() && 0 != src.get_uint64()) {
    dst.set_uint64_value(extract_pure_id(src.get_uint64()));
  } else if (src.is_varchar_or_char()) {
    // __all_sys_stat column `value`
    int64_t value = OB_INVALID_ID;
    if (OB_FAIL(ObSchemaUtils::str_to_int(src.get_string(), value))) {
      LOG_WARN("fail to covert str to int", K(ret), K(src));
    } else if (value > 0) {
      value = extract_pure_id(value);
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
        dst.set_char_value(buf, static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  return ret;
}

// Because system variable schema is attached to tenant schema before ver 2.2.0,
// we can construct system varaible's ddl operation from tenant's ddl operation while schema split.
static int convert_operation_type(const ObObj& src, ObObj& dst, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  dst = src;
  if (src.is_null() || !src.is_int()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src", K(ret), K(src));
  } else if (OB_DDL_ALTER_SYS_VAR == src.get_int()) {
    // Ignore system variable's new ddl operation generated in in upgrade mode.
  } else if (src.get_int() <= OB_DDL_TENANT_OPERATION_BEGIN || src.get_int() >= OB_DDL_TENANT_OPERATION_END) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation_type not match", K(ret), K(src));
  } else {
    dst.set_int_value(OB_DDL_ALTER_SYS_VAR);
  }
  return ret;
}

static int deep_copy_str_for_spliter(const ObObj& src, ObObj& dst, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  dst = src;
  if (src.is_null()) {
    // do nothing
  } else if (OB_FAIL(ob_write_obj(allocator, src, dst))) {
    LOG_WARN("fail to write obj", K(ret), K(src));
  } else if (ob_is_string_type(src.get_type()) && 0 == dst.get_string_len()) {
    // NOTE::for empty varchar, will set ptr != NULL, len = 0.
    // spliter will use this obj for add_column
    const ObString SPLITER_EMPTY_STRING(0, 0, OB_EMPTY_STR);
    dst.set_common_value(SPLITER_EMPTY_STRING);
  }
  return ret;
}

int ObTableSchemaSpliter::ObSplitTableIterator::init(ObSqlString& sql, const uint64_t exec_tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (sql.empty() || OB_INVALID_TENANT_ID == exec_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql), K(exec_tenant_id));
  } else if (OB_FAIL(base_sql_.assign(sql.ptr()))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    start_idx_ = 0;
    cur_idx_ = 0;
    exec_tenant_id_ = exec_tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableSchemaSpliter::ObSplitTableIterator::destroy()
{}

int ObTableSchemaSpliter::ObSplitTableIterator::check_stop()
{
  return executor_.check_stop();
}

int ObTableSchemaSpliter::ObSplitTableIterator::get_next_row(const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (cur_idx_ < rows_.count()) {
    // do nothing
  } else if (OB_FAIL(get_next_batch())) {
    LOG_WARN("fail to get next batch", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (0 == rows_.count()) {
    ret = OB_ITER_END;
  } else {
    row = rows_.at(cur_idx_);
    cur_idx_++;
    start_idx_++;
  }
  return ret;
}

int ObTableSchemaSpliter::ObSplitTableIterator::get_next_batch()
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(sql_str.append_fmt("%s LIMIT %ld, %ld", base_sql_.ptr(), start_idx_, MAX_FETCH_ROW_COUNT))) {
    LOG_WARN("fail to construct sql", K(ret));
  } else {
    rows_.reuse();
    alloc_.reuse();
    cur_idx_ = 0;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      observer::ObInnerSQLResult* result = NULL;
      if (OB_ISNULL(sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("proxy is null", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, exec_tenant_id_, sql_str.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K_(exec_tenant_id), K(sql_str));
      } else if (OB_ISNULL(result = static_cast<observer::ObInnerSQLResult*>(res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL sql executing result", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          const ObNewRow* cur_row = NULL;
          ObNewRow* new_row = NULL;
          if (OB_FAIL(check_stop())) {
            LOG_WARN("executor is stopped", K(ret));
          } else if (OB_ISNULL(cur_row = result->get_row())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row is null", K(ret));
          } else if (OB_FAIL(ob_create_row(alloc_, cur_row->get_count(), new_row))) {
            LOG_WARN("fail to create row", K(ret), KPC(cur_row));
          } else if (OB_FAIL(ob_write_row(alloc_, *cur_row, *new_row))) {
            LOG_WARN("fail to write row", K(ret), KPC(cur_row));
          } else if (OB_FAIL(rows_.push_back(new_row))) {
            LOG_WARN("fail to push back row", K(ret));
          }
        }
        if (OB_ITER_END != ret) {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("fail to iterate row", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTableSchemaSpliter::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(build_column_mapping())) {
    LOG_WARN("fail to build column_mapping", K(ret));
  } else if (OB_FAIL(construct_base_sql())) {
    LOG_WARN("fail to construct base sql", K(ret));
  } else if (OB_FAIL(add_extra_condition())) {
    LOG_WARN("fail to add extra condition", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableSchemaSpliter::build_column_mapping()
{
  int ret = OB_SUCCESS;
  mapping_.set_block_allocator(ObWrapperAllocator(allocator_));
  ObColumnIterByPrevNextID iter(table_schema_);
  const ObColumnSchemaV2* column_schema = NULL;
  while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
    if (OB_FAIL(check_stop())) {
      LOG_WARN("executor is stopped", K(ret));
    } else if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is null", K(ret));
    } else if (column_schema->is_shadow_column()) {
      // don't show shadow columns for select * from idx
      continue;
    } else if (column_schema->is_hidden()) {
      // jump hidden column
      continue;
    } else if (0 == column_schema->get_column_name_str().case_compare("gmt_modified") ||
               0 == column_schema->get_column_name_str().case_compare("gmt_create")) {
      continue;
    } else {
      MapItem item;
      const ObString& column_name = column_schema->get_column_name();
      char* buf = NULL;
      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(column_name.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else {
        MEMCPY(buf, column_name.ptr(), column_name.length() + 1);
        buf[column_name.length()] = '\0';
        item.base_col_name_ = ObString(column_name.length() + 1, buf);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_convert_func(*column_schema, item))) {
          LOG_WARN("fail to add convert_func", K(ret), KPC(column_schema), K(item));
        } else if (OB_FAIL(mapping_.push_back(item))) {
          LOG_WARN("fail to push back item", K(ret), K(item));
        }
      }
    }
  }
  if (ret != OB_ITER_END) {
    LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTableSchemaSpliter::add_convert_func(const ObColumnSchemaV2& column_schema, MapItem& item)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_schema_.get_table_id();
  item.convert_func_ = deep_copy_str_for_spliter;
  if (ObSysTableChecker::is_cluster_private_tenant_table(table_id)) {
    // tenant space table which can write in stanby cluster, no need to filter tenant_id
  } else if (column_name_equal(item.base_col_name_, "tenant_id")) {
    item.convert_func_ = filter_tenant_id;
    LOG_INFO("set convertor", K(ret), K(column_schema.get_column_name()));
  } else if (OB_ALL_DDL_OPERATION_TID == extract_pure_id(column_schema.get_table_id()) &&
             column_name_equal(item.base_col_name_, "exec_tenant_id")) {
    item.convert_func_ = filter_tenant_id;
    LOG_INFO("set convertor", K(ret), K(column_schema.get_column_name()));
  }
  return ret;
}

bool ObTableSchemaSpliter::column_name_equal(const ObString& column_name, const char* target)
{
  ObString tmp_str(column_name.length() - 1, column_name.ptr());
  return 0 == tmp_str.compare(target);
}

int ObTableSchemaSpliter::construct_base_sql()
{
  int ret = OB_SUCCESS;
  base_sql_.reset();
  if (mapping_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count", K(ret), "count", mapping_.count());
  } else if (base_sql_.assign_fmt("SELECT ")) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    FOREACH_CNT_X(item, mapping_, OB_SUCC(ret))
    {
      const ObString& name = item->base_col_name_;
      if (OB_FAIL(base_sql_.append_fmt("%s%.*s", 0 == __INNER_I__(item) ? "" : ", ", name.length(), name.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(base_sql_.append_fmt(" FROM %s WHERE 1=1", table_schema_.get_table_name()))) {
      LOG_WARN("fail to append sql", K(ret));
    }
  }
  return ret;
}

int ObTableSchemaSpliter::set_column_name_with_tenant_id(const char* column_name)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  bool finded = false;
  FOREACH_CNT_X(item, mapping_, OB_SUCC(ret))
  {
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is null", K(ret));
    } else if (column_name_equal(item->base_col_name_, column_name)) {
      item->convert_func_ = filter_column_tenant_id;
      finded = true;
      LOG_INFO("set convertor", K(ret), K(column_name));
      break;
    }
  }
  if (OB_SUCC(ret) && !finded) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_name not match", K(ret), "column_name", column_name);
  }
  return ret;
}

int ObTableSchemaSpliter::quick_check(const uint64_t tenant_id, bool& passed)
{
  int ret = OB_SUCCESS;
  int64_t row_count_from_sys = OB_INVALID_ID;
  int64_t row_count_from_tenant = OB_INVALID_ID;
  passed = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                                        "WHERE tenant_id = %ld %s",
                     table_schema_.get_table_name(),
                     tenant_id,
                     extra_condition_.empty() ? "" : extra_condition_.ptr()))) {
        LOG_WARN("fail to assgin sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "count", row_count_from_sys, int64_t);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                                        "WHERE tenant_id = %ld %s",
                     table_schema_.get_table_name(),
                     OB_INVALID_TENANT_ID,
                     extra_condition_.empty() ? "" : extra_condition_.ptr()))) {
        LOG_WARN("fail to assgin sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "count", row_count_from_tenant, int64_t);
      }
    }
  }
  if (OB_SUCC(ret)) {
    passed = (row_count_from_sys == row_count_from_tenant);
  }
  return ret;
}

int ObTableSchemaSpliter::process()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObSchemaGetterGuard schema_guard;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (!schema_service_.is_tenant_full_schema(OB_SYS_TENANT_ID)) {
    ret = OB_EAGAIN;
    LOG_WARN("fail to check if tenant is full schema", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", K(ret));
  } else if (tenant_ids.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant count", K(ret), "count", tenant_ids.count());
  } else {
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret))
    {
      if (OB_FAIL(process(*tenant_id))) {
        LOG_WARN("process failed", K(ret), K(*tenant_id));
      }
    }
  }
  return ret;
}

int ObTableSchemaSpliter::process(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    bool passed = false;
    if (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id) {
      LOG_INFO("skip sys tenant", K(ret));
    } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
      LOG_INFO("standby cluster skip normal tenant", K(ret), K(tenant_id));
    } else if (OB_FAIL(quick_check(tenant_id, passed))) {
      LOG_WARN("quick check failed", K(ret), K(tenant_id), "table_name", table_schema_.get_table_name());
    } else if (passed) {
      LOG_INFO("row count equal, just passed", K(tenant_id), "table_name", table_schema_.get_table_name());
    } else {
      // step 1: migrate data
      if (OB_SUCC(ret)) {
        LOG_INFO("[UPGRADE] start to migrate data", K(ret), K(tenant_id), "table_name", table_schema_.get_table_name());
        int64_t start = ObTimeUtility::current_time();
        if (OB_FAIL(migrate(tenant_id))) {
          LOG_WARN("fail to migrate data", K(ret), "table_name", table_schema_.get_table_name());
        }
        int64_t end = ObTimeUtility::current_time();
        LOG_INFO("[UPGRADE] end migrate data",
            K(ret),
            K(tenant_id),
            "table_name",
            table_schema_.get_table_name(),
            "cost",
            end - start);
      }
      // step 2: check data
      if (OB_SUCC(ret)) {
        LOG_INFO("[UPGRADE] start to check data", K(ret), K(tenant_id), "table_name", table_schema_.get_table_name());
        int64_t start = ObTimeUtility::current_time();
        if (OB_FAIL(check(tenant_id))) {
          LOG_WARN("fail to check data", K(ret), "table_name", table_schema_.get_table_name());
        }
        int64_t end = ObTimeUtility::current_time();
        LOG_INFO("[UPGRADE] end check data",
            K(ret),
            K(tenant_id),
            "table_name",
            table_schema_.get_table_name(),
            "cost",
            end - start);
      }
    }
  }
  return ret;
}

int ObTableSchemaSpliter::init_sys_iterator(const uint64_t tenant_id, ObSplitTableIterator& sys_iter)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(construct_sql(tenant_id, sql))) {
    LOG_WARN("fail to add addition", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_iter.init(sql, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTableSchemaSpliter::init_tenant_iterator(const uint64_t tenant_id, ObSplitTableIterator& tenant_iter)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(construct_sql(OB_INVALID_TENANT_ID, sql))) {
    LOG_WARN("fail to add addition", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_iter.init(sql, tenant_id))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTableSchemaSpliter::migrate(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator batch_allocator;
  ObSplitTableIterator sys_iter(executor_, sql_proxy_);
  ObSEArray<ObNewRow*, BATCH_REPLACE_ROW_COUNT> replace_rows;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(init_sys_iterator(tenant_id, sys_iter))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  } else {
    const ObNewRow* orig_row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(sys_iter.get_next_row(orig_row))) {
      if (OB_ISNULL(orig_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret));
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (mapping_.count() != orig_row->get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cell count not match", K(ret), "map_count", mapping_.count(), "cell_count", orig_row->get_count());
      } else if (replace_rows.count() >= BATCH_REPLACE_ROW_COUNT) {
        if (OB_FAIL(batch_replace_rows(tenant_id, replace_rows))) {
          LOG_WARN("fail to replace row", K(ret), K(tenant_id), "row_count", replace_rows.count());
        } else {
          replace_rows.reset();
          batch_allocator.reset();
        }
      }
      if (OB_SUCC(ret)) {
        ObNewRow* new_row = NULL;
        if (OB_FAIL(ob_create_row(batch_allocator, mapping_.count(), new_row))) {
          LOG_WARN("fail to create row", K(ret));
        } else if (OB_ISNULL(new_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is null", K(ret));
        } else if (OB_FAIL(replace_rows.push_back(new_row))) {
          LOG_WARN("fail to push back row", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < orig_row->get_count(); i++) {
            const ObObj& input = orig_row->get_cell(i);
            const MapItem& item = mapping_[i];
            ObObj& output = new_row->get_cell(i);
            if (OB_FAIL(check_stop())) {
              LOG_WARN("executor is stopped", K(ret));
            } else if (input.is_null() || OB_ISNULL(item.convert_func_)) {
              output = input;
            } else if (OB_FAIL(item.convert_func_(input, output, batch_allocator))) {
              LOG_WARN("fail to convert data", K(ret), K(input));
            }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (replace_rows.count() > 0 && OB_FAIL(batch_replace_rows(tenant_id, replace_rows))) {
        LOG_WARN("fail to replace row", K(ret), K(tenant_id), "row_count", replace_rows.count());
      }
    } else {
      ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("iter row failed", K(ret), K(tenant_id), "table_name", table_schema_.get_table_name());
    }
  }
  return ret;
}

int ObTableSchemaSpliter::batch_replace_rows(const uint64_t tenant_id, ObIArray<ObNewRow*>& replace_rows)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (replace_rows.count() <= 0 || mapping_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "row_count", replace_rows.count(), "map_count", mapping_.count());
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    const int64_t start = ObTimeUtility::current_time();
    share::ObDMLSqlSplicer dml;
    for (int64_t i = 0; OB_SUCC(ret) && i < replace_rows.count(); i++) {
      const ObNewRow* row = replace_rows.at(i);
      if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret));
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (mapping_.count() != row->get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), "row_count", row->get_count(), "map_count", mapping_.count());
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < mapping_.count(); j++) {
          const ObString& column_name = mapping_[j].base_col_name_;
          const ObObj& obj = row->get_cell(j);
          if (OB_FAIL(check_stop())) {
            LOG_WARN("executor is stopped", K(ret));
          } else if (obj.is_null()) {
            if (OB_FAIL(dml.add_column(true /*is_null*/, column_name.ptr()))) {
              LOG_WARN("fail to add column", K(ret), K(obj), K(column_name));
            }
          } else {
            switch (obj.get_type()) {
              case ObTinyIntType: {  // bool
                if (OB_FAIL(dml.add_column(column_name.ptr(), obj.get_tinyint()))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              case ObDoubleType: {
                if (OB_FAIL(dml.add_column(column_name.ptr(), obj.get_double()))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              case ObNumberType: {
                // Only __all_sequence_object(_history), __all_sequence_value contains columns defined as numeric type.
                // And we can treat them like columns defined as Int64 type.
                int64_t value = OB_INVALID_ID;
                if (!obj.get_number().is_valid_int64(value)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("value range not match", K(ret), K(obj));
                } else if (OB_FAIL(dml.add_column(column_name.ptr(), value))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              case ObTimestampType: {
                if (OB_FAIL(dml.add_time_column(column_name.ptr(), obj.get_timestamp()))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              case ObVarcharType:
              case ObLongTextType: {
                if (OB_FAIL(dml.add_column(column_name.ptr(), ObHexEscapeSqlStr(obj.get_varchar())))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              case ObIntType: {
                if (OB_FAIL(dml.add_column(column_name.ptr(), obj.get_int()))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              case ObUInt64Type: {
                if (OB_FAIL(dml.add_column(column_name.ptr(), ObRealUInt64(obj.get_uint64())))) {
                  LOG_WARN("fail to add column", K(ret), K(column_name), K(obj));
                }
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid obj type", K(ret), K(column_name), K(obj));
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(dml.finish_row())) {
          LOG_WARN("fail to finish row", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("proxy is null", K(ret));
    } else if (OB_FAIL(check_stop())) {
      LOG_WARN("executor is stopped", K(ret));
    } else if (OB_FAIL(dml.splice_batch_replace_sql_without_plancache(table_schema_.get_table_name(), sql))) {
      LOG_WARN("splice sql failed", K(ret));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    }
    LOG_INFO("[UPGRADE] batch replace rows",
        K(ret),
        K(tenant_id),
        "table_name",
        table_schema_.get_table_name(),
        "row_count",
        replace_rows.count(),
        "affected_rows",
        affected_rows,
        "cost",
        ObTimeUtility::current_time() - start);
  }
  return ret;
}

int ObTableSchemaSpliter::check(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSplitTableIterator sys_iter(executor_, sql_proxy_);
  ObSplitTableIterator tenant_iter(executor_, sql_proxy_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(init_sys_iterator(tenant_id, sys_iter))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_iterator(tenant_id, tenant_iter))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  } else {
    const ObNewRow* sys_row = NULL;
    const ObNewRow* tenant_row = NULL;
    ObArenaAllocator row_allocator;
    while (OB_SUCC(ret) && OB_SUCC(sys_iter.get_next_row(sys_row))) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (OB_FAIL(tenant_iter.get_next_row(tenant_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_ISNULL(sys_row) || OB_ISNULL(tenant_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret), KPC(sys_row), KPC(tenant_row));
      } else if (sys_row->get_count() != tenant_row->get_count() || mapping_.count() != sys_row->get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cells num not match",
            K(ret),
            "sys_cells_count",
            sys_row->get_count(),
            "tenant_cells_count",
            tenant_row->get_count(),
            "map_cells_count",
            mapping_.count());
      } else {
        row_allocator.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < sys_row->get_count(); i++) {
          const ObObj& input = sys_row->get_cell(i);
          const ObObj& tenant_obj = tenant_row->get_cell(i);
          const MapItem& item = mapping_[i];
          ObObj output;
          if (OB_FAIL(check_stop())) {
            LOG_WARN("executor is stopped", K(ret));
          } else if (input.is_null() || OB_ISNULL(item.convert_func_)) {
            output = input;
          } else if (OB_FAIL(item.convert_func_(input, output, row_allocator))) {
            LOG_WARN("fail to convert data", K(ret), K(input));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(compare_obj(output, tenant_obj))) {
            LOG_WARN("fail to compare obj", K(ret), K(output), K(tenant_obj), K(i), KPC(sys_row), KPC(tenant_row));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      int temp_ret = tenant_iter.get_next_row(tenant_row);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_ERR_UNEXPECTED;  // overwrite ret;
        LOG_WARN("row count not match",
            K(ret),
            K(temp_ret),
            K(tenant_id),
            "table_name",
            table_schema_.get_table_name(),
            KPC(sys_row),
            KPC(tenant_row));
      }
    } else {
      ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("iter row failed", K(ret), K(tenant_id), "table_name", table_schema_.get_table_name());
    }
  }
  return ret;
}

int ObTableSchemaSpliter::compare_obj(const ObObj& obj1, const ObObj& obj2)
{
  int ret = OB_SUCCESS;
  const ObObjMeta& meta1 = obj1.get_meta();
  const ObObjMeta& meta2 = obj2.get_meta();
  ObObjTypeClass tc1 = meta1.get_type_class();
  ObObjTypeClass tc2 = meta2.get_type_class();
  obj_cmp_func cmp_op_func = NULL;
  if (meta1.get_type() != meta2.get_type() || meta1.get_collation_type() != meta2.get_collation_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta type not match", K(ret), K(obj1), K(obj2), K(meta1), K(meta2));
  } else if (OB_FAIL(ObObjCmpFuncs::get_cmp_func(tc1, tc2, ObCmpOp::CO_EQ, cmp_op_func))) {
    LOG_WARN("fail to get compare func", K(ret), K(obj1), K(obj2));
  } else if (OB_ISNULL(cmp_op_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get compare func", K(ret), K(obj1), K(obj2));
  } else {
    int cmp = ObObjCmpFuncs::CR_FALSE;
    ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, true, INVALID_TZ_OFF, default_null_pos());
    if (OB_UNLIKELY(ObObjCmpFuncs::CR_OB_ERROR == (cmp = cmp_op_func(obj1, obj2, cmp_ctx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to compare obj1 and obj2", K(ret), K(obj1), K(obj2));
    } else if (!static_cast<bool>(cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj not equal", K(ret), K(obj1), K(obj2));
    }
  }
  return ret;
}

int ObTableSchemaSpliter::add_extra_condition()
{
  // do nothing
  return OB_SUCCESS;
}

int ObTableSchemaSpliter::construct_sql(const uint64_t tenant_id, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("%s AND tenant_id = %ld %s",
                 base_sql_.ptr(),
                 tenant_id,
                 extra_condition_.empty() ? "" : extra_condition_.ptr()))) {
    LOG_WARN("fail to assign sql", K(ret));
  }
  return ret;
}

int ObTableSchemaSpliter::check_stop()
{
  return executor_.check_stop();
}

#define SCHEMA_SPLITER_FILTER_INNER_TABLE_IMPL(TID, SchemaSpliter)                                             \
  int SchemaSpliter::add_extra_condition()                                                                     \
  {                                                                                                            \
    int ret = OB_SUCCESS;                                                                                      \
    extra_condition_.reset();                                                                                  \
    if (OB_FAIL(extra_condition_.append_fmt(" AND (table_id & 0xFFFFFFFFFF) > %ld ", OB_MIN_USER_TABLE_ID))) { \
      LOG_WARN("fail to append sql", K(ret));                                                                  \
    }                                                                                                          \
    return ret;                                                                                                \
  }

SCHEMA_SPLITER_FILTER_INNER_TABLE_IMPL(OB_ALL_COLUMN_TID, ObAllColumnSchemaSpliter);
SCHEMA_SPLITER_FILTER_INNER_TABLE_IMPL(OB_ALL_COLUMN_HISTORY_TID, ObAllColumnHistorySchemaSpliter);
SCHEMA_SPLITER_FILTER_INNER_TABLE_IMPL(OB_ALL_TABLE_TID, ObAllTableSchemaSpliter);
SCHEMA_SPLITER_FILTER_INNER_TABLE_IMPL(OB_ALL_TABLE_HISTORY_TID, ObAllTableHistorySchemaSpliter);

// DDL operations as below won't be migrated to new tables in tenant space:
// 1. tenant schema related.
// 2. inner table schema related.
// 3. system variable schema related.
int ObAllDdlOperationSchemaSpliter::add_extra_condition()
{
  int ret = OB_SUCCESS;
  extra_condition_.reset();
  if (OB_FAIL(extra_condition_.append_fmt(
          " AND (operation_type > %d || operation_type < %d) "
          " AND (operation_type > %d || operation_type < %d) "
          " AND (operation_type > %d || operation_type < %d || (table_id & 0xFFFFFFFFFF) > %ld) "
          " ORDER BY schema_version ASC",
          OB_DDL_TENANT_OPERATION_END,
          OB_DDL_TENANT_OPERATION_BEGIN,
          OB_DDL_SYS_VAR_OPERATION_END,
          OB_DDL_SYS_VAR_OPERATION_BEGIN,
          OB_DDL_TABLE_OPERATION_END,
          OB_DDL_TABLE_OPERATION_BEGIN,
          OB_MIN_USER_TABLE_ID))) {
    LOG_WARN("fail to append sql", K(ret));
  }
  return ret;
}

int ObSysVarDDLOperationSchemaSpliter::build_column_mapping()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableSchemaSpliter::build_column_mapping())) {
    LOG_WARN("fail to add basic column mapping", K(ret));
  } else {
    FOREACH_CNT_X(item, mapping_, OB_SUCC(ret))
    {
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item is null", K(ret));
      } else {
        const ObString& name = item->base_col_name_;
        if (column_name_equal(name, "operation_type")) {
          item->convert_func_ = convert_operation_type;
          LOG_INFO("set convertor", K(ret), K(name));
        } else if (column_name_equal(name, "table_id") || column_name_equal(name, "database_id") ||
                   column_name_equal(name, "data_table_id") || column_name_equal(name, "tablegroup_id")) {
          item->convert_func_ = filter_column_tenant_id;
          LOG_INFO("set convertor", K(ret), K(name));
        }
      }
    }
  }
  return ret;
}

int ObSysVarDDLOperationSchemaSpliter::add_extra_condition()
{
  int ret = OB_SUCCESS;
  extra_condition_.reset();
  if (OB_FAIL(extra_condition_.append_fmt(" AND schema_version in "
                                          " (SELECT distinct(schema_version) FROM %s ) ORDER BY schema_version ASC",
          OB_ALL_SYS_VARIABLE_HISTORY_TNAME))) {
    LOG_WARN("fail to append sql", K(ret));
  }
  return ret;
}

#define SCEHMA_SPLITER_FILTER_TENANT_ID_IMPL(TID, SCHEMASPLITER, SCHEMA_ID)                                            \
  int SCHEMASPLITER::quick_check(const uint64_t tenant_id, bool& passed)                                               \
  {                                                                                                                    \
    int ret = OB_SUCCESS;                                                                                              \
    ObSqlString sql;                                                                                                   \
    SMART_VAR(ObMySQLProxy::MySQLResult, res)                                                                          \
    {                                                                                                                  \
      sqlclient::ObMySQLResult* result = NULL;                                                                         \
      if (!is_inited_) {                                                                                               \
        ret = OB_NOT_INIT;                                                                                             \
        LOG_WARN("not init", K(ret));                                                                                  \
      } else if (OB_ISNULL(sql_proxy_)) {                                                                              \
        ret = OB_ERR_UNEXPECTED;                                                                                       \
        LOG_WARN("proxy is null", K(ret));                                                                             \
      } else if (OB_FAIL(check_stop())) {                                                                              \
        LOG_WARN("executor is stopped", K(ret));                                                                       \
      } else if (OB_INVALID_TENANT_ID == tenant_id || (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id)) {                 \
        ret = OB_INVALID_ARGUMENT;                                                                                     \
        LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));                                                           \
      } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "                                     \
                                        "WHERE (" #SCHEMA_ID " >> 40) != 0 OR tenant_id != 0",                         \
                     table_schema_.get_table_name()))) {                                                               \
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {                                               \
        LOG_WARN("fail to execute sql", K(ret), K(sql));                                                               \
      } else if (OB_ISNULL(result = res.get_result())) {                                                               \
        ret = OB_ERR_UNEXPECTED;                                                                                       \
        LOG_WARN("fail to get sql result", K(ret));                                                                    \
      } else if (OB_FAIL(result->next())) {                                                                            \
        LOG_WARN("fail to get result", K(ret));                                                                        \
      } else {                                                                                                         \
        int32_t count = OB_INVALID_COUNT;                                                                              \
        EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);                                                     \
        if (OB_SUCC(ret)) {                                                                                            \
          passed = (count == 0);                                                                                       \
          LOG_INFO("remain row count", K(ret), K(count));                                                              \
        }                                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
    return ret;                                                                                                        \
  }                                                                                                                    \
  int SCHEMASPLITER::check(const uint64_t tenant_id)                                                                   \
  {                                                                                                                    \
    int ret = OB_SUCCESS;                                                                                              \
    bool passed = false;                                                                                               \
    if (!is_inited_) {                                                                                                 \
      ret = OB_NOT_INIT;                                                                                               \
      LOG_WARN("not init", K(ret));                                                                                    \
    } else if (OB_INVALID_TENANT_ID == tenant_id || (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id)) {                   \
      ret = OB_INVALID_ARGUMENT;                                                                                       \
      LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));                                                             \
    } else if (OB_FAIL(quick_check(tenant_id, passed))) {                                                              \
      LOG_WARN("check failed", K(ret), K(tenant_id));                                                                  \
    } else if (!passed) {                                                                                              \
      ret = OB_ERR_UNEXPECTED;                                                                                         \
      LOG_WARN("check failed", K(ret), K(tenant_id));                                                                  \
    }                                                                                                                  \
    return ret;                                                                                                        \
  }                                                                                                                    \
  int SCHEMASPLITER::migrate(const uint64_t tenant_id)                                                                 \
  {                                                                                                                    \
    int ret = OB_SUCCESS;                                                                                              \
    ObSqlString sql;                                                                                                   \
    int64_t affected_rows = 0;                                                                                         \
    if (!is_inited_) {                                                                                                 \
      ret = OB_NOT_INIT;                                                                                               \
      LOG_WARN("not init", K(ret));                                                                                    \
    } else if (OB_ISNULL(sql_proxy_)) {                                                                                \
      ret = OB_ERR_UNEXPECTED;                                                                                         \
      LOG_WARN("proxy is null", K(ret));                                                                               \
    } else if (OB_FAIL(check_stop())) {                                                                                \
      LOG_WARN("executor is stopped", K(ret));                                                                         \
    } else if (OB_INVALID_TENANT_ID == tenant_id || (!iter_sys_ && OB_SYS_TENANT_ID == tenant_id)) {                   \
      ret = OB_INVALID_ARGUMENT;                                                                                       \
      LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));                                                             \
    } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET " #SCHEMA_ID " = (" #SCHEMA_ID " & 0xFFFFFFFFFF), tenant_id = 0 " \
                                      "WHERE " #SCHEMA_ID " >> 40 != 0 or tenant_id != 0",                             \
                   table_schema_.get_table_name()))) {                                                                 \
      LOG_WARN("sql append failed", K(ret));                                                                           \
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {                                      \
      LOG_WARN("execute sql failed", K(ret), K(tenant_id));                                                            \
    }                                                                                                                  \
    return ret;                                                                                                        \
  }
SCEHMA_SPLITER_FILTER_TENANT_ID_IMPL(OB_ALL_SEQUENCE_V2_TID, ObAllSequenceV2SchemaSpliter, sequence_key);
SCEHMA_SPLITER_FILTER_TENANT_ID_IMPL(
    OB_ALL_TENANT_GC_PARTITION_INFO_TID, ObAllTenantGcPartitionInfoSchemaSpliter, table_id);

int ObAllClogHistoryInfoV2SchemaSpliter::quick_check(const uint64_t tenant_id, bool& passed)
{
  int ret = OB_SUCCESS;
  int64_t row_count_from_sys = OB_INVALID_ID;
  int64_t row_count_from_tenant = OB_INVALID_ID;
  passed = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                                        "WHERE (table_id & 0xffffffffff) = %ld %s",
                     table_schema_.get_table_name(),
                     tenant_id,
                     extra_condition_.empty() ? "" : extra_condition_.ptr()))) {
        LOG_WARN("fail to assgin sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "count", row_count_from_sys, int64_t);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s WHERE 1 = 1 %s",
                     table_schema_.get_table_name(),
                     extra_condition_.empty() ? "" : extra_condition_.ptr()))) {
        LOG_WARN("fail to assgin sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "count", row_count_from_tenant, int64_t);
      }
    }
  }
  if (OB_SUCC(ret)) {
    passed = (row_count_from_sys == row_count_from_tenant);
  }
  return ret;
}

int ObAllClogHistoryInfoV2SchemaSpliter::construct_sql(const uint64_t tenant_id, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("%s AND (table_id & 0xffffffffff) = %ld %s",
                 base_sql_.ptr(),
                 tenant_id,
                 extra_condition_.empty() ? "" : extra_condition_.ptr()))) {
    LOG_WARN("fail to assign sql", K(ret));
  }
  return ret;
}

int ObAllTenantPartitionMetaTableSchemaSpliter::quick_check(const uint64_t tenant_id, bool& passed)
{
  UNUSED(tenant_id);
  UNUSED(passed);
  return OB_SUCCESS;
}

int ObAllTenantPartitionMetaTableSchemaSpliter::construct_sql(const uint64_t tenant_id, ObSqlString& sql)
{
  UNUSED(tenant_id);
  UNUSED(sql);
  return OB_SUCCESS;
}

int ObAllTenantPartitionMetaTableSchemaSpliter::process()
{
  return OB_SUCCESS;
}

int ObAllTableV2SchemaSpliter::add_convert_func(const ObColumnSchemaV2& column_schema, MapItem& item)
{
  int ret = OB_SUCCESS;
  UNUSED(column_schema);
  item.convert_func_ = deep_copy_str_for_spliter;
  return ret;
}

int ObAllTableV2SchemaSpliter::construct_base_sql()
{
  int ret = OB_SUCCESS;
  base_sql_.reset();
  if (mapping_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count", K(ret), "count", mapping_.count());
  } else if (base_sql_.assign_fmt("SELECT ")) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    FOREACH_CNT_X(item, mapping_, OB_SUCC(ret))
    {
      const ObString& name = item->base_col_name_;
      if (OB_FAIL(base_sql_.append_fmt("%s%.*s", 0 == __INNER_I__(item) ? "" : ", ", name.length(), name.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAllTableV2SchemaSpliter::construct_sql(const uint64_t tenant_id, const char* tname, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(tname)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), KP(tname));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(sql.assign_fmt("%s FROM %s WHERE tenant_id = %ld or (table_id & 0xffffffffff) < %ld "
                               "ORDER BY tenant_id ASC, table_id ASC",
            base_sql_.ptr(),
            tname,
            OB_SYS_TENANT_ID,
            OB_MIN_USER_TABLE_ID))) {
      LOG_WARN("fail to assign sql", K(ret));
    }
  } else {
    if (OB_FAIL(sql.assign_fmt("%s FROM %s WHERE tenant_id = 0 "
                               "ORDER BY tenant_id ASC, table_id ASC",
            base_sql_.ptr(),
            tname))) {
      LOG_WARN("fail to assign sql", K(ret));
    }
  }
  return ret;
}

int ObAllTableV2SchemaSpliter::init_sys_iterator(const uint64_t tenant_id, ObSplitTableIterator& sys_iter)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(construct_sql(tenant_id, OB_ALL_TABLE_TNAME, sql))) {
    LOG_WARN("fail to add addition", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_iter.init(sql, tenant_id))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAllTableV2SchemaSpliter::init_tenant_iterator(const uint64_t tenant_id, ObSplitTableIterator& tenant_iter)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(construct_sql(tenant_id, OB_ALL_TABLE_V2_TNAME, sql))) {
    LOG_WARN("fail to add addition", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_iter.init(sql, tenant_id))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAllTableV2SchemaSpliter::quick_check(const uint64_t tenant_id, bool& passed)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(schema_service_.check_tenant_can_use_new_table(tenant_id, passed))) {
    LOG_WARN("fail to check tenant can use new table", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAllTableV2HistorySchemaSpliter::add_convert_func(const ObColumnSchemaV2& column_schema, MapItem& item)
{
  int ret = OB_SUCCESS;
  UNUSED(column_schema);
  item.convert_func_ = deep_copy_str_for_spliter;
  return ret;
}

int ObAllTableV2HistorySchemaSpliter::construct_base_sql()
{
  int ret = OB_SUCCESS;
  base_sql_.reset();
  if (mapping_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count", K(ret), "count", mapping_.count());
  } else if (base_sql_.assign_fmt("SELECT ")) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    FOREACH_CNT_X(item, mapping_, OB_SUCC(ret))
    {
      const ObString& name = item->base_col_name_;
      if (OB_FAIL(base_sql_.append_fmt("%s%.*s", 0 == __INNER_I__(item) ? "" : ", ", name.length(), name.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAllTableV2HistorySchemaSpliter::construct_sql(const uint64_t tenant_id, const char* tname, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(tname)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), KP(tname));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(sql.assign_fmt("%s FROM %s WHERE tenant_id = %ld or (table_id & 0xffffffffff) < %ld "
                               "ORDER BY tenant_id ASC, table_id ASC, schema_version ASC",
            base_sql_.ptr(),
            tname,
            OB_SYS_TENANT_ID,
            OB_MIN_USER_TABLE_ID))) {
      LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
    }
  } else {
    if (OB_FAIL(sql.assign_fmt("%s FROM %s WHERE tenant_id = 0 "
                               "ORDER BY tenant_id ASC, table_id ASC, schema_version ASC",
            base_sql_.ptr(),
            tname))) {
      LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObAllTableV2HistorySchemaSpliter::init_sys_iterator(const uint64_t tenant_id, ObSplitTableIterator& sys_iter)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(construct_sql(tenant_id, OB_ALL_TABLE_HISTORY_TNAME, sql))) {
    LOG_WARN("fail to add addition", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_iter.init(sql, tenant_id))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAllTableV2HistorySchemaSpliter::init_tenant_iterator(const uint64_t tenant_id, ObSplitTableIterator& tenant_iter)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(construct_sql(tenant_id, OB_ALL_TABLE_V2_HISTORY_TNAME, sql))) {
    LOG_WARN("fail to add addition", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_iter.init(sql, tenant_id))) {
    LOG_WARN("fail to init iter", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAllTableV2HistorySchemaSpliter::quick_check(const uint64_t tenant_id, bool& passed)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stopped", K(ret));
  } else if (OB_FAIL(schema_service_.check_tenant_can_use_new_table(tenant_id, passed))) {
    LOG_WARN("fail to check tenant can use new table", K(ret), K(tenant_id));
  }
  return ret;
}
}  // namespace rootserver
}  // namespace oceanbase
