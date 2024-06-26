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

#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "ob_external_table_file_mgr.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "observer/ob_inner_sql_connection.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "share/external_table/ob_external_table_utils.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "share/external_table/ob_external_table_file_rpc_processor.h"
#include "share/external_table/ob_external_table_file_rpc_proxy.h"
#include "storage/ob_common_id_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace sql;
using namespace transaction::tablelock;
using namespace pl;
using namespace common::sqlclient;
namespace share
{

int ObExternalTableFilesKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
   int ret = OB_SUCCESS;
   ObExternalTableFilesKey *new_value = NULL;
   ObDataBuffer allocator(buf, buf_len);
   if (OB_ISNULL(new_value = OB_NEWx(ObExternalTableFilesKey, &allocator))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("fail to allocate memory", K(ret));
   } else {
     new_value->tenant_id_ = this->tenant_id_;
     new_value->table_id_ = this->table_id_;
     new_value->partition_id_ = this->partition_id_;
     key = new_value;
   }
   return ret;
 }

int64_t ObExternalTableFiles::size() const
{
   int64_t size = sizeof(*this) + sizeof(ObString) * file_urls_.count()
                  + sizeof(int64_t) * file_ids_.count() + sizeof(int64_t) * file_sizes_.count();
   for (int i = 0; i < file_urls_.count(); ++i) {
     size += file_urls_.at(i).length();
   }
   return size;
 }

int ObExternalTableFiles::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(buf, buf_len);
  ObExternalTableFiles *new_value = NULL;
  if (OB_ISNULL(new_value = OB_NEWx(ObExternalTableFiles, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  }

  if (OB_SUCC(ret) && this->file_urls_.count() > 0) {
    if (OB_FAIL(new_value->file_urls_.allocate_array(allocator, this->file_urls_.count()))) {
      LOG_WARN("fail to allocate array", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < this->file_urls_.count(); i++) {
        OZ (ob_write_string(allocator, this->file_urls_.at(i), new_value->file_urls_.at(i)));
      }
    }
  }

  if (OB_SUCC(ret) && this->file_ids_.count() > 0) {
    if (OB_FAIL(new_value->file_ids_.allocate_array(allocator, this->file_ids_.count()))) {
      LOG_WARN("fail to allocate array", K(ret));
    } else {
      MEMCPY(new_value->file_ids_.get_data(), this->file_ids_.get_data(),
             sizeof(int64_t) * this->file_ids_.count());
    }
  }

  if (OB_SUCC(ret) && this->file_sizes_.count() > 0) {
    if (OB_FAIL(new_value->file_sizes_.allocate_array(allocator, this->file_sizes_.count()))) {
      LOG_WARN("fail to allocate array", K(ret));
    } else {
      MEMCPY(new_value->file_sizes_.get_data(), this->file_sizes_.get_data(),
             sizeof(int64_t) * this->file_sizes_.count());
    }
  }
  if (OB_SUCC(ret)) {
    new_value->create_ts_ = this->create_ts_;
  }
  value = new_value;
  return ret;
}

int ObExternalTableFileManager::flush_cache(const uint64_t tenant_id, const uint64_t table_id, const uint64_t part_id)
{
  int ret = OB_SUCCESS;
  ObExternalTableFilesKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = table_id;
  key.partition_id_ = part_id;
  if (OB_FAIL(kv_cache_.erase(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to erase value", K(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObExternalTableFileManager::clear_inner_table_files(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  int64_t affected_rows = 0;
  OZ (delete_sql.assign_fmt("DELETE FROM %s WHERE TABLE_ID = %lu",
                            OB_ALL_EXTERNAL_TABLE_FILE_TNAME, table_id));
  OZ (trans.write(tenant_id, delete_sql.ptr(), affected_rows));
  LOG_DEBUG("check clear rows", K(affected_rows));
  return ret;
}


int ObExternalTableFileManager::clear_inner_table_files_within_one_part(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t part_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  int64_t affected_rows = 0;
  OZ (delete_sql.assign_fmt("DELETE FROM %s WHERE TABLE_ID = %lu AND PART_ID = %lu",
                            OB_ALL_EXTERNAL_TABLE_FILE_TNAME, table_id, part_id));
  OZ (trans.write(tenant_id, delete_sql.ptr(), affected_rows));
  LOG_DEBUG("check clear rows", K(affected_rows));
  return ret;
}
int ObExternalTableFileManager::init()
{
  int ret = OB_SUCCESS;
  OZ (kv_cache_.init("external_table_file_cache"));
  return ret;
}

ObExternalTableFileManager &ObExternalTableFileManager::get_instance()
{
  static ObExternalTableFileManager instance_;
  return instance_;
}

int ObExternalTableFileManager::get_external_files(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const bool is_local_file_on_disk,
    ObIAllocator &allocator,
    ObIArray<ObExternalFileInfo> &external_files,
    ObIArray<ObNewRange *> *range_filter /*default = NULL*/)
{
  return get_external_files_by_part_id(tenant_id, table_id, table_id, is_local_file_on_disk,  allocator, external_files, range_filter);
}

int ObExternalTableFileManager::get_external_files_by_part_ids(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<int64_t> &partition_ids,
    const bool is_local_file_on_disk,
    ObIAllocator &allocator,
    ObIArray<ObExternalFileInfo> &external_files,
    ObIArray<ObNewRange *> *range_filter /*default = NULL*/)
{
  int ret = OB_SUCCESS;
  if (partition_ids.empty()) {
     OZ (get_external_files_by_part_id(tenant_id, table_id, table_id, is_local_file_on_disk,  allocator, external_files, range_filter));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      OZ (get_external_files_by_part_id(tenant_id, table_id, partition_ids.at(i), is_local_file_on_disk,  allocator, external_files, range_filter));
    }
  }
  return ret;
}

int ObExternalTableFileManager::get_external_files_by_part_id(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t partition_id,
    const bool is_local_file_on_disk,
    ObIAllocator &allocator,
    ObIArray<ObExternalFileInfo> &external_files,
    ObIArray<ObNewRange *> *range_filter /*default = NULL*/)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObExternalTableFiles *ext_files = NULL;
  ObExternalTableFilesKey key;
  key.tenant_id_ = tenant_id;
  key.table_id_ = table_id;
  key.partition_id_ = partition_id;
  if (OB_FAIL(kv_cache_.get(key, ext_files, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get from KVCache", K(ret), K(key));
    }
  }

  if ((OB_SUCC(ret) && is_cache_value_timeout(*ext_files))
      || OB_ENTRY_NOT_EXIST == ret) {
    if (OB_FAIL(fill_cache_from_inner_table(key, ext_files, handle))) {
      LOG_WARN("fail to fill cache from inner table", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < ext_files->file_urls_.count(); ++i) {
    bool in_ranges = false;
    if (range_filter != NULL && OB_FAIL(ObExternalTableUtils::is_file_id_in_ranges(*range_filter,
                                                                        ext_files->file_ids_.at(i),
                                                                        in_ranges))) {
      LOG_WARN("failed to judge file id in ranges", K(ret));
    } else if (range_filter == NULL || in_ranges) {
      ObExternalFileInfo file_info;
      ObString file_url = ext_files->file_urls_.at(i);
      file_info.file_id_ = ext_files->file_ids_.at(i);
      file_info.file_size_ = ext_files->file_sizes_.at(i);
      file_info.part_id_ = partition_id;
      if (is_local_file_on_disk) {
        ObString ip_port = file_url.split_on(ip_delimiter);
        OZ (file_info.file_addr_.parse_from_string(ip_port));
      }
      OZ (ob_write_string(allocator, file_url, file_info.file_url_));
      OZ (external_files.push_back(file_info));
    }
  }
  LOG_TRACE("get external file list result", K(table_id), K(is_local_file_on_disk), K(external_files));
  return ret;
}


int ObExternalTableFileManager::build_row_for_file_name(ObNewRow &row, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObj *obj_array = nullptr;
  const int size = 1;
  if (OB_ISNULL(obj_array = static_cast<ObObj*>(
              allocator.alloc(sizeof(ObObj) * size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    for (ObObj *ptr = obj_array; ptr < obj_array + size; ++ptr) {
      new(ptr)ObObj();
      ptr->set_collation_type(ObCharset::get_system_collation());
    }
    row.assign(obj_array, size);
  }
  return ret;
}

int ObExternalTableFileManager::get_genarated_expr_from_partition_column(const ObColumnSchemaV2 *column_schema,
                                                                         const ObTableSchema *table_schema,
                                                                         ObSQLSessionInfo *session_info,
                                                                         ObRawExprFactory *expr_factory,
                                                                         ObSchemaGetterGuard &schema_guard,
                                                                         ObIAllocator &allocator,
                                                                         ObRawExpr *&gen_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (column_schema->is_generated_column()) {
    ObColumnSchemaV2 tmp;
    ObSchemaChecker schema_checker;
    ObResolverParams params;
    params.session_info_ = session_info;
    params.allocator_ = &allocator;
    params.expr_factory_ = expr_factory;
    ObString col_def;
    if (OB_FAIL(schema_checker.init(schema_guard))) {
      LOG_WARN("init schema checker failed", K(ret));
    } else if (FALSE_IT(params.schema_checker_ = &schema_checker)) {
    } else if (OB_FAIL(column_schema->get_orig_default_value().get_string(col_def))) {
      LOG_WARN("get generated column definition failed", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_generated_column_expr(params, col_def, const_cast<ObTableSchema &>(*table_schema),
                                                                      tmp, gen_expr))) {
      LOG_WARN("resolve generated column expr failed", K(ret));
    } else if (OB_FAIL(gen_expr->formalize(session_info))) {
      LOG_WARN("formalize failed", K(ret));
    } else {
      ObExprResType expected_type;
      expected_type.set_meta(column_schema->get_meta_type());
      expected_type.set_accuracy(column_schema->get_accuracy());
      expected_type.set_result_flag(ObRawExprUtils::calc_column_result_flag(*column_schema));
      if (ObRawExprUtils::need_column_conv(expected_type, *gen_expr)) {
        if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*expr_factory, column_schema, gen_expr, session_info))) {
          LOG_WARN("create cast expr failed", K(ret));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external table partition expr is not generated column", K(ret));
  }
  return ret;
}

int ObExternalTableFileManager::cg_expr_by_mocking_field_expr(const ObTableSchema *table_schema,
                                                              ObRawExpr *gen_expr,
                                                              ObSQLSessionInfo *session_info,
                                                              ObRawExprFactory *expr_factory,
                                                              share::schema::ObSchemaGetterGuard &schema_guard,
                                                              ObIAllocator &allocator,
                                                              ObTempExpr *&temp_expr)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *mock_field_expr = NULL;
  RowDesc row_desc;
  ObRawExpr *file_name_expr = NULL;
  if (OB_FAIL(expr_factory->create_raw_expr(T_REF_COLUMN, mock_field_expr))) {
    LOG_WARN("create column ref raw expr failed", K(ret));
  } else if (OB_ISNULL(mock_field_expr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FALSE_IT(mock_field_expr->set_column_attr(table_schema->get_table_name(), "_file_name"))) {
    LOG_WARN(("field_expr is null"));
  } else if (OB_FAIL(mock_field_expr->add_flag(IS_COLUMN))) {
    LOG_WARN("failed to add flag IS_COLUMN", K(ret));
  } else if (OB_FAIL(row_desc.init())) {
    LOG_WARN("row desc init failed", K(ret));
  } else if (OB_FAIL(row_desc.add_column(mock_field_expr))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_metadata_filename_expr(gen_expr, file_name_expr))) {
    LOG_WARN("get metadata filename column failed", K(ret));
  } else if (OB_ISNULL(file_name_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gen part expr of ext table with no metadata file_name", K(ret));
  } else if (OB_FALSE_IT(mock_field_expr->set_collation_type(file_name_expr->get_result_meta().get_collation_type()))) {
  } else if (OB_FALSE_IT(mock_field_expr->set_data_type(file_name_expr->get_result_meta().get_type()))) {
  } else if (OB_FAIL(ObTransformUtils::replace_expr(file_name_expr,
                                                    mock_field_expr,
                                                    gen_expr))) {
    LOG_WARN("replace exprs failed", K(ret));
  } else if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(gen_expr,
                                                                      row_desc,
                                                                      allocator,
                                                                      session_info,
                                                                      &schema_guard,
                                                                      temp_expr))) {
    LOG_WARN("fail to gen temp expr", K(ret));
  }
  return ret;
}

int ObExternalTableFileManager::cg_partition_expr_rt_expr(const ObTableSchema *table_schema,
                                                          ObRawExprFactory *expr_factory,
                                                          ObSQLSessionInfo *session_info,
                                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                                          ObIAllocator &allocator,
                                                          ObIArray<ObTempExpr *> &temp_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> partition_column_ids;
  if (OB_ISNULL(expr_factory) || OB_ISNULL(table_schema) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr_factory), K(table_schema), K(session_info));
  }
  OZ (table_schema->get_partition_key_info().get_column_ids(partition_column_ids));
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_column_ids.count(); i++) {
    const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(partition_column_ids.at(i));
    ObString expr_str = column_schema->get_cur_default_value().get_string();
    if (column_schema->is_generated_column()) {
      ObRawExpr *gen_expr = NULL;
      ObTempExpr *temp_expr = NULL;
      if (OB_FAIL(get_genarated_expr_from_partition_column(column_schema, table_schema, session_info,
                                                           expr_factory, schema_guard, allocator, gen_expr))) {
        LOG_WARN("get generated expr from partition column failed", K(ret));
      } else if (OB_FAIL(cg_expr_by_mocking_field_expr(table_schema, gen_expr, session_info,
                                                       expr_factory, schema_guard, allocator, temp_expr))) {
        LOG_WARN("cg expr failed", K(ret));
      } else if (OB_FAIL(temp_exprs.push_back(temp_expr))) {
        LOG_WARN("push back failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("external table partition expr is not generated column", K(ret));
    }
  }
  return ret;
}

int ObExternalTableFileManager::find_partition_existed(ObIArray<ObNewRow> &existed_part,
                                                       ObNewRow &file_part_val,
                                                       int64_t &found)
{
  int ret = OB_SUCCESS;
  found = -1;
  for (int64_t i = 0; OB_SUCC(ret) && found == -1 && i < existed_part.count(); i++) {
    bool match = true;
    CK (OB_LIKELY(existed_part.at(i).get_count() == file_part_val.get_count()));
    for (int64_t j = 0; OB_SUCC(ret) && match && j < file_part_val.get_count(); j++) {
      if (!existed_part.at(i).get_cell(j).can_compare(file_part_val.get_cell(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not compare obj", K(existed_part.at(i)), K(file_part_val.get_cell(j)));
      } else if (existed_part.at(i).get_cell(j) == file_part_val.get_cell(j)) {
        //do nothing
      } else {
        match = false;
      }
    }
    if (match) {
      found = i;
    }
  }
  return ret;
}

int ObExternalTableFileManager::alter_partition_for_ext_table(ObMySQLTransaction &trans,
                                                               sql::ObExecContext &exec_ctx,
                                                               ObAlterTableStmt *alter_table_stmt,
                                                               ObIArray<int64_t> &file_part_ids)
{
  int ret = OB_SUCCESS;
  UNUSED(trans); //TODO: use the same trans to create partition
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  ObAlterTableRes res;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_stmt->get_alter_table_arg(), res))) {
    LOG_WARN("alter table failed", K(ret));
  } else if (OB_UNLIKELY(res.res_arg_array_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create partition may be failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < res.res_arg_array_.count(); i++) {
      OZ (file_part_ids.push_back(res.res_arg_array_.at(i).part_object_id_));
    }
  }
  return ret;
}

int ObExternalTableFileManager::add_partition_for_alter_stmt(ObAlterTableStmt *&alter_table_stmt,
                                                             const ObString &part_name,
                                                             ObNewRow &part_val)
{
  int ret = OB_SUCCESS;
  ObPartition partition;
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add partition for alter stmt failed", K(ret));
  } else if (OB_FAIL(partition.set_part_name(const_cast<ObString &>(part_name)))) {
    LOG_WARN("set partition name failed", K(ret));
  } else if (OB_FAIL(partition.add_list_row(part_val))) {
    LOG_WARN("add list row failed", K(ret));
  } else if (OB_FAIL(alter_table_stmt->get_alter_table_arg().alter_table_schema_.add_partition(partition))) {
    LOG_WARN("failed to add partition", K(ret));
  }
  return ret;
}

int ObExternalTableFileManager::create_alter_table_stmt(sql::ObExecContext &exec_ctx,
                                                        const ObTableSchema *table_schema,
                                                        const ObDatabaseSchema *database_schema,
                                                        const int64_t part_num,
                                                        const ObAlterTableArg::AlterPartitionType alter_part_type,
                                                        ObAlterTableStmt *&alter_table_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = exec_ctx.get_my_session();
  ObResolverParams params;
  CK (OB_NOT_NULL(session_info));
  CK (OB_NOT_NULL(table_schema));
  CK (OB_NOT_NULL(database_schema));
  params.expr_factory_ = exec_ctx.get_expr_factory();
  params.stmt_factory_ = exec_ctx.get_stmt_factory();
  params.allocator_ = &exec_ctx.get_allocator();
  params.session_info_ = exec_ctx.get_my_session();
  params.query_ctx_ = exec_ctx.get_stmt_factory()->get_query_ctx();
  SMART_VAR (ObAlterTableResolver, alter_table_resolver, params) {
    if (alter_table_stmt == NULL &&
        NULL == (alter_table_stmt = alter_table_resolver.create_stmt<ObAlterTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create alter table stmt", K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_tz_info_wrap(session_info->get_tz_info_wrap()))) {
      SQL_RESV_LOG(WARN, "failed to set_tz_info_wrap", "tz_info_wrap", session_info->get_tz_info_wrap(), K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_nls_formats(
        session_info->get_local_nls_date_format(),
        session_info->get_local_nls_timestamp_format(),
        session_info->get_local_nls_timestamp_tz_format()))) {
      SQL_RESV_LOG(WARN, "failed to set_nls_formats", K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_origin_database_name(database_schema->get_database_name_str()))) {
      SQL_RESV_LOG(WARN, "failed to set origin database name", K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_origin_table_name(table_schema->get_table_name_str()))) {
      SQL_RESV_LOG(WARN, "failed to set origin table name", K(ret));
    } else {
      alter_table_stmt->set_tenant_id(table_schema->get_tenant_id());
      alter_table_stmt->set_table_id(table_schema->get_table_id());
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_schema_version(
                                                                                table_schema->get_schema_version());
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.
                          get_part_option().set_part_func_type(table_schema->get_part_option().get_part_func_type());
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_part_num(part_num);
      alter_table_stmt->set_alter_table_partition();
      alter_table_stmt->get_alter_table_arg().alter_part_type_ = alter_part_type;
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.alter_type_ = OB_DDL_ALTER_TABLE;
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_part_level(table_schema->get_part_level());
      alter_table_stmt->get_alter_table_arg().tz_info_ = session_info->get_tz_info_wrap().get_tz_info_offset();
      alter_table_stmt->get_alter_table_arg().is_inner_ = session_info->is_inner();
      alter_table_stmt->get_alter_table_arg().exec_tenant_id_ = session_info->get_effective_tenant_id();
      alter_table_stmt->get_alter_table_arg().session_id_ = session_info->get_sessid_for_table();
    }
  }
  return ret;
}

int ObExternalTableFileManager::get_all_partition_list_val(const ObTableSchema *table_schema, ObIArray<ObNewRow> &part_vals, ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(table_schema));
  if (table_schema->get_partition_num() > 0) {
    CK(OB_NOT_NULL(table_schema->get_part_array()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_num(); i++) {
    ObPartition *partition = table_schema->get_part_array()[i];
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret));
    } else if (partition->get_part_name().compare("P_DEFAULT") == 0) {
      //skip
    } else {
      const ObIArray<ObNewRow> &list_val = partition->get_list_row_values();
      if (OB_LIKELY(list_val.count() == 1)) {
        const ObNewRow &row = list_val.at(0);
        OZ (part_vals.push_back(row));
        OZ (part_ids.push_back(partition->get_part_id()));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("external table partition list value num should be one", K(ret));
      }
    }
  }
  return ret;
}

int ObExternalTableFileManager::calculate_file_part_val_by_file_name(const ObTableSchema *table_schema,
                                                                const ObIArray<ObExternalFileInfoTmp> &file_infos,
                                                                ObIArray<ObNewRow> &part_vals,
                                                                share::schema::ObSchemaGetterGuard &schema_guard,
                                                                ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = exec_ctx.get_allocator();
  ObArray<sql::ObTempExpr *> temp_exprs;
  ObNewRow file_name_row; //only store file name, auto-part list val will only be calc by file name.
  CK (OB_NOT_NULL(table_schema) && OB_LIKELY(table_schema->is_external_table()));
  const bool is_local_storage = ObSQLUtils::is_external_files_on_local_disk(table_schema->get_external_file_location());
  OZ (cg_partition_expr_rt_expr(table_schema, exec_ctx.get_expr_factory(), exec_ctx.get_my_session(),
                                schema_guard, exec_ctx.get_allocator(), temp_exprs));
  OZ (build_row_for_file_name(file_name_row, exec_ctx.get_allocator()));
  for (int64_t i = 0; OB_SUCC(ret) && i < file_infos.count(); i++) {
    ObNewRow list_val;
    ObObj *obj_array = nullptr;
    if (file_name_row.get_count() > 0) {
      file_name_row.get_cell(0).set_string(ObVarcharType, is_local_storage ?
                                          file_infos.at(i).file_url_.after(ip_delimiter) : file_infos.at(i).file_url_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row cell count not expected", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(obj_array = static_cast<ObObj*>(
                allocator.alloc(sizeof(ObObj) * temp_exprs.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      for (ObObj *ptr = obj_array; ptr < obj_array + temp_exprs.count(); ++ptr) {
        new(ptr)ObObj();
        ptr->set_collation_type(ObCharset::get_system_collation());
      }
      if (OB_SUCC(ret)) {
        list_val.assign(obj_array, temp_exprs.count());
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < temp_exprs.count(); j++) {
      OZ (temp_exprs.at(j)->eval(exec_ctx, file_name_row, list_val.get_cell(j)));
    }
    OZ (part_vals.push_back(list_val));
  }
  return ret;
}

int ObExternalTableFileManager::add_item_to_map(ObIAllocator &allocator,
                                                common::hash::ObHashMap<int64_t, ObArray<ObExternalFileInfoTmp> *> &hash_map,
                                                int64_t part_id, const ObExternalFileInfoTmp &file_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObExternalFileInfoTmp> *part_file_urls = NULL;
  OZ (hash_map.get_refactored(part_id, part_file_urls));
  if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_SUCCESS;
    if (OB_ISNULL(part_file_urls = OB_NEWx(ObArray<ObExternalFileInfoTmp>, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new array", KR(ret));
    } else if (OB_FAIL(hash_map.set_refactored(part_id, part_file_urls))) {
      LOG_WARN("fail to set refactored", KR(ret), K(part_id));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != part_file_urls) {
        part_file_urls->~ObArray<ObExternalFileInfoTmp>();
        allocator.free(part_file_urls);
        part_file_urls = NULL;
      }
    }
  } else {
    LOG_WARN("hash map get key value failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    OZ (part_file_urls->push_back(file_info));
  }
  return ret;
}

int ObExternalTableFileManager::calculate_all_files_partitions(share::schema::ObSchemaGetterGuard &schema_guard,
                                                             ObExecContext &exec_ctx,
                                                             const ObTableSchema *table_schema,
                                                             const ObIArray<ObExternalFileInfoTmp> &file_infos,
                                                             ObIArray<int64_t> &file_part_ids,
                                                             ObIArray<ObNewRow> &partitions_to_add,
                                                             ObIArray<ObPartition *> &partitions_to_del)
{
  int ret = OB_SUCCESS;
  int64_t mock_part_id = 0;
  ObArray<ObNewRow> existed_part_vals;
  ObArray<int64_t> existed_part_ids;
  ObArray<ObNewRow> file_part_vals;
  CK (OB_NOT_NULL(table_schema) && OB_LIKELY(table_schema->is_external_table()));
  OZ (get_all_partition_list_val(table_schema, existed_part_vals, existed_part_ids));
  OZ (calculate_file_part_val_by_file_name(table_schema, file_infos, file_part_vals, schema_guard, exec_ctx));
  for (int64_t i = 0; OB_SUCC(ret) && i < file_part_vals.count(); i++) {
    int64_t idx = -1;
    OZ (find_partition_existed(existed_part_vals, file_part_vals.at(i), idx));
    if (OB_FAIL(ret)) {
    } else if (idx == -1) {
      OZ (existed_part_vals.push_back(file_part_vals.at(i)));
      OZ (existed_part_ids.push_back(mock_part_id));
      OZ (file_part_ids.push_back(mock_part_id));
      OZ (partitions_to_add.push_back(file_part_vals.at(i)));
      mock_part_id--;
    } else if (idx >= 0 && idx < existed_part_ids.count()) {
      OZ (file_part_ids.push_back(existed_part_ids.at(idx)));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find item failed", K(ret), K(idx));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_num(); i++) {
    bool found = false;
    ObPartition *partition = table_schema->get_part_array()[i];
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret));
    } else if (partition->get_part_name().compare("P_DEFAULT") == 0) {
      found = true;
    }
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < file_part_ids.count(); j++) {
      if (partition->get_part_id() == file_part_ids.at(j)) {
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      OZ (partitions_to_del.push_back(partition));
    }
  }
  return ret;
}

int ObExternalTableFileManager::update_inner_table_file_list(
    sql::ObExecContext &exec_ctx,
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes,
    const uint64_t part_id)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArenaAllocator allocator;
  CK (OB_NOT_NULL(GCTX.sql_proxy_),
      OB_NOT_NULL(GCTX.schema_service_));
  OZ (trans.start(GCTX.sql_proxy_, tenant_id));
  OZ (lock_for_refresh(trans, tenant_id, table_id));
  ObArray<ObExternalFileInfoTmp> file_infos;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
    OZ (file_infos.push_back(ObExternalFileInfoTmp(file_urls.at(i), file_sizes.at(i), part_id)));
  }
  if (OB_FAIL(ret)) {
  } else if (part_id != -1) {
    OZ (update_inner_table_files_list_by_part(trans, tenant_id, table_id, part_id, file_infos));
  } else {
    OZ (update_inner_table_files_list_by_table(exec_ctx, trans, tenant_id, table_id, file_infos));
  }
  OZ (trans.end(true));
  if (trans.is_started()) {
    trans.end(false);
  }
  return ret;
}

int ObExternalTableFileManager::get_file_sizes_by_map(ObIArray<ObString> &file_urls,
                                                      ObIArray<int64_t> &file_sizes,
                                                      common::hash::ObHashMap<ObString, int64_t> &map)
{
  int ret = OB_SUCCESS;
  file_sizes.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
    int64_t size = 0;
    if (OB_FAIL(map.get_refactored(file_urls.at(i), size))) {
      LOG_WARN("get file url to size failed", K(ret));
    } else if (OB_FAIL(file_sizes.push_back(size))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

int ObExternalTableFileManager::update_inner_table_files_list_by_table(
    sql::ObExecContext &exec_ctx,
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObExternalFileInfoTmp> &file_infos)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObArenaAllocator allocator;
  CK (OB_NOT_NULL(sql_proxy),
      OB_NOT_NULL(GCTX.schema_service_));
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  OZ (schema_guard.get_table_schema(tenant_id, table_id, table_schema));
  CK (OB_NOT_NULL(table_schema));
  OZ (schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema));
  CK (OB_NOT_NULL(database_schema));
  if (OB_FAIL(ret)) {
  } else if (!table_schema->is_partitioned_table()) {
    OZ (update_inner_table_files_list_by_part(trans, tenant_id, table_id, table_id, file_infos));
  } else {
    int64_t max_part_id = 0;
    common::hash::ObHashMap<int64_t, ObArray<ObExternalFileInfoTmp> *> part_id_to_file_urls; //part id to file urls.
    if (OB_SUCC(ret)) {
      OZ (part_id_to_file_urls.create(file_infos.count() + 1, "ExtFileUrl")); //todo: too large

      int64_t partition_to_add_num = 0;
      ObArray<int64_t> part_ids;
      ObArray<ObNewRow> partitions_to_add;
      ObArray<ObPartition *> partitions_to_del;
      OZ (calculate_all_files_partitions(schema_guard, exec_ctx, table_schema, file_infos, part_ids, partitions_to_add, partitions_to_del));
      if (OB_SUCC(ret)) {
        int64_t max_part_idx = 0;
        OZ (table_schema->get_max_part_idx(max_part_idx, true/*without default*/));

        if (OB_SUCC(ret) && partitions_to_del.count() > 0) {
          ObAlterTableStmt *alter_table_stmt = NULL;
          OZ (create_alter_table_stmt(exec_ctx, table_schema, database_schema, partitions_to_add.count(), ObAlterTableArg::DROP_PARTITION, alter_table_stmt));
          if (OB_SUCC(ret)) {
            for (int64_t i = 0; OB_SUCC(ret) && i < partitions_to_del.count(); i++) {
              if (OB_ISNULL(partitions_to_del.at(i))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("partitions to del is null", K(ret));
              } else if (OB_FAIL(alter_table_stmt->get_alter_table_arg().alter_table_schema_.add_partition(*partitions_to_del.at(i)))) {
                LOG_WARN("failed to add partition", K(ret));
              }
            }
          }
          ObArray<ObExternalFileInfoTmp> empty;
          // Will not delete the partition now.
          // ObArray<int64_t> file_part_ids_del;
          // ObSEArray<ObAddr, 8> all_servers;
          // OZ (GCTX.location_service_->external_table_get(tenant_id, table_id, all_servers));
          // OZ (alter_partition_for_ext_table(trans, exec_ctx, alter_table_stmt, file_part_ids_del));
          for (int i = 0; OB_SUCC(ret) && i < partitions_to_del.count(); i++) {
            if (OB_ISNULL(partitions_to_del.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("partitions to del is null", K(ret));
            } else {
              OZ (update_inner_table_files_list_by_part(trans, tenant_id, table_id, partitions_to_del.at(i)->get_part_id(), empty));
            }
          }
        }
        if (OB_SUCC(ret) && partitions_to_add.count() > 0) {
          ObAlterTableStmt *alter_table_stmt = NULL;
          OZ (create_alter_table_stmt(exec_ctx, table_schema, database_schema, partitions_to_add.count(), ObAlterTableArg::ADD_PARTITION, alter_table_stmt));
          for (int64_t i = 0; OB_SUCC(ret) && i < partitions_to_add.count(); i++) {
            ObSqlString tmp_part_name;
            ObString part_name;
            OZ (tmp_part_name.append_fmt("P%ld", ++max_part_idx));
            OZ (ob_write_string(exec_ctx.get_allocator(), tmp_part_name.string(), part_name));
            OZ (add_partition_for_alter_stmt(alter_table_stmt, part_name, partitions_to_add.at(i)));
          }
          //ObFixedArray<int64_t, ObIAllocator> file_part_ids_added(&exec_ctx.get_allocator(), partitions_to_add.count());
          ObArray<int64_t> file_part_ids_added;
          OZ (alter_partition_for_ext_table(trans, exec_ctx, alter_table_stmt, file_part_ids_added));
          #define IS_EXT_MOCK_PART_ID(id) id <= 0 ? true : false
          #define GET_EXT_MOCK_PART_ID(id) -id
          #define CHECK_EXT_MOCK_PART_ID_VALID(real_part_ids, id) if (OB_UNLIKELY(id < 0 \
                                                                            && id >= real_part_ids.count())) {\
                                                                    ret = OB_ERR_UNEXPECTED; \
                                                                  }
          #define MAP_EXT_MOCK_PART_ID_TO_REAL_PART_ID(real_part_ids, id) real_part_ids.at(id)
          //make mock_part_id to real part id;
          for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); i++) {
            int64_t part_id = 0;
            if (IS_EXT_MOCK_PART_ID(part_ids.at(i))) {
              int64_t mock_part_id = GET_EXT_MOCK_PART_ID(part_ids.at(i));
              CHECK_EXT_MOCK_PART_ID_VALID(file_part_ids_added, mock_part_id);
              part_id = MAP_EXT_MOCK_PART_ID_TO_REAL_PART_ID(file_part_ids_added, mock_part_id);
            } else {
              part_id = part_ids.at(i);
            }
            if (OB_SUCC(ret)) {
              OZ (add_item_to_map(exec_ctx.get_allocator(), part_id_to_file_urls, part_id, file_infos.at(i)));
            }
          }
          #undef GET_EXT_MOCK_PART_ID
          #undef IS_EXT_MOCK_PART_ID
        }
      }

      //OZ (get_part_id_to_file_urls_map(table_schema, database_schema, is_local_storage, file_urls, file_sizes, schema_guard, exec_ctx, trans, part_id_to_file_urls));
      for (common::hash::ObHashMap<int64_t, ObArray<ObExternalFileInfoTmp> *>::iterator it = part_id_to_file_urls.begin();
          OB_SUCC(ret) && it != part_id_to_file_urls.end(); it++) {
        CK (OB_NOT_NULL(it->second));
        //OZ (get_file_sizes_by_map(*it->second, mapped_sizes, file_urls_to_sizes));
        OZ (update_inner_table_files_list_by_part(trans, tenant_id, table_id, it->first, *it->second));
      }
    }

    for (common::hash::ObHashMap<int64_t, ObArray<ObExternalFileInfoTmp> *>::iterator it = part_id_to_file_urls.begin(); it != part_id_to_file_urls.end(); it++) {
      if (nullptr != it->second) {
        it->second->~ObArray<ObExternalFileInfoTmp>();
        exec_ctx.get_allocator().free(it->second);
        it->second = nullptr;
      }
    }
  }
  return ret;
}

int ObExternalTableFileManager::get_external_file_list_on_device(
    const ObString &location,
    const ObString &pattern,
    const ObExprRegexpSessionVariables &regexp_vars,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes,
    const ObString &access_info,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  sql::ObExternalDataAccessDriver driver;
  if (OB_FAIL(driver.init(location, access_info))) {
    LOG_WARN("init external data access driver failed", K(ret));
  } else if (OB_FAIL(driver.get_file_list(location, pattern, regexp_vars, file_urls, file_sizes, allocator))) {
    LOG_WARN("get file urls failed", K(ret));
  }
  if (driver.is_opened()) {
    driver.close();
  }

  LOG_DEBUG("show external table files", K(file_urls), K(access_info));
  return ret;
}

int ObExternalTableFileManager::update_inner_table_files_list_by_part(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t partition_id,
    const ObIArray<ObExternalFileInfoTmp> &file_infos)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtil::current_time();
  ObSEArray<ObExternalFileInfoTmp, 16> old_file_infos;
  ObSEArray<int64_t, 16> old_file_ids;
  ObSEArray<ObExternalFileInfoTmp, 16> insert_file_infos;
  ObSEArray<int64_t, 16> insert_file_ids;
  ObSEArray<ObExternalFileInfoTmp, 16> update_file_infos;
  ObSEArray<int64_t, 16> update_file_ids;
  ObSEArray<ObExternalFileInfoTmp, 16> delete_file_infos;
  ObSEArray<int64_t, 16> delete_file_ids;
  ObArenaAllocator allocator;
  ObSqlString update_sql;
  ObSqlString insert_sql;
  ObSqlString delete_sql;
  int64_t update_rows = 0;
  int64_t insert_rows = 0;
  int64_t max_file_id = 0;// ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_FILE_ID - 1
  common::hash::ObHashMap<ObString, int64_t> hash_map;
  OZ(get_all_records_from_inner_table(allocator, tenant_id, table_id, partition_id, old_file_infos, old_file_ids));
  OZ(hash_map.create(std::max(file_infos.count(), old_file_infos.count()) + 1, "ExternalFile"));
  for (int64_t i = 0; OB_SUCC(ret) && i < old_file_infos.count(); i++) {
    OZ(hash_map.set_refactored(old_file_infos.at(i).file_url_, old_file_ids.at(i)));
    max_file_id = old_file_ids.at(i) > max_file_id ? old_file_ids.at(i) : max_file_id;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < file_infos.count(); i++) {
    int64_t file_id = 0;
    OZ(hash_map.get_refactored(file_infos.at(i).file_url_, file_id));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      OZ(insert_file_infos.push_back(file_infos.at(i)));
      OZ(insert_file_ids.push_back(++max_file_id));
    } else if (ret == OB_SUCCESS) {
      OZ(update_file_infos.push_back(file_infos.at(i)));
      OZ(update_file_ids.push_back(file_id));
    }
  }
  OZ(hash_map.reuse());
  for (int64_t i = 0; OB_SUCC(ret) && i < file_infos.count(); i++) {
    OZ(hash_map.set_refactored(file_infos.at(i).file_url_, 1));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_file_infos.count(); i++) {
    int64_t existed = 0;
    OZ(hash_map.get_refactored(old_file_infos.at(i).file_url_, existed));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      OZ(delete_file_infos.push_back(old_file_infos.at(i)));
      OZ(delete_file_ids.push_back(old_file_ids.at(i)));
    }
  }
  if (OB_SUCC(ret) && delete_file_infos.count() > 0) {
    OZ(delete_sql.assign_fmt("UPDATE %s SET DELETE_VERSION = %ld WHERE (TABLE_ID, PART_ID, FILE_ID) IN (",
                              OB_ALL_EXTERNAL_TABLE_FILE_TNAME, cur_time));
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_file_infos.count(); i++) {
      OZ(delete_sql.append_fmt("%c(%ld, %ld, %ld)", (0 == i) ? ' ' : ',', table_id, partition_id,
                                                      delete_file_ids.at(i)));
    }
    OZ(delete_sql.append(")"));
    OZ(trans.write(tenant_id, delete_sql.ptr(), update_rows));
  }
  if (OB_SUCC(ret) && update_file_infos.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_file_infos.count(); i++) {
      OZ(update_sql.assign_fmt("UPDATE %s SET"
                              " CREATE_VERSION = CASE WHEN DELETE_VERSION != %ld THEN %ld ELSE CREATE_VERSION end,"
                              " DELETE_VERSION = %ld, FILE_SIZE = %ld WHERE TABLE_ID = %lu AND PART_ID = %lu AND FILE_ID=%ld",
                              OB_ALL_EXTERNAL_TABLE_FILE_TNAME,
                              MAX_VERSION, cur_time,
                              MAX_VERSION, update_file_infos.at(i).file_size_, table_id, partition_id,
                              update_file_ids.at(i)));
      OZ (trans.write(tenant_id, update_sql.ptr(), update_rows));
    }
  }
  if (OB_SUCC(ret) && insert_file_infos.count() > 0) {
    OZ(insert_sql.assign_fmt("INSERT INTO %s(TABLE_ID,PART_ID,FILE_ID,FILE_URL,CREATE_VERSION,DELETE_VERSION,FILE_SIZE) VALUES",
                                OB_ALL_EXTERNAL_TABLE_FILE_TNAME));
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_file_infos.count(); i++) {
        OZ(insert_sql.append_fmt("%c(%lu,%lu,%ld,'%.*s',%ld,%ld,%ld)",
                                  (0 == i) ? ' ' : ',', table_id, partition_id,
                                  insert_file_ids.at(i),
                                  insert_file_infos.at(i).file_url_.length(), insert_file_infos.at(i).file_url_.ptr(),
                                  cur_time, MAX_VERSION, insert_file_infos.at(i).file_size_));
    }
    OZ(trans.write(tenant_id, insert_sql.ptr(), insert_rows));
  }

  return ret;
}

int ObExternalTableFileManager::get_all_records_from_inner_table(ObIAllocator &allocator,
                                                                  int64_t tenant_id,
                                                                  int64_t table_id,
                                                                  int64_t partition_id,
                                                                  ObIArray<ObExternalFileInfoTmp> &file_urls,
                                                                  ObIArray<int64_t> &file_ids)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    OZ (sql.append_fmt("SELECT file_url, file_id FROM %s"
                        " WHERE table_id = %lu AND part_id = %lu",
                        OB_ALL_EXTERNAL_TABLE_FILE_TNAME, table_id, partition_id));
    OZ (GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()));
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObString file_url;
          int64_t file_id;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "file_url", file_url);
          EXTRACT_INT_FIELD_MYSQL(*result, "file_id", file_id, int64_t);
          ObString tmp_url;
          OZ (ob_write_string(allocator, file_url, tmp_url));
          ObExternalFileInfoTmp file_info;
          file_info.file_url_ = tmp_url;
          OZ (file_urls.push_back(file_info));
          OZ (file_ids.push_back(file_id));
        }
        if (OB_FAIL(ret) && OB_ITER_END != ret) {
          LOG_WARN("get next result failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}
int ObExternalTableFileManager::fill_cache_from_inner_table(
    const ObExternalTableFilesKey &key,
    const ObExternalTableFiles *&ext_files,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;

  //only one worker need do the job
  int64_t bucket_id = key.hash() % LOAD_CACHE_LOCK_CNT;
  int64_t total_wait_secs = 0;

  while (OB_FAIL(fill_cache_locks_[bucket_id].lock(LOCK_TIMEOUT))
         && OB_TIMEOUT == ret && !THIS_WORKER.is_timeout()) {
    total_wait_secs += LOAD_CACHE_LOCK_CNT;
    LOG_WARN("fill external table cache wait", K(total_wait_secs));
  }
  if (OB_SUCC(ret)) {
    //try fetch again
    if (OB_FAIL(kv_cache_.get(key, ext_files, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get from KVCache", K(ret), K(key));
      }
    }

    if ((OB_SUCC(ret) && is_cache_value_timeout(*ext_files))
        || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        ObSqlString sql;
        int64_t cur_time = ObTimeUtil::current_time();

        if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
        }

        OZ (sql.append_fmt("SELECT file_url, file_id, file_size FROM %s"
                           " WHERE table_id = %lu AND part_id = %lu"
                           " AND create_version <=%ld AND %ld < delete_version",
                           OB_ALL_EXTERNAL_TABLE_FILE_TNAME, key.table_id_, key.partition_id_,
                           cur_time, cur_time));
        OZ (GCTX.sql_proxy_->read(res, key.tenant_id_, sql.ptr()));

        if (OB_SUCC(ret)) {
          if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret));
          } else {
            ObSEArray<ObString, 16> temp_file_urls;
            ObSEArray<int64_t, 16> temp_file_ids;
            ObSEArray<int64_t, 16> temp_file_sizes;
            ObArenaAllocator allocator;
            while (OB_SUCC(result->next())) {
              ObString file_url;
              ObString tmp_url;
              int64_t file_id = INT64_MAX;
              int64_t file_size = 0;
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "file_url", tmp_url);
              EXTRACT_INT_FIELD_MYSQL(*result, "file_id", file_id, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "file_size", file_size, int64_t);
              OZ (ob_write_string(allocator, tmp_url, file_url));
              OZ (temp_file_urls.push_back(file_url));
              OZ (temp_file_ids.push_back(file_id));
              OZ (temp_file_sizes.push_back(file_size));
            }
            if (OB_FAIL(ret) && OB_ITER_END != ret) {
              LOG_WARN("get next result failed", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
            if (OB_SUCC(ret)) {
              ObExternalTableFiles temp_ext_files;
              temp_ext_files.create_ts_ = cur_time;
              temp_ext_files.file_urls_ = ObArrayWrap<ObString>(temp_file_urls.get_data(), temp_file_urls.count());
              temp_ext_files.file_ids_ = ObArrayWrap<int64_t>(temp_file_ids.get_data(), temp_file_ids.count());
              temp_ext_files.file_sizes_ = ObArrayWrap<int64_t>(temp_file_sizes.get_data(), temp_file_sizes.count());
              OZ (kv_cache_.put_and_fetch(key, temp_ext_files, ext_files, handle, true));
            }
            LOG_TRACE("external table file urls", K(temp_file_urls), K(key));
          }
        }
      }
      LOG_TRACE("external table fill cache", K(ext_files), K(key));
    }
  }
  if (fill_cache_locks_[bucket_id].self_locked()) {
    fill_cache_locks_[bucket_id].unlock();
  }
  return ret;
}

int ObExternalTableFileManager::lock_for_refresh(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t object_id)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = NULL;
  if (OB_ISNULL(conn = dynamic_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    ObLockObjRequest lock_arg;
    lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_EXTERNAL_TABLE_REFRESH;
    lock_arg.obj_id_ = object_id;
    lock_arg.lock_mode_ = EXCLUSIVE;
    lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = 1000L * 1000L * 2; //2s
    if (OB_FAIL(lock_arg.owner_id_.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                      get_tid_cache()))) {
      LOG_WARN("failed to get owner id", K(ret), K(get_tid_cache()));
    } else {
      while (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, lock_arg, conn)) && !THIS_WORKER.is_timeout()) {
        LOG_WARN("lock failed try again", K(ret));
      }
    }
  }


  return ret;
}

int ObExternalTableFileManager::flush_external_file_cache(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t part_id,
    const ObIArray<ObAddr> &all_servers)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObAsyncRpcTaskWaitContext<ObRpcAsyncFlushExternalTableKVCacheCallBack> context;
  int64_t send_task_count = 0;
  OZ (context.init());
  OZ (context.get_cb_list().reserve(all_servers.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < all_servers.count(); i++) {
    ObFlushExternalTableFileCacheReq req;
    int64_t timeout = ObExternalTableFileManager::CACHE_EXPIRE_TIME;
    req.tenant_id_ = tenant_id;
    req.table_id_ = table_id;
    req.partition_id_ = part_id;
    ObRpcAsyncFlushExternalTableKVCacheCallBack* async_cb = nullptr;
    if (OB_ISNULL(async_cb = OB_NEWx(ObRpcAsyncFlushExternalTableKVCacheCallBack, (&allocator), (&context)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async cb memory", K(ret));
    }
    OZ (context.get_cb_list().push_back(async_cb));
    OZ (GCTX.external_table_proxy_->to(all_servers.at(i))
                                            .by(tenant_id)
                                            .timeout(timeout)
                                            .flush_file_kvcahce(req, async_cb));
    if (OB_SUCC(ret)) {
      send_task_count++;
    }
  }

  context.set_task_count(send_task_count);

  do {
    int temp_ret = context.wait_executing_tasks();
    if (OB_SUCCESS != temp_ret) {
      LOG_WARN("fail to wait executing task", K(temp_ret));
      if (OB_SUCC(ret)) {
        ret = temp_ret;
      }
    }
  } while(0);

  for (int64_t i = 0; OB_SUCC(ret) && i < context.get_cb_list().count(); i++) {
    ret = context.get_cb_list().at(i)->get_task_resp().rcode_.rcode_;
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        // flush timeout is OK, because the file cache has already expire
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("async flush kvcache process failed", K(ret));
      }
    }
  }
  for (int64_t i = 0; i < context.get_cb_list().count(); i++) {
    context.get_cb_list().at(i)->~ObRpcAsyncFlushExternalTableKVCacheCallBack();
  }
  return ret;
}

int ObExternalTableFileManager::refresh_external_table(const uint64_t tenant_id,
                                                       const uint64_t table_id,
                                                       ObSchemaGetterGuard &schema_guard,
                                                       ObExecContext &exec_ctx) {
  int ret = OB_SUCCESS;
  ObArray<ObString> file_urls;
  ObArray<int64_t> file_sizes;
  ObExprRegexpSessionVariables regexp_vars;
  const ObTableSchema *table_schema = NULL;
  OZ (schema_guard.get_table_schema(tenant_id,
                                    table_id,
                                    table_schema));
  CK (table_schema != NULL);
  OZ (refresh_external_table(tenant_id, table_schema, exec_ctx));
  return ret;
}

int ObExternalTableFileManager::refresh_external_table(const uint64_t tenant_id,
                                                       const ObTableSchema *table_schema,
                                                       ObExecContext &exec_ctx) {
  int ret = OB_SUCCESS;
  ObArray<ObString> file_urls;
  ObArray<int64_t> file_sizes;
  ObExprRegexpSessionVariables regexp_vars;
  CK (table_schema != NULL);
  CK (exec_ctx.get_my_session() != NULL);
  if (OB_SUCC(ret) && ObSQLUtils::is_external_files_on_local_disk(table_schema->get_external_file_location())) {
    OZ (ObSQLUtils::check_location_access_priv(table_schema->get_external_file_location(), exec_ctx.get_my_session()));
  }
  ObSqlString full_path;
  CK (GCTX.location_service_);
  OZ (exec_ctx.get_my_session()->get_regexp_session_vars(regexp_vars));
  OZ (ObExternalTableUtils::collect_external_file_list(
              tenant_id,
              table_schema->get_table_id(),
              table_schema->get_external_file_location(),
              table_schema->get_external_file_location_access_info(),
              table_schema->get_external_file_pattern(), regexp_vars, exec_ctx.get_allocator(),
              full_path,
              file_urls, file_sizes));
  //TODO [External Table] opt performance
  ObSEArray<ObAddr, 8> all_servers;
  OZ (GCTX.location_service_->external_table_get(tenant_id, table_schema->get_table_id(), all_servers));
  OZ (ObExternalTableFileManager::get_instance().update_inner_table_file_list(exec_ctx, tenant_id, table_schema->get_table_id(), file_urls, file_sizes));
  if (OB_SUCC(ret)) {
    if (table_schema->is_partitioned_table()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_num(); i++) {
        CK (OB_NOT_NULL(table_schema->get_part_array()[i]));
        OZ (ObExternalTableFileManager::get_instance().flush_external_file_cache(tenant_id, table_schema->get_table_id(),
        table_schema->get_part_array()[i]->get_part_id(), all_servers));
      }
    } else {
      OZ (ObExternalTableFileManager::get_instance().flush_external_file_cache(tenant_id, table_schema->get_table_id(),
        table_schema->get_table_id(),  all_servers));
    }
  }
  return ret;
}

int ObExternalTableFileManager::auto_refresh_external_table(ObExecContext &exec_ctx, const int64_t interval) {
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  CK (exec_ctx.get_my_session() != NULL);
  CK (exec_ctx.get_sql_ctx()->schema_guard_ != NULL);
  CK (OB_NOT_NULL(GCTX.sql_proxy_),
      OB_NOT_NULL(GCTX.schema_service_));
  uint64_t tenant_id = 0;
  if (OB_SUCC(ret)) {
    tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();
  }
  OZ (trans.start(GCTX.sql_proxy_, tenant_id));
  if (OB_SUCC(ret)) {
    if (interval == 0) {
      ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
      OZ (exec_ctx.get_sql_ctx()->schema_guard_->get_table_schemas_in_tenant(tenant_id, table_schemas));
      for (int i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
        const ObSimpleTableSchemaV2 *simple_table = table_schemas.at(i);
        CK (simple_table != NULL);
        if (OB_SUCC(ret) && simple_table->get_table_type() == ObTableType::EXTERNAL_TABLE) {
          const ObTableSchema *table_schema = NULL;
          OZ (exec_ctx.get_sql_ctx()->schema_guard_->get_table_schema(tenant_id, simple_table->get_table_id(), table_schema));
          CK (table_schema != NULL);
          if (OB_SUCC(ret) && (2 == ((table_schema->get_table_flags() & 0B1100) >> 2))) {
            OZ (refresh_external_table(tenant_id, simple_table->get_table_id(), *exec_ctx.get_sql_ctx()->schema_guard_, exec_ctx));
          }
        }
      }
    } else if (interval == -1) {
      OZ (delete_auto_refresh_job(exec_ctx, trans));
    } else if (interval > 0) {
      OZ (delete_auto_refresh_job(exec_ctx, trans));
      OZ (create_auto_refresh_job(exec_ctx, interval, trans));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "interval value");
      LOG_WARN("interval not supported", K(ret), K(interval));
    }

  }
  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    bool commit = OB_SUCC(ret);
    if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(temp_ret));
    }
  }
  return ret;
}


int ObExternalTableFileManager::delete_auto_refresh_job(ObExecContext &ctx, ObMySQLTransaction &trans) {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
          "delete from %s where tenant_id = %lu and job_name= '%s'",
          share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
          0UL, auto_refresh_job_name))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_ISNULL(ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    } else if (OB_FAIL(trans.write(ctx.get_my_session()->get_effective_tenant_id(), sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr());
    }
  }
  return ret;
}

int ObExternalTableFileManager::create_repeat_job_sql_(const bool is_oracle_mode,
                                                      const uint64_t tenant_id,
                                                      const int64_t job_id,
                                                      const char *job_name,
                                                      const ObString &exec_env,
                                                      const int64_t start_usec,
                                                      ObSqlString &job_action,
                                                      ObSqlString &interval,
                                                      const int64_t interval_ts,
                                                      ObSqlString &raw_sql)
{
  int ret = OB_SUCCESS;
  int64_t end_date = 64060560000000000;//4000-01-01 00:00:00.000000
  int64_t default_duration_sec = 24 * 60 * 60; //one day
  share::ObDMLSqlSplicer dml;
  OZ (dml.add_pk_column("tenant_id", 0));
  OZ (dml.add_column("job_name", ObHexEscapeSqlStr(ObString(job_name))));
  OZ (dml.add_pk_column("job", job_id));
  OZ (dml.add_column("lowner", is_oracle_mode ? "SYS" : "root@%"));
  OZ (dml.add_column("powner", is_oracle_mode ? "SYS" : "root@%"));
  OZ (dml.add_column("cowner", is_oracle_mode ? "SYS" : "oceanbase"));
  OZ (dml.add_time_column("next_date", start_usec));
  OZ (dml.add_column("total", 0));
  OZ (dml.add_column("`interval#`", ObHexEscapeSqlStr(interval.string()))); //ObString("FREQ=SECONDLY; INTERVAL=1")
  OZ (dml.add_column("flag", 0));
  OZ (dml.add_column("what", ObHexEscapeSqlStr(job_action.string())));
  OZ (dml.add_column("nlsenv", ""));
  OZ (dml.add_column("field1", ""));
  OZ (dml.add_column("exec_env", ObHexEscapeSqlStr(exec_env)));
  OZ (dml.add_column("job_style", "REGULER"));
  OZ (dml.add_column("program_name", ""));
  OZ (dml.add_column("job_type", "STORED_PROCEDURE"));
  OZ (dml.add_column("job_action", ObHexEscapeSqlStr(job_action.string())));
  OZ (dml.add_column("number_of_argument", 0));
  OZ (dml.add_time_column("start_date", start_usec));
  OZ (dml.add_column("repeat_interval", ObHexEscapeSqlStr(interval.string()))); //ObString("FREQ=SECONDLY; INTERVAL=1")
  OZ (dml.add_raw_time_column("end_date", end_date));
  OZ (dml.add_column("job_class", "DEFAULT_JOB_CLASS"));
  OZ (dml.add_column("enabled", true));
  OZ (dml.add_column("auto_drop", false));
  OZ (dml.add_column("comments", "used to auto refresh external tables"));
  OZ (dml.add_column("credential_name", ""));
  OZ (dml.add_column("destination_name", ""));
  OZ (dml.add_column("interval_ts", interval_ts));
  OZ (dml.add_column("max_run_duration", default_duration_sec));
  OZ (dml.splice_values(raw_sql));
  return ret;
}

int ObExternalTableFileManager::create_auto_refresh_job(ObExecContext &ctx, const int64_t interval, ObMySQLTransaction &trans) {
  int ret = OB_SUCCESS;
  #ifndef ALL_TENANT_SCHEDULER_JOB_COLUMN_NAME
  #define ALL_TENANT_SCHEDULER_JOB_COLUMN_NAME  "tenant_id, " \
                                              "job_name, " \
                                              "job, "  \
                                              "lowner, " \
                                              "powner, " \
                                              "cowner, "  \
                                              "next_date,"      \
                                              "total,"          \
                                              "`interval#`,"     \
                                              "flag," \
                                              "what," \
                                              "nlsenv,"    \
                                              "field1,"        \
                                              "exec_env,"\
                                              "job_style,"\
                                              "program_name,"\
                                              "job_type,"\
                                              "job_action,"\
                                              "number_of_argument,"\
                                              "start_date,"\
                                              "repeat_interval,"\
                                              "end_date,"\
                                              "job_class,"\
                                              "enabled,"\
                                              "auto_drop,"\
                                              "comments,"\
                                              "credential_name,"\
                                              "destination_name,"\
                                              "interval_ts,"\
                                              "max_run_duration"
  #endif
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  CK (ctx.get_my_session() != NULL);
  OZ (sql::ObExecEnv::gen_exec_env(*ctx.get_my_session(), buf, OB_MAX_PROC_ENV_LENGTH, pos));
  ObString exec_env(pos, buf);
  ObCommonID raw_id;
  bool is_oracle_mode = false;
  OZ (ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(ctx.get_my_session()->get_effective_tenant_id(), is_oracle_mode));
  OZ (storage::ObCommonIDUtils::gen_unique_id(ctx.get_my_session()->get_effective_tenant_id(), raw_id));
  int64_t max_job_id = raw_id.id() + dbms_scheduler::ObDBMSSchedTableOperator::JOB_ID_OFFSET;
  ObSqlString raw_sql;
  OZ (raw_sql.append_fmt("INSERT INTO %s( "ALL_TENANT_SCHEDULER_JOB_COLUMN_NAME") VALUES ",
                                  share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME));
  uint64_t start_usec = ObTimeUtility::current_time();
  ObSqlString job_action;
  ObSqlString interval_str;
  int64_t interval_ts = 1000000L * interval;
  OZ (job_action.append("dbms_external_table.auto_refresh_external_table()"));
  OZ (interval_str.append_fmt("FREQ=SECONDLY; INTERVAL=%ld", interval));
  ObSqlString tmp_sql;
  OZ (create_repeat_job_sql_(is_oracle_mode, 0, 0, auto_refresh_job_name, exec_env, start_usec, job_action, interval_str, interval_ts, tmp_sql));
  OZ (raw_sql.append_fmt("(%s)", tmp_sql.ptr()));
  tmp_sql.reset();
  OZ (create_repeat_job_sql_(is_oracle_mode, 0, max_job_id, auto_refresh_job_name, exec_env, start_usec, job_action, interval_str, interval_ts, tmp_sql));
  OZ (raw_sql.append_fmt(",(%s);", tmp_sql.ptr()));
  int64_t affected_rows = 0;
  OZ (trans.write(ctx.get_my_session()->get_effective_tenant_id(), raw_sql.ptr(), affected_rows));
  CK (affected_rows == 2);
  return ret;

}

OB_SERIALIZE_MEMBER(ObExternalFileInfo, file_url_, file_id_, file_addr_, file_size_, part_id_);

}
}
