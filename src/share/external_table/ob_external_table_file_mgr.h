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

#ifndef _OB_EXTERNAL_TABLE_FILE_MANAGER_H_
#define _OB_EXTERNAL_TABLE_FILE_MANAGER_H_

#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "src/sql/resolver/ob_stmt_resolver.h"

namespace oceanbase {
namespace sql {
class ObExprRegexpSessionVariables;
class ObAlterTableStmt;
}

namespace share {

struct ObExternalFileInfo {
  ObExternalFileInfo() : file_id_(INT64_MAX), part_id_(0), file_size_(0) {}
  common::ObString file_url_;
  int64_t file_id_;
  int64_t part_id_;
  common::ObAddr file_addr_;
  int64_t file_size_;
  TO_STRING_KV(K_(file_url), K_(file_id), K_(part_id), K_(file_addr), K_(file_size));
  OB_UNIS_VERSION(1);
};

class ObExternalTableFilesKey : public ObIKVCacheKey
{
public:
  ObExternalTableFilesKey() : tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    partition_id_(OB_INVALID_ID)
  {}
  virtual ~ObExternalTableFilesKey() {}
  bool operator ==(const ObIKVCacheKey &other) const override {
    const ObExternalTableFilesKey &other_key = reinterpret_cast<const ObExternalTableFilesKey&>(other);
    return this->tenant_id_ == other_key.tenant_id_
        && this->table_id_ == other_key.table_id_
        && this->partition_id_ == other_key.partition_id_;
  }
  uint64_t hash() const override {
    return common::murmurhash(this, sizeof(ObExternalTableFilesKey), 0);
  }
  uint64_t get_tenant_id() const override { return tenant_id_; }
  int64_t size() const override { return sizeof(*this); }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(partition_id_));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t partition_id_;
};

class ObExternalTableFiles : public ObIKVCacheValue
{
public:
  ObExternalTableFiles() : create_ts_(0) {}
  virtual ~ObExternalTableFiles() {}
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K(file_urls_), K(file_ids_), K(create_ts_));
public:
  ObArrayWrap<ObString> file_urls_;
  ObArrayWrap<int64_t> file_ids_;
  ObArrayWrap<int64_t> file_sizes_;
  int64_t create_ts_;
};

class ObExternalTableFileManager
{
private:
  struct ObExternalFileInfoTmp {
    ObExternalFileInfoTmp(common::ObString file_url, int64_t file_size, int64_t part_id) :
                          file_url_(file_url), file_size_(file_size), part_id_(part_id) {}
    ObExternalFileInfoTmp() : file_url_(), file_size_(0), part_id_(0) {}
    common::ObString file_url_;
    int64_t file_size_;
    int64_t part_id_;
    TO_STRING_KV(K_(file_url),K_(part_id), K_(file_size));
  };

public:
  static const int64_t CACHE_EXPIRE_TIME = 20 * 1000000L; //20s
  static const int64_t MAX_VERSION = INT64_MAX;
  static const int64_t LOAD_CACHE_LOCK_CNT = 16;
  static const int64_t LOCK_TIMEOUT = 2 * 1000000L;

  const char* auto_refresh_job_name = "auto_refresh_external_table_job";
  const char ip_delimiter = '%';

  ObExternalTableFileManager() {}

  int init();

  static ObExternalTableFileManager &get_instance();

  int get_external_files_by_part_ids(
      const uint64_t tenant_id,
      const uint64_t table_id,
      ObIArray<int64_t> &partition_ids,
      const bool is_local_file_on_disk,
      common::ObIAllocator &allocator,
      common::ObIArray<ObExternalFileInfo> &external_files,
      common::ObIArray<ObNewRange *> *range_filter = NULL);

  int get_external_files(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const bool is_local_file_on_disk,
      common::ObIAllocator &allocator,
      common::ObIArray<ObExternalFileInfo> &external_files,
      common::ObIArray<ObNewRange *> *range_filter = NULL);

  int get_external_files_by_part_id(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t partition_id,
      const bool is_local_file_on_disk,
      common::ObIAllocator &allocator,
      common::ObIArray<ObExternalFileInfo> &external_files,
      common::ObIArray<ObNewRange *> *range_filter = NULL);

  int flush_cache(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t part_id);

  int update_inner_table_file_list(sql::ObExecContext &exec_ctx,
                                  const uint64_t tenant_id,
                                  const uint64_t table_id,
                                  ObIArray<ObString> &file_urls,
                                  ObIArray<int64_t> &file_sizes,
                                  const uint64_t part_id = -1);

  int get_all_records_from_inner_table(ObIAllocator &allocator,
                                    int64_t tenant_id,
                                    int64_t table_id,
                                    int64_t partition_id,
                                    ObIArray<ObExternalFileInfoTmp> &file_infos,
                                    ObIArray<int64_t> &file_ids);
  int clear_inner_table_files(
      const uint64_t tenant_id,
      const uint64_t table_id,
      ObMySQLTransaction &trans);

  int clear_inner_table_files_within_one_part(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t part_id,
    ObMySQLTransaction &trans);

  int get_external_file_list_on_device(const common::ObString &location,
                                       const common::ObString &pattern,
                                       const sql::ObExprRegexpSessionVariables &regexp_vars,
                                       common::ObIArray<common::ObString> &file_urls,
                                       common::ObIArray<int64_t> &file_sizes,
                                       const common::ObString &access_info,
                                       common::ObIAllocator &allocator);

  int flush_external_file_cache(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t part_id,
    const ObIArray<ObAddr> &all_servers);

  int refresh_external_table(const uint64_t tenant_id,
                            const uint64_t table_id,
                            ObSchemaGetterGuard &schema_guard,
                            ObExecContext &exec_ctx);

  int refresh_external_table(const uint64_t tenant_id,
                              const ObTableSchema *table_schema,
                              ObExecContext &exec_ctx);

  int auto_refresh_external_table(ObExecContext &exec_ctx, const int64_t interval);
private:
  int delete_auto_refresh_job(ObExecContext &exec_ctx, ObMySQLTransaction &trans);
  int create_auto_refresh_job(ObExecContext &ctx, const int64_t interval, ObMySQLTransaction &trans);
  int update_inner_table_files_list_by_part(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t partition_id,
      const ObIArray<ObExternalFileInfoTmp> &file_infos);

    int update_inner_table_files_list_by_table(
    sql::ObExecContext &exec_ctx,
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObExternalFileInfoTmp> &file_infos);

  bool is_cache_value_timeout(const ObExternalTableFiles &ext_files) {
    return ObTimeUtil::current_time() - ext_files.create_ts_ > CACHE_EXPIRE_TIME;
  }
  int fill_cache_from_inner_table(
      const ObExternalTableFilesKey &key,
      const ObExternalTableFiles *&ext_files,
      ObKVCacheHandle &handle);
  int lock_for_refresh(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t object_id);

  int construct_add_partition_sql(ObSqlString &sql,
                                  const ObString &database_name,
                                  const ObString &table_name,
                                  const ObString &partition_name,
                                  common::ObFixedArray<ObObj, ObIAllocator> &val_list);

  int build_row_for_file_name(ObNewRow &row, ObIAllocator &allocator);

  int get_genarated_expr_from_partition_column(const ObColumnSchemaV2 *column_schema,
                                               const ObTableSchema *table_schema,
                                              ObSQLSessionInfo *session_info,
                                              ObRawExprFactory *expr_factory,
                                              ObSchemaGetterGuard &schema_guard,
                                              ObIAllocator &allocator,
                                              ObRawExpr *&gen_expr);

  int cg_expr_by_mocking_field_expr(const ObTableSchema *table_schema,
                                    ObRawExpr *gen_expr,
                                    ObSQLSessionInfo *session_info,
                                    ObRawExprFactory *expr_factory,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    ObIAllocator &allocator,
                                    ObTempExpr *&temp_expr);

  int cg_partition_expr_rt_expr(const ObTableSchema *table_schema,
                                ObRawExprFactory *expr_factory,
                                ObSQLSessionInfo *session_info,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                ObIAllocator &allocator,
                                ObIArray<ObTempExpr *> &temp_exprs);
  int get_all_partition_list_val(const ObTableSchema *table_schema, ObIArray<ObNewRow> &part_vals, ObIArray<int64_t> &part_ids);
  int calculate_file_part_val_by_file_name(const ObTableSchema *table_schema,
                                          const ObIArray<ObExternalFileInfoTmp> &file_infos,
                                          ObIArray<ObNewRow> &part_vals,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          ObExecContext &exec_ctx);
  int find_partition_existed(ObIArray<ObNewRow> &existed_part,
                            ObNewRow &file_part_val,
                            int64_t &found);
  int get_part_id_to_file_urls_map(const ObTableSchema *table_schema,
                                    const ObDatabaseSchema *database_schema,
                                    const bool is_local_storage,
                                    ObIArray<ObString> &file_urls,
                                    ObIArray<int64_t> &file_sizes,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    ObExecContext &exec_ctx,
                                    ObMySQLTransaction &trans,
                                    common::hash::ObHashMap<int64_t, ObIArray<ObString> *> &hash_map);

  int get_file_sizes_by_map(ObIArray<ObString> &file_urls,
                            ObIArray<int64_t> &file_sizes,
                            common::hash::ObHashMap<ObString, int64_t> &map);

  int calculate_all_files_partitions(share::schema::ObSchemaGetterGuard &schema_guard,
                                                             ObExecContext &exec_ctx,
                                                             const ObTableSchema *table_schema,
                                                             const ObIArray<ObExternalFileInfoTmp> &file_infos,
                                                             ObIArray<int64_t> &file_part_ids,
                                                             ObIArray<ObNewRow> &partitions_to_add,
                                                             ObIArray<ObPartition *> &partitions_to_del);

  int add_item_to_map(ObIAllocator &allocator,
                      common::hash::ObHashMap<int64_t, ObArray<ObExternalFileInfoTmp> *> &hash_map,
                      int64_t part_id, const ObExternalFileInfoTmp &file_info);

  int create_alter_table_stmt(sql::ObExecContext &exec_ctx,
                              const ObTableSchema *table_schema,
                              const ObDatabaseSchema *database_schema,
                              const int64_t part_num,
                              const ObAlterTableArg::AlterPartitionType alter_part_type,
                              ObAlterTableStmt *&alter_table_stmt);

  int alter_partition_for_ext_table(ObMySQLTransaction &trans,
                                    sql::ObExecContext &exec_ctx,
                                    ObAlterTableStmt *alter_table_stmt,
                                    ObIArray<int64_t> &file_part_ids);

  int add_partition_for_alter_stmt(ObAlterTableStmt *&alter_table_stmt,
                                  const ObString &part_name,
                                  ObNewRow &part_val);

  int create_repeat_job_sql_(const bool is_oracle_mode,
                            const uint64_t tenant_id,
                            const int64_t job_id,
                            const char *job_name,
                            const ObString &exec_env,
                            const int64_t start_usec,
                            ObSqlString &job_action,
                            ObSqlString &interval,
                            const int64_t interval_ts,
                            ObSqlString &raw_sql);

private:
  common::ObSpinLock fill_cache_locks_[LOAD_CACHE_LOCK_CNT];
  common::ObKVCache<ObExternalTableFilesKey, ObExternalTableFiles> kv_cache_;
};


}
}

#endif /* _OB_EXTERNAL_TABLE_FILE_MANAGER_H_ */
