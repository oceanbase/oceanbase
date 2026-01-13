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
#include "src/sql/resolver/ob_stmt_resolver.h"
#include "sql/engine/table/ob_odps_table_row_iter.h"

namespace oceanbase {
namespace sql {
class ObExprRegexpSessionVariables;
class ObAlterTableStmt;
}

namespace share {

struct ObExternalTableBasicFileInfo
{
  ObExternalTableBasicFileInfo() :
    url_(), size_(OB_INVALID_SIZE), last_modify_time_(OB_INVALID_TIMESTAMP), content_digest_()
  {}
  ObExternalTableBasicFileInfo(const ObString &url, const int64_t size) :
    url_(url),
    size_(size), last_modify_time_(OB_INVALID_TIMESTAMP), content_digest_()
  {}
  ObExternalTableBasicFileInfo(const ObString &url, const int64_t size,
                               const int64_t last_modify_time, const ObString &digest) :
    url_(url),
    size_(size), last_modify_time_(last_modify_time), content_digest_(digest)
  {}
  void reset()
  {
    size_ = OB_INVALID_SIZE;
    last_modify_time_ = OB_INVALID_TIMESTAMP;
    url_.reset();
    content_digest_.reset();
  }
  bool is_valid() const
  {
    return (!url_.empty() && size_ != OB_INVALID_SIZE && last_modify_time_ != OB_INVALID_TIMESTAMP
            && !content_digest_.empty());
  }
  TO_STRING_KV(K_(url), K_(size), K_(last_modify_time), K_(content_digest));
  ObString url_;
  int64_t size_;
  int64_t last_modify_time_;
  ObString content_digest_;
};

struct ObExternalFileInfo
{
  ObExternalFileInfo() :
    file_id_(INT64_MAX), part_id_(0), file_size_(OB_INVALID_SIZE), row_start_(0), row_count_(0),
    pos_del_files_(), modify_time_(OB_INVALID_TIMESTAMP), content_digest_()
  {}
  common::ObString file_url_;
  int64_t file_id_;
  int64_t part_id_;
  common::ObAddr file_addr_;
  int64_t file_size_;
  int64_t row_start_;
  int64_t row_count_;
  common::ObString session_id_;
  common::ObSEArray<common::ObString, 4> pos_del_files_;
  int64_t modify_time_;
  common::ObString content_digest_;
  int deep_copy(ObIAllocator &allocator, const ObExternalFileInfo &other);
  int assign(const ObExternalFileInfo &other);
  TO_STRING_KV(K_(file_url), K_(file_id), K_(part_id), K_(file_addr), K_(file_size),
               K_(modify_time), K_(row_start), K_(row_count), K_(session_id), K_(content_digest),
               K_(pos_del_files));
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

class ObExternalTableFileListKey : public ObIKVCacheKey
{
public:
  ObExternalTableFileListKey() : tenant_id_(OB_INVALID_ID), path_(nullptr)
  {
  }
  virtual ~ObExternalTableFileListKey() = default;
  bool operator==(const ObIKVCacheKey &other) const override
  {
    const ObExternalTableFileListKey &other_key
        = reinterpret_cast<const ObExternalTableFileListKey &>(other);
    return this->tenant_id_ == other_key.tenant_id_
           && this->path_.case_compare(other_key.path_) == 0;
  }
  uint64_t hash() const override;
  uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  int64_t size() const override
  {
    return sizeof(*this) + path_.length() + 1;
  }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K(tenant_id_), K(path_));

public:
  uint64_t tenant_id_;
  ObString path_;
};

class ObExternalTablePartitionsKey : public ObIKVCacheKey
{
public:
  ObExternalTablePartitionsKey() : tenant_id_(OB_INVALID_ID), catalog_id_(OB_INVALID_ID)
  {
  }
  virtual ~ObExternalTablePartitionsKey() = default;
  bool operator==(const ObIKVCacheKey &other) const override;
  uint64_t hash() const override;
  uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  int64_t size() const override
  {
    return sizeof(ObExternalTablePartitionsKey) + db_name_.length() + 1 + tb_name_.length() + 1;
  }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(db_name_), K(tb_name_));

public:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObString db_name_;
  ObString tb_name_;
};

struct PartitionInfo
{
  PartitionInfo() : partition_(), path_(), modify_ts_(0)
  {
  }
  // 分区，比如 pa=a/pb=b/pc=c
  ObString partition_;
  // 该分区对应的文件路径，比如 hdfs://abc/def
  ObString path_;
  // 该分区的分区值，比如 [a,b,c]
  ObArrayWrap<ObString> partition_values_;
  int64_t modify_ts_;
  TO_STRING_KV(K_(partition), K_(path), K_(modify_ts));

  int64_t get_size() const
  {
    int64_t size = sizeof(PartitionInfo)
                   + sizeof(ObString) + partition_.length() + 1
                   + sizeof(ObString) + path_.length() + 1;
    for (int64_t i = 0; i < partition_values_.count(); ++i) {
      size += sizeof(ObString) + partition_values_.at(i).length() + 1;
    }
    return size;
  }
};

class ObExternalTablePartitions : public ObIKVCacheValue
{
public:
  ObExternalTablePartitions() : create_ts_(0), partition_infos_()
  {
  }
  virtual ~ObExternalTablePartitions() = default;
  int64_t size() const override
  {
    int64_t size = sizeof(ObExternalTablePartitions);
    for (int i = 0; i < partition_infos_.count(); ++i) {
      size += partition_infos_.at(i).get_size();
    }

    return size;
  }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K(create_ts_), K(partition_infos_));

public:
  int64_t create_ts_;  // ms
  ObArrayWrap<PartitionInfo> partition_infos_;
};


class ObExternalTableFiles : public ObIKVCacheValue
{
public:
  ObExternalTableFiles() : create_ts_(0), modify_ts_(0) {}
  virtual ~ObExternalTableFiles() {}
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K(file_urls_), K(content_digests_), K(file_ids_), K(modify_times_), K(create_ts_), K(modify_ts_));
public:
  ObArrayWrap<ObString> file_urls_;
  ObArrayWrap<ObString> content_digests_;
  ObArrayWrap<int64_t> file_ids_;
  ObArrayWrap<int64_t> file_sizes_;
  ObArrayWrap<int64_t> modify_times_;  // 记录每个文件自己的修改时间
  int64_t create_ts_; // cache 的创建时间，用于标记 cache 刷新
  int64_t modify_ts_; // 当前文件列表整体的最新修改时间，用来标记当前文件列表是否有变更。
                      // 比如：对于文件存储来说，就是文件列表所属上一层目录的修改时间
                      //      对于hive来说，就是分区信息或者表属性的ddl时间
};

class ObExternalTableFileManager
{
public:
  struct ObExternalFileInfoTmp
  {
    ObExternalFileInfoTmp(common::ObString file_url, common::ObString content_digest,
                          int64_t file_size, int64_t modify_time, int64_t part_id,
                          int64_t delete_version = MAX_VERSION) :
      file_url_(file_url),
      content_digest_(content_digest), file_size_(file_size), modify_time_(modify_time),
      part_id_(part_id), delete_version_(delete_version)
    {}
    ObExternalFileInfoTmp() :
      file_url_(), content_digest_(), file_size_(0), modify_time_(0), part_id_(0),
      delete_version_(0)
    {}
    common::ObString file_url_;
    common::ObString content_digest_;
    int64_t file_size_;
    int64_t modify_time_;
    int64_t part_id_;
    int64_t delete_version_;
    TO_STRING_KV(K_(file_url), K_(part_id), K_(file_size), K_(modify_time), K_(delete_version),
                 K_(content_digest));
  };

  static const int64_t CACHE_EXPIRE_TIME = 20 * 1000000L; //20s
  static const int64_t MAX_VERSION = INT64_MAX;
  static const int64_t LOAD_CACHE_LOCK_CNT = 16;
  static const int64_t LOCK_TIMEOUT = 2 * 1000000L;

  const char* auto_refresh_job_name = "auto_refresh_external_table_job";
  const char ip_delimiter = '%';
  const char equals_delimiter = '=';

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

  int get_mocked_external_table_files(
      const uint64_t tenant_id,
      ObIArray<int64_t> &partition_ids,
      sql::ObExecContext &ctx,
      const ObDASScanCtDef &das_ctdef,
      ObIArray<ObExternalFileInfo> &external_files);

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

  int update_inner_table_file_list(
    sql::ObExecContext &exec_ctx, const uint64_t tenant_id, const uint64_t table_id,
    common::ObArray<share::ObExternalTableBasicFileInfo> &basic_file_infos,
    common::ObIArray<uint64_t> &updated_part_ids, bool &has_partition_changed,
    const uint64_t part_id = -1, bool collect_statistic = true);

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

  int get_external_file_list_on_device_with_cache(
    const common::ObString &location,
    const uint64_t tenant_id,
    const uint64_t ts,
    const common::ObString &pattern,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObExternalTableFiles &external_table_files,
    const common::ObString &access_info,
    common::ObIAllocator &allocator,
    int64_t refresh_interval_ms);

  int get_external_file_list_on_device_with_cache(
      const ObIArray<common::ObString> &location,
      const uint64_t tenant_id,
      const ObIArray<int64_t> &part_id,
      const ObIArray<int64_t> &part_modify_ts,
      const common::ObString &pattern,
      const common::ObString &access_info,
      common::ObIAllocator &allocator,
      int64_t refresh_interval_ms,
      ObIArray<ObExternalTableFiles *> &external_table_files,
      ObIArray<int64_t> &reorder_part_id);

  int get_external_file_list_on_device(const ObString &location,
                                       const uint64_t modify_ts,
                                       const ObString &pattern,
                                       const ObExprRegexpSessionVariables &regexp_vars,
                                       const ObString &access_info,
                                       ObIAllocator &allocator,
                                       ObExternalTableFiles &file_list);

  int get_partitions_info_with_cache(const ObTableSchema &table_schema,
                                     ObSqlSchemaGuard &sql_schema_guard,
                                     common::ObIAllocator& allocator,
                                     int64_t refresh_interval_ms,
                                     ObArray<PartitionInfo*> &partition_infos);

  int get_partitions_info(const ObTableSchema &table_schema,
                          ObSqlSchemaGuard &sql_schema_guard,
                          common::ObIAllocator &allocator,
                          ObExternalTablePartitions &pts);

  int flush_external_file_cache(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t part_id,
    const ObIArray<ObAddr> &all_servers);

  int refresh_external_table(const uint64_t tenant_id,
                            const uint64_t table_id,
                            ObSchemaGetterGuard &schema_guard,
                            ObExecContext &exec_ctx,
                            bool &has_partition_changed);

  int refresh_external_table(const uint64_t tenant_id,
                              const ObTableSchema *table_schema,
                              ObSchemaGetterGuard &schema_guard,
                              ObExecContext &exec_ctx,
                              bool &has_partition_changed);

  int auto_refresh_external_table(ObExecContext &exec_ctx, const int64_t interval);
  static int calculate_odps_part_val_by_part_spec(const ObTableSchema *table_schema,
                                          const ObIArray<ObExternalFileInfoTmp> &file_infos,
                                          ObIArray<ObNewRow> &part_vals,
                                          ObIAllocator &allocator);
  static int get_external_file_list_on_device(const common::ObString &location,
                                              const common::ObString &pattern,
                                              const sql::ObExprRegexpSessionVariables &regexp_vars,
                                              common::ObIArray<common::ObString> &file_urls,
                                              common::ObIArray<int64_t> &file_sizes,
                                              common::ObIArray<int64_t> &last_modify_times,
                                              common::ObIArray<common::ObString> &content_digests,
                                              const common::ObString &access_info,
                                              common::ObIAllocator &allocator);
  static int collect_files_content_digest(const common::ObString &location,
                                          const common::ObString &access_info,
                                          const common::ObIArray<common::ObString> &file_urls,
                                          common::ObIAllocator &allocator,
                                          common::ObIArray<ObString> &content_digests);

private:
  int collect_odps_table_statistics(const bool collect_statistic,
                                    const uint64_t tenant_id,
                                    const uint64_t table_id,
                                    ObIArray<uint64_t> &updated_part_ids,
                                    ObMySQLTransaction &trans);
  int delete_auto_refresh_job(ObExecContext &exec_ctx, ObMySQLTransaction &trans);
  int create_auto_refresh_job(ObExecContext &ctx, const int64_t interval, ObMySQLTransaction &trans);
  int update_inner_table_files_list_by_part(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t partition_id,
      const common::ObIArray<ObExternalFileInfoTmp> &file_infos,
      common::ObIArray<uint64_t> &updated_part_ids);

  int update_inner_table_files_list_by_table(
    sql::ObExecContext &exec_ctx,
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const common::ObIArray<ObExternalFileInfoTmp> &file_infos,
    common::ObIArray<uint64_t> &updated_part_ids,
    bool &has_partition_changed);

  bool is_cache_value_timeout(const ObExternalTableFiles &ext_files) {
    return ObTimeUtil::current_time() - ext_files.create_ts_ > CACHE_EXPIRE_TIME;
  }
  bool is_cache_value_timeout(int64_t create_ts_ms, int64_t refresh_interval_ms) {
    return ObTimeUtil::current_time_ms() - create_ts_ms > refresh_interval_ms;
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

  int collect_file_list_by_expr_parallel(uint64_t tenant_id,
                                         const ObIArray<ObString> &location,
                                         const ObString &pattern,
                                         const ObString &access_info,
                                         ObIAllocator &allocator,
                                         ObIArray<ObExternalTableFiles *> &files,
                                         ObFixedArray<ObString, ObIAllocator> *files_location);

  int calculate_dop(int64_t task_count, uint64_t tenant_id, int64_t &dop);

  int convert_to_external_table_files(const char *buf,
                                      const int64_t buf_len,
                                      const ObString location,
                                      ObIAllocator &allocator,
                                      ObExternalTableFiles &file_list);

  int get_one_location_from_cache(const common::ObString &location,
                                  const uint64_t tenant_id,
                                  const int64_t &modify_ts,
                                  ObIAllocator &allocator,
                                  ObIArray<ObExternalTableFiles *> &external_table_files,
                                  int64_t refresh_interval_ms);

  int insert_one_location_to_cache(int64_t tenant_id,
                                   ObString location,
                                   ObExternalTableFiles &file_list);

private:
  common::ObSpinLock fill_cache_locks_[LOAD_CACHE_LOCK_CNT];
  common::ObKVCache<ObExternalTableFilesKey, ObExternalTableFiles> kv_cache_;
  common::ObKVCache<ObExternalTableFileListKey, ObExternalTableFiles> external_file_list_cache_;
  common::ObKVCache<ObExternalTablePartitionsKey, ObExternalTablePartitions> external_partitions_cache_;
};


}
}

#endif /* _OB_EXTERNAL_TABLE_FILE_MANAGER_H_ */
