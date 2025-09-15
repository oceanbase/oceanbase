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

#ifndef _OB_EXTERNAL_TABLE_UTILS_H_
#define _OB_EXTERNAL_TABLE_UTILS_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/optimizer/file_prune/ob_hive_file_pruner.h"
#include "src/share/external_table/ob_external_table_file_mgr.h"
#include "src/sql/engine/table/ob_external_file_access.h"
#include "sql/optimizer/file_prune/ob_lake_table_file_map.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObNewRange;
class ObAddr;
}

namespace sql
{
class ObDASTabletLoc;
class ObExecContext;
class ObExternalTableAccessService;
class ObExprRegexContext;
class ObExprRegexpSessionVariables;
}

namespace share
{

struct ObExternalPathFilter {
  ObExternalPathFilter(sql::ObExprRegexContext &regex_ctx, common::ObIAllocator &allocator)
    : regex_ctx_(regex_ctx), allocator_(allocator) {}
  int init(const common::ObString &pattern, const sql::ObExprRegexpSessionVariables &regexp_vars);
  bool is_inited();
  int is_filtered(const common::ObString &path, bool &is_filtered);
  sql::ObExprRegexContext &regex_ctx_;
  common::ObIAllocator &allocator_;
  common::ObArenaAllocator temp_allocator_;
};

class ObCachedExternalFileInfoKey final : public common::ObIKVCacheKey
{
public:
  ObCachedExternalFileInfoKey() = default;
  ~ObCachedExternalFileInfoKey() = default;
  bool operator==(const common::ObIKVCacheKey &other) const;
  uint64_t hash() const override;
  int64_t size() const override;
  uint64_t get_tenant_id() const override;
  int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K_(tenant_id), K_(file_path));

  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  ObString file_path_;
};

class ObCachedExternalFileInfoValue final : public common::ObIKVCacheValue
{
public:
  ObCachedExternalFileInfoValue() = default;
  ~ObCachedExternalFileInfoValue() = default;
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K_(file_size));
  int64_t file_size_ = OB_INVALID_SIZE;
};

class ObCachedExternalFileInfoCollector
{
public:
  int init();
  static ObCachedExternalFileInfoCollector &get_instance();
  int collect_file_size(const common::ObString &url, const common::ObObjectStorageInfo *storage_info, int64_t &file_size);
private:
  const int64_t bucket_num_ = 10;
  common::ObBucketLock bucket_lock_;
  common::ObKVCache<ObCachedExternalFileInfoKey, ObCachedExternalFileInfoValue> kv_cache_;
};

class ObExternalFileInfoCollector
{
public:
  const static int64_t MAX_CONTENT_DIGEST_LEN = 128;
  ObExternalFileInfoCollector(common::ObIAllocator &allocator) :
    allocator_(allocator), storage_info_(nullptr)
  {}
  int init(const common::ObString &location, const ObString &access_info);
  int get_file_list(const common::ObString &path, const common::ObString &pattern,
                    const ObExprRegexpSessionVariables &regexp_vars,
                    common::ObIArray<common::ObString> &file_urls,
                    common::ObIArray<int64_t> &file_sizes, common::ObIArray<int64_t> &modify_times,
                    common::ObIArray<ObString> &content_digests);
  int collect_files_content_digest(const common::ObIArray<common::ObString> &file_urls,
                                   common::ObIArray<ObString> &content_digests);
  int collect_file_content_digest(const common::ObString &url, ObString &content_digest);
  int collect_files_modify_time(const common::ObIArray<common::ObString> &file_urls,
                                common::ObIArray<int64_t> &modify_times);
  int collect_file_modify_time(const common::ObString &url, int64_t &modify_time);
  int collect_file_size(const common::ObString &url, int64_t &file_size, bool enable_cache = false);

private:
  int convert_to_full_file_urls(const common::ObString &location,
                                const common::ObIArray<common::ObString> &file_urls,
                                common::ObIArray<common::ObString> &full_file_urls);

private:
  const char DUMMY_EMPTY_CHAR = '\0';
  common::ObIAllocator &allocator_;
  common::ObStorageType storage_type_;
  share::ObBackupStorageInfo backup_storage_info_;
  share::ObHDFSStorageInfo hdfs_storage_info_;
  common::ObObjectStorageInfo *storage_info_;
};

class ObExternalFileListArrayOpWithFilter : public ObBaseDirEntryOperator
{
public:
  ObExternalFileListArrayOpWithFilter(ObIArray<common::ObString> &name_array,
                                      ObIArray<int64_t> &file_size, ObIArray<int64_t> &modify_times,
                                      ObIArray<common::ObString> &content_digests,
                                      ObExternalPathFilter *filter, ObIAllocator &array_allocator) :
    name_array_(name_array),
    file_size_(file_size), modify_times_(modify_times), content_digests_(content_digests),
    filter_(filter), allocator_(array_allocator)
  {}

  virtual bool need_get_file_meta() const override
  {
    return true;
  }
  bool is_valid_content_digests() const
  {
    return (content_digests_.count() != 0 && content_digests_.count() == file_size_.count());
  }
  bool is_valid_modify_times() const
  {
    return (modify_times_.count() != 0 && modify_times_.count() == file_size_.count());
  }
  int func(const dirent *entry);

private:
  ObIArray <ObString>& name_array_;
  ObIArray <int64_t>& file_size_;
  ObIArray <int64_t>& modify_times_;
  ObIArray <ObString>& content_digests_;
  ObExternalPathFilter *filter_;
  ObIAllocator& allocator_;
};

class ObLocalFileListArrayOpWithFilter : public ObBaseDirEntryOperator
{
public:
  ObLocalFileListArrayOpWithFilter(ObIArray <common::ObString> &name_array,
                                   ObIArray <int64_t>& file_size,
                                   const ObString &path,
                                   const ObString &origin_path,
                                   ObExternalPathFilter *filter,
                                   ObIAllocator &array_allocator)
    : name_array_(name_array), file_size_(file_size), path_(path), origin_path_(origin_path),
      filter_(filter), allocator_(array_allocator) {}
  virtual bool need_get_file_meta() const override { return true; }
  int func(const dirent *entry);
private:
  ObIArray <ObString> &name_array_;
  ObIArray <int64_t> &file_size_;
  const ObString &path_;
  const ObString &origin_path_;
  ObExternalPathFilter *filter_;
  ObIAllocator &allocator_;
};

class ObExternalTableUtils {
 public:
  enum ExternalTableRangeColumn {
    PARTITION_ID = 0,
    FILE_URL,
    FILE_ID,
    ROW_GROUP_NUMBER,
    LINE_NUMBER,
    SESSION_ID,
    SPLIT_IDX,
    FILE_SIZE,
    MODIFY_TIME,
    CONTENT_DIGEST,
    DELETE_FILE_URLS,
    DELETE_FILE_SIZES,
    DELETE_FILE_MODIFY_TIMES,
    DATA_FILE_FORMAT,
    DELETE_FILE_FORMATS,
    MAX_EXTERNAL_FILE_SCANKEY
  };

  struct ExternalTableFileUrlCompare
  {
    bool operator()(ObExternalTableBasicFileInfo& l, ObExternalTableBasicFileInfo& r)
    {
      return l.url_ < r.url_;
    }
  };

 public:
  static const char *dummy_file_name();

  // range_filter is from query_range
  static int is_file_id_in_ranges(const common::ObIArray<common::ObNewRange *> &range_filter,
                                  const int64_t &file_id,
                                  bool &in_ranges);
  static int resolve_file_id_range(const common::ObNewRange &range,
                                   const int64_t &column_idx,
                                   int64_t &start_file,
                                   int64_t &end_file);
  // file_id is same in start and end
  static int resolve_line_number_range(const common::ObNewRange &range,
                                       const int64_t &column_idx,
                                       int64_t &start_lineno,
                                       int64_t &end_lineno);
  static int resolve_odps_start_step(const common::ObNewRange &range,
                                       const int64_t &column_idx,
                                       int64_t &start,
                                       int64_t &step);
  static int convert_external_table_new_range(const common::ObString &file_url,
                                              const common::ObString &content_digest,
                                              const int64_t file_size,
                                              const int64_t modify_time,
                                              const int64_t file_id,
                                              const uint64_t ref_table_id,
                                              const common::ObNewRange &range,
                                              common::ObIAllocator &allocator,
                                              common::ObNewRange &new_range,
                                              bool &is_valid);
  static int convert_lake_table_new_range(const sql::ObILakeTableFile *file,
                                          const int64_t file_id,
                                          const uint64_t part_id,
                                          ObIAllocator &allocator,
                                          ObNewRange &new_range);
  static int convert_external_table_empty_range(const common::ObString &file_url,
                                                const common::ObString &content_digest,
                                                const int64_t file_size,
                                                const int64_t modify_time,
                                                const int64_t file_id,
                                                const uint64_t ref_table_id,
                                                common::ObIAllocator &allocator,
                                                common::ObNewRange &new_range);

  static int prepare_single_scan_range(const uint64_t tenant_id,
                                       const ObDASScanCtDef &das_ctdef,
                                       ObDASScanRtDef *das_rtdef,
                                       ObExecContext &exec_ctx,
                                       ObIArray<int64_t> &partition_ids,
                                       common::ObIArray<common::ObNewRange *> &ranges,
                                       common::ObIAllocator &range_allocator,
                                       common::ObIArray<common::ObNewRange *> &new_range,
                                       bool is_file_on_disk,
                                       ObExecContext &ctx);
  static int prepare_lake_table_single_scan_range(ObExecContext &exec_ctx,
                                                  ObDASTableLoc *tab_loc,
                                                  ObDASTabletLoc *tablet_loc,
                                                  ObIAllocator &range_allocator,
                                                  ObIArray<ObNewRange *> &new_ranges);
  static int calc_assigned_files_to_sqcs(
    const common::ObIArray<ObExternalFileInfo> &files,
    common::ObIArray<int64_t> &assigned_idx,
    int64_t sqc_count);
  static int assigned_files_to_sqcs_by_load_balancer(
    const common::ObIArray<ObExternalFileInfo> &files,
    const ObIArray<ObPxSqcMeta> &sqcs,
    common::ObIArray<int64_t> &assigned_idx);
  static int select_external_table_loc_by_load_balancer(
    const common::ObIArray<ObExternalFileInfo> &files,
    const ObIArray<ObAddr> &all_locations,
    ObIArray<ObAddr> &target_locations);

  static int assign_odps_file_to_sqcs(
    ObDfo &dfo,
    ObExecContext &exec_ctx,
    ObIArray<ObPxSqcMeta> &sqcs,
    int64_t parallel,
    ObODPSGeneralFormat::ApiMode odps_api_mode);
  static int split_odps_to_sqcs_process_tunnel(common::ObIArray<share::ObExternalFileInfo> &files,
                                        ObIArray<ObPxSqcMeta> &sqcs,
                                        int parallel);
  static int split_odps_to_sqcs_storage_api(int64_t split_task_count, int64_t table_total_file_size,
      const ObString &session_str, const ObString &new_file_urls, ObIArray<ObPxSqcMeta> &sqcs, int parallel,
      ObIAllocator &range_allocator, ObODPSGeneralFormat::ApiMode odps_api_mode);
  static int filter_files_in_locations(common::ObIArray<share::ObExternalFileInfo> &files,
      common::ObIArray<common::ObAddr> &locations);

  static int plugin_split_tasks(
      ObIAllocator &allocator,
      const ObString &external_table_format_str,
      ObDfo &dfo,
      ObIArray<ObPxSqcMeta> &sqcs,
      int64_t parallel);

  static int collect_external_file_list(
    const ObSQLSessionInfo* session_ptr_in,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
    const ObString &properties,
    const bool &is_partitioned_table,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIAllocator &allocator,
    common::ObSqlString &full_path,
    ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);

  static int collect_local_files_on_servers(
    const uint64_t tenant_id,
    const ObString &location,
    const ObString &pattern,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIArray<ObAddr> &all_servers,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes,
    ObIArray<int64_t> &modify_times,
    ObIArray<ObString> &content_digests,
    common::ObSqlString &partition_path,
    ObIAllocator &allocator);

  static int build_basic_file_infos(const ObIArray<ObString> &file_urls,
                                    const ObIArray<int64_t> &file_sizes,
                                    const ObIArray<int64_t> &modify_times,
                                    const ObIArray<ObString> &content_digests,
                                    ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);
  static int get_file_list(const common::ObString &path, const common::ObString &pattern,
                           const ObString &access_info,
                           const ObExprRegexpSessionVariables &regexp_vars,
                           common::ObIArray<common::ObString> &file_urls,
                           common::ObIArray<int64_t> &file_sizes,
                           common::ObIArray<ObString> &content_digests,
                           common::ObIAllocator &allocator);

  static int make_external_table_scan_range(const common::ObString &file_url,
                                            const common::ObString &content_digest,
                                            const int64_t file_size,
                                            const int64_t modify_time,
                                            const int64_t file_id,
                                            const uint64_t ref_table_id,
                                            const int64_t first_lineno,
                                            const int64_t last_lineno,
                                            const common::ObString &session_id,
                                            const int64_t first_split_idx,
                                            const int64_t last_split_idx,
                                            common::ObIAllocator &allocator,
                                            common::ObNewRange &new_range);
  static int make_external_table_scan_range(const common::ObString &file_url,
                                            const common::ObString &content_digest,
                                            const int64_t file_size,
                                            const int64_t modify_time,
                                            const int64_t file_id,
                                            const uint64_t ref_table_id,
                                            const int64_t first_lineno,
                                            const int64_t last_lineno,
                                            const common::ObString &session_id,
                                            const int64_t first_split_idx,
                                            const int64_t last_split_idx,
                                            const common::ObIArray<ObLakeDeleteFile> &delete_files,
                                            const ObExternalFileFormat::FormatType &file_format,
                                            common::ObIAllocator &allocator,
                                            common::ObNewRange &new_range);

  static int process_delete_files(const common::ObIArray<ObLakeDeleteFile> &delete_files,
                                  common::ObIAllocator &allocator,
                                  common::ObObj *obj_start,
                                  common::ObObj *obj_end);
  static int process_file_format(const ObExternalFileFormat::FormatType &file_format,
                                 const common::ObIArray<ObLakeDeleteFile> &delete_files,
                                 common::ObIAllocator &allocator,
                                 common::ObObj *obj_start,
                                 common::ObObj *obj_end);
  static bool is_skipped_insert_column(const schema::ObColumnSchemaV2& column);
  static int get_external_file_location(const ObTableSchema &table_schema,
                                        ObSchemaGetterGuard &schema_guard,
                                        ObIAllocator &allocator,
                                        ObString &file_location);
  static int get_external_file_location_access_info(const ObTableSchema &table_schema,
                                                    ObSchemaGetterGuard &schema_guard,
                                                    ObString &access_info);
  static int remove_external_file_list(const uint64_t tenant_id,
                                       const ObString &location,
                                       const ObString &access_info,
                                       const ObString &pattern,
                                       const sql::ObExprRegexpSessionVariables &regexp_vars,
                                       ObIAllocator &allocator);
  static int collect_file_basic_info(const common::ObString &location, const ObString &access_info,
                                     const common::ObString &file_url,
                                     common::ObIAllocator &allocator, int64_t &file_size,
                                     int64_t &modify_time, ObString &content_digest);

  static int collect_external_file_list_with_cache(
    const uint64_t tenant_id,
    const uint64_t ts,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIAllocator &allocator,
    int64_t refresh_interval_ms,
    ObIArray<ObHiveFileDesc> &hive_file_desc,
    int64_t part_id);

  static int collect_partitions_info_with_cache(const ObTableSchema &table_schema,
                                                ObSqlSchemaGuard &sql_schema_guard,
                                                ObIAllocator &allocator,
                                                int64_t refresh_interval_ms,
                                                ObArray<PartitionInfo*> &partition_infos);

  static int create_external_file_url_info(const common::ObString &file_location,
                                           const common::ObString &access_info,
                                           const common::ObString &full_file_name,
                                           common::ObIAllocator &allocator,
                                           ObExternalFileUrlInfo *&file_info);
  static int get_credential_field_name(ObSqlString &str, int64_t opt);

  static int fetch_odps_partition_info_for_task_assign(ObIAllocator &allocator,
                                                       const ObTableScanSpec *scan_op,
                                                       ObExecContext &exec_ctx,
                                                       uint64_t tenant_id,
                                                       ObIArray<ObExternalFileInfo> &external_table_files,
                                                       int64_t parallel,
                                                       bool &one_partition_per_thread);
  static ObString get_part_spec(const ObString &file_info)
  {
      return file_info;
  }
  static ObString get_part_spec(const ObExternalFileInfo &file_info)
  {
      return file_info.file_url_;
  }
  static ObString get_part_spec(const ObExternalTableFileManager::ObExternalFileInfoTmp &file_info)
  {
      return file_info.file_url_;
  }

  template<typename T, typename U>
  static int fetch_row_count_wrapper(const ObString &part_spec, const ObString &properties, int get_row_count_or_size, int64_t &row_count)
  {
    int ret = OB_SUCCESS;
    T odps_driver;
    if (OB_FAIL(U::init_odps_driver(get_row_count_or_size, THIS_WORKER.get_session(), properties, odps_driver))) {
      LOG_WARN("failed to init odps driver", K(ret));
    } else if (OB_FAIL(U::fetch_row_count(part_spec, get_row_count_or_size, odps_driver, row_count))) {
      LOG_WARN("failed to fetch row count", K(ret));
    }
    return ret;
  }

  template<typename PartRecordType>
  static int fetch_odps_partition_info(const ObString& porperty_str,
    const ObIArray<PartRecordType>& partition_strs, const bool get_row_count_or_size,
    const uint64_t tenant_id, const uint64_t table_ref_id, const int64_t parallel,
    ObHashMap<ObOdpsPartitionKey, int64_t>& partition_str_to_file_size,
    ObIAllocator& allocator_for_map_key/* life time longer than partition_str_to_file_size*/) {
    int ret = OB_SUCCESS;
    ObSqlString query_sql;
    int64_t dop_of_collect_external_table_statistics = 1;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid()) && static_cast<int64_t>(tenant_config->_dop_of_collect_external_table_statistics) > 0) {
      dop_of_collect_external_table_statistics = tenant_config->_dop_of_collect_external_table_statistics;
    } else {
      int64_t default_dop = max((partition_strs.count() / 4 > 0) ? (partition_strs.count() / 4) : 1, parallel);
      double min_cpu;
      double max_cpu;
      if (OB_ISNULL(GCTX.omt_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(GCTX.omt_->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
        LOG_WARN("fail to get tenant cpu", K(ret));
      } else {
        dop_of_collect_external_table_statistics = min(default_dop, (int)max_cpu);
      }
    }

    if (OB_SUCC(ret) && !partition_str_to_file_size.created()) {
      if (OB_UNLIKELY(partition_strs.count() == 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition_strs is empty", K(ret));
      } else {
        OZ(partition_str_to_file_size.create(partition_strs.count(), ObMemAttr(MTL_ID(), "ODPS_PART_SIZE")));
      }
    }
    if (OB_SUCC(ret)) {
      // 构建正确的VALUES查询, 存在limit的时候不查询
      int64_t remain_timeout = THIS_WORKER.get_timeout_remain();
      OZ(query_sql.assign_fmt("SELECT/*+ query_timeout(%ld) parallel(%ld) no_rewrite PQ_SUBQUERY(HASH ALL) */ partition_str, (select "
                              "CALC_ODPS_SIZE(partition_str, %d, property_str) file_size from dual) as file_size FROM ",
                              remain_timeout, dop_of_collect_external_table_statistics, get_row_count_or_size/* 获取partition size */));
      OZ(query_sql.append_fmt("(SELECT/*+ no_rewrite*/ * FROM "));
      OZ(query_sql.append_fmt(
          "(VALUES ROW('%.*s')) property_name(property_str), ", static_cast<int>(porperty_str.length()), porperty_str.ptr()));
      OZ(query_sql.append("(VALUES "));
      int64_t file_size = 0;
      int64_t file_to_collect = 0;

      for (int64_t i = 0; OB_SUCC(ret) && i < partition_strs.count(); ++i) {
        const ObString &odps_partition = get_part_spec(partition_strs.at(i));
        if (0 == odps_partition.compare(ObExternalTableUtils::dummy_file_name())) {
          // do nothing
        } else if (OB_SUCC(partition_str_to_file_size.get_refactored(ObOdpsPartitionKey(table_ref_id, odps_partition), file_size))) {
          // do nothing
        } else if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          // ODPSQz IF NON Partition ROW()
          OZ(query_sql.append_fmt(
              "ROW(\"%.*s\")", static_cast<int>(odps_partition.length()), odps_partition.ptr()));
          if (i < partition_strs.count() - 1) {
            OZ(query_sql.append(","));
          }
          file_to_collect += 1;
        }
      }
      OZ(query_sql.append(")  partition_name(partition_str)),"));
      OZ(query_sql.append("(select count(*) from oceanbase.__all_dummy);"));

      if (file_to_collect > 0) {
        ObMySQLTransaction trans;

        CK(OB_NOT_NULL(GCTX.sql_proxy_));
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to assign query sql", KR(ret));
        } else {
          OZ(trans.start(GCTX.sql_proxy_, tenant_id));

          LOG_INFO("odps query_sql", K(query_sql));
          SMART_VAR(ObISQLClient::ReadResult, result)
          {
            if (OB_FAIL(trans.read(result, tenant_id, query_sql.ptr()))) {
              LOG_WARN("failed to read result", KR(ret));
            } else {
              ObMySQLResult *res = NULL;
              int res_ret = OB_SUCCESS;
              if (OB_ISNULL(res = result.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(query_sql));
              } else {
                while (OB_SUCCESS == (res_ret = res->next())) {
                  char partition_str[1024];
                  int64_t tmp_real_str_len = 0;
                  int64_t file_size = 0;

                  EXTRACT_STRBUF_FIELD_MYSQL(
                      *res, "partition_str", partition_str, static_cast<int64_t>(sizeof(partition_str)), tmp_real_str_len);
                  EXTRACT_INT_FIELD_MYSQL(*res, "file_size", file_size, int64_t);
                  ObString tmp_partition_str(tmp_real_str_len, partition_str);
                  ObString partition_str_ob;
                  OZ(ob_write_string(allocator_for_map_key, tmp_partition_str, partition_str_ob));
                  OZ(partition_str_to_file_size.set_refactored(ObOdpsPartitionKey(table_ref_id, partition_str_ob), file_size));
                  LOG_TRACE("ODPS get partition info ", K(get_row_count_or_size == 0? "row count" : "size"), K(partition_str_ob), K(file_size));
                }
                if (res_ret != OB_ITER_END) {
                  ret = res_ret;
                  LOG_WARN("failed to get next row", KR(ret), K(tenant_id), K(query_sql));
                }
              }
            }
          }
        }
        OZ(trans.end(true));
        if (trans.is_started()) {
          trans.end(false);
        }
      }
    }
    return ret;
  }
private:
  static int classification_file_basic_info(
    const ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos,
    ObIArray<common::ObString> &file_urls, ObIArray<int64_t> *file_sizes = nullptr,
    ObIArray<common::ObString> *content_digests = nullptr,
    ObIArray<int64_t> *modify_times = nullptr);
  static int prepare_single_scan_range_(const uint64_t tenant_id,
                                        const ObDASScanCtDef &das_ctdef,
                                        ObDASScanRtDef *das_rtdef,
                                        ObExecContext &exec_ctx,
                                        ObIArray<int64_t> &partition_ids,
                                        common::ObIArray<common::ObNewRange *> &ranges,
                                        common::ObIAllocator &range_allocator,
                                        common::ObIArray<common::ObNewRange *> &new_range,
                                        bool is_file_on_disk,
                                        ObExecContext &ctx);
private:
  static bool is_left_edge(const common::ObObj &value);
  static bool is_right_edge(const common::ObObj &value);
  static int64_t get_edge_value(const common::ObObj &edge);
  static int sort_external_files(ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);

};
}
}
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
