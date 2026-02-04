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
#include "sql/engine/connector/ob_odps_jni_connector.h"

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
namespace schema
{
class ObSchemaGetterGuard;
}


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
  int collect_dirs_with_spec_level(
      ObIAllocator &allocator,
      const common::ObString &path,
      int64_t spec_level,
      common::ObIArray<common::ObString> &dir_urls,
      common::ObIArray<int64_t> &modify_times);
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
  struct ExternalTableFileUrlCompare
  {
    bool operator()(ObExternalTableBasicFileInfo& l, ObExternalTableBasicFileInfo& r)
    {
      return l.url_ < r.url_;
    }
  };

 public:
  static const char *dummy_file_name();
  static int get_tenant_compat_version(schema::ObSchemaGetterGuard &schema_guard,
                                       const uint64_t tenant_id,
                                       uint64_t &compat_version);

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
  static int resolve_odps_start_step(const ObOdpsScanTask *scan_task,
                                       int64_t &start,
                                       int64_t &step);
  static int convert_external_table_scan_task(const common::ObString &file_url,
                                              const common::ObString &content_digest,
                                              const int64_t file_size,
                                              const int64_t modify_time,
                                              const int64_t file_id,
                                              const uint64_t ref_table_id,
                                              const common::ObNewRange &range,
                                              common::ObIAllocator &allocator,
                                              ObFileScanTask *scan_task,
                                              bool &is_valid);
  static int convert_lake_table_scan_task(const int64_t file_id,
                                          const uint64_t part_id,
                                          ObFileScanTask *scan_task);
  static int convert_external_table_empty_task(const common::ObString &file_url,
                                                const common::ObString &content_digest,
                                                const int64_t file_size,
                                                const int64_t modify_time,
                                                const int64_t file_id,
                                                const uint64_t ref_table_id,
                                                common::ObIAllocator &allocator,
                                                ObFileScanTask *scan_task);

  static int prepare_single_scan_task(const uint64_t tenant_id,
                                       const ObDASScanCtDef &das_ctdef,
                                       ObDASScanRtDef *das_rtdef,
                                       ObExecContext &exec_ctx,
                                       ObIArray<int64_t> &partition_ids,
                                       common::ObIArray<common::ObNewRange *> &ranges,
                                       common::ObIAllocator &allocator,
                                       common::ObIArray<ObIExtTblScanTask *> &scan_tasks,
                                       bool is_file_on_disk,
                                       ObExecContext &ctx);
  static int prepare_lake_table_single_scan_task(ObExecContext &exec_ctx,
                                                ObDASTableLoc *tab_loc,
                                                ObDASTabletLoc *tablet_loc,
                                                ObIAllocator &allocator,
                                                ObIArray<ObNewRange *> &ranges,
                                                ObIArray<ObIExtTblScanTask *> &scan_tasks);
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

 static int make_file_scan_task(const common::ObString &file_url,
                                const common::ObString &content_digest,
                                const int64_t file_size,
                                const int64_t modify_time,
                                const int64_t file_id,
                                const uint64_t ref_table_id,
                                const int64_t first_lineno,
                                const int64_t last_lineno,
                                ObFileScanTask *scan_task);

  static int make_odps_scan_task(const common::ObString &file_url,
                                const uint64_t part_id,
                                const int64_t first_lineno,
                                const int64_t last_lineno,
                                const common::ObString &session_id,
                                const int64_t first_split_idx,
                                const int64_t last_split_idx,
                                ObOdpsScanTask &scan_task);

  static int make_parallel_parse_csv_task(const ObExternalFileInfo &file_info,
                                          const int64_t first_lineno,
                                          const int64_t last_lineno,
                                          const int64_t start_pos,
                                          const int64_t end_pos,
                                          const int64_t chunk_idx,
                                          const int64_t chunk_cnt,
                                          ObExtTableScanTask *scan_task);
  static int make_parallel_parse_csv_task(const ObExtTableScanTask &original_task,
                                          const int64_t first_lineno,
                                          const int64_t last_lineno,
                                          const int64_t start_pos,
                                          const int64_t end_pos,
                                          const int64_t chunk_idx,
                                          const int64_t chunk_cnt,
                                          ObExtTableScanTask *scan_task);

  static bool is_skipped_insert_column(const schema::ObColumnSchemaV2& column);
  static int concat_external_file_location(const ObString &location,
                                           const ObString &sub_path,
                                           ObSqlString &full_path);
  static int resolve_location_for_load_and_select_into(ObSchemaGetterGuard &schema_guard,
                                                       const ObSQLSessionInfo &session_info,
                                                       common::ObIAllocator &allocator,
                                                       const common::ObString &location_name,
                                                       const common::ObString &sub_path,
                                                       common::ObString &full_path,
                                                       common::ObString *access_info = NULL,
                                                       bool check_oss_prefix = false);
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

  static int collect_external_file_list_with_cache(ObSQLSessionInfo &session,
                                                   const uint64_t tenant_id,
                                                   const ObIArray<ObString> &part_path,
                                                   const ObIArray<int64_t> &part_id,
                                                   const ObString &access_info,
                                                   const ObString &pattern,
                                                   ObIAllocator &allocator,
                                                   int64_t refresh_interval_ms,
                                                   ObIArray<ObHiveFileDesc> &hive_file_desc);

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


private:
  static int classification_file_basic_info(
    const ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos,
    ObIArray<common::ObString> &file_urls, ObIArray<int64_t> *file_sizes = nullptr,
    ObIArray<common::ObString> *content_digests = nullptr,
    ObIArray<int64_t> *modify_times = nullptr);
  static int prepare_single_scan_task_(const uint64_t tenant_id,
                                        const ObDASScanCtDef &das_ctdef,
                                        ObDASScanRtDef *das_rtdef,
                                        ObExecContext &exec_ctx,
                                        ObIArray<int64_t> &partition_ids,
                                        common::ObIArray<common::ObNewRange *> &ranges,
                                        common::ObIAllocator &allocator,
                                        common::ObIArray<ObIExtTblScanTask *> &scan_tasks,
                                        bool is_file_on_disk,
                                        ObExecContext &ctx);
private:
  static bool is_left_edge(const common::ObObj &value);
  static bool is_right_edge(const common::ObObj &value);
  static int64_t get_edge_value(const common::ObObj &edge);
  static int sort_external_files(ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);

  // odps
private:
  static int fetch_odps_all_partitions_info_for_task_assign(
      ObIAllocator &allocator, const ObTableScanSpec *scan_op,
      ObExecContext &exec_ctx, uint64_t tenant_id,
      ObIArray<ObExternalFileInfo> &external_table_files, int64_t parallel,
      bool &one_partition_per_thread);
  static int add_odps_file_to_addr(ObIArray<ObPxSqcMeta> &sqcs,
                                   int64_t addr_idx,
                                   const ObExternalFileInfo &small_file,
                                   ObIAllocator &allocator) {
    int ret = OB_SUCCESS;
    ObPxSqcMeta &sqc = sqcs.at(addr_idx);
    OZ(sqc.get_access_external_table_files().push_back(small_file));

    return ret;
  }
// odps
public:
  static int assign_odps_file_to_sqcs(
    ObDfo &dfo,
    ObExecContext &exec_ctx,
    ObIArray<ObPxSqcMeta> &sqcs,
    int64_t parallel,
    ObODPSGeneralFormat::ApiMode odps_api_mode);

  template <typename PartRecordType>
  static int fetch_odps_all_partitions_size(
      ObSQLSessionInfo &session,
      const ObString &porperty_str,
      const ObIArray<PartRecordType> &partition_strs,
      const uint64_t tenant_id,
      const uint64_t table_ref_id,
      common::ObIAllocator &allocator_for_map_key,
      common::hash::ObHashMap<ObOdpsPartitionKey, int64_t>
          &partition_str_to_file_size /* life time longer than partition_str_to_file_size*/) {
    int ret = OB_SUCCESS;
    int64_t dop_of_collect_external_table_statistics = 1;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid()) &&
        static_cast<int64_t>(
            tenant_config->_dop_of_collect_external_table_statistics) > 0) {
      dop_of_collect_external_table_statistics =
          tenant_config->_dop_of_collect_external_table_statistics;
    } else {
      int64_t default_dop =
          (partition_strs.count() / 4 > 0) ? (partition_strs.count() / 4) : 1;
      double min_cpu;
      double max_cpu;
      if (OB_ISNULL(GCTX.omt_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(
                     GCTX.omt_->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
        LOG_WARN("fail to get tenant cpu", K(ret));
      } else {
        dop_of_collect_external_table_statistics =
            min(default_dop, (int)max_cpu);
      }
    }
    if (OB_SUCC(ret) && !partition_str_to_file_size.created()) {
      if (OB_UNLIKELY(partition_strs.count() == 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition_strs is empty", K(ret));
      } else {
        OZ(partition_str_to_file_size.create(
            partition_strs.count(), ObMemAttr(MTL_ID(), "ODPS_PART_SIZE")));
      }
    }
    ObSEArray<PartRecordType, 20> partition_strs_filter;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_strs.count(); ++i) {
      int64_t file_size = 0;
      const ObString &odps_partition = get_part_spec(partition_strs.at(i));
      if (0 == odps_partition.compare(ObExternalTableUtils::dummy_file_name())) {
        // do nothing
      } else if (OB_SUCC(partition_str_to_file_size.get_refactored(
                   ObOdpsPartitionKey(table_ref_id, odps_partition),
                   file_size))) {
        // do nothing
      } else if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        OZ(partition_strs_filter.push_back(partition_strs.at(i)));
      }
    }
    ObSEArray<ObString, 20> partition_names_ret;
    ObSEArray<int64_t, 20> partition_file_sizes_ret;
    ObSEArray<ObString, 20> session_ids_ret;
    if (OB_SUCC(ret) && partition_strs_filter.count() > 0) {
      OZ(fetch_odps_all_partitions_info(
          session,
          porperty_str, partition_strs_filter,
          sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_SIZE,
          dop_of_collect_external_table_statistics, tenant_id,
          allocator_for_map_key, partition_names_ret, partition_file_sizes_ret,
          session_ids_ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_names_ret.count(); ++i) {
      const ObString &partition_name = partition_names_ret.at(i);
      const int64_t file_size = partition_file_sizes_ret.at(i);
      OZ(partition_str_to_file_size.set_refactored(ObOdpsPartitionKey(table_ref_id, partition_name), file_size));
    }
    return ret;
  }

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

  struct FileInfoWithIdx {
    const ObExternalFileInfo *file_info_;
    int64_t file_idx_;
    int64_t sqc_idx_;
    int64_t remain_file_size_;
    bool should_split_; // 是否需要分块
    int64_t start_permyriad_;
    int64_t end_permyriad_;
    int64_t process_size_;
    FileInfoWithIdx() : file_info_(nullptr), file_idx_(-1), sqc_idx_(-1), remain_file_size_(0), should_split_(false), start_permyriad_(0), end_permyriad_(100), process_size_(0) {}
    FileInfoWithIdx(const ObExternalFileInfo *file_info, int64_t file_idx, int64_t sqc_idx, int64_t remain_file_size, bool should_split, int64_t start_permyriad, int64_t end_permyriad)
      : file_info_(file_info), file_idx_(file_idx), sqc_idx_(sqc_idx), remain_file_size_(remain_file_size), should_split_(should_split), start_permyriad_(start_permyriad), end_permyriad_(end_permyriad), process_size_(0) {}
    TO_STRING_KV(K(file_idx_), K(sqc_idx_), K(remain_file_size_), K(should_split_), K(process_size_));
  };

    // SQC文件集合结构，用于跟踪每个SQC的负载情况
  struct SqcFileSet {
    int64_t total_file_size_;
    int64_t total_file_count_;
    int64_t total_processing_time_ms_; // 总处理时间（毫秒）
    int64_t sqc_idx_;
    SqcFileSet() : total_file_size_(0), total_file_count_(0), total_processing_time_ms_(0), sqc_idx_(-1) {}
    // 计算添加文件后的总处理时间
    int64_t calc_total_time_with_file(int64_t file_size, int64_t file_count = 1) const {
      int64_t processing_time = file_size / (10 * 1024 * 1024); // 处理时间（毫秒），10MB/s
      int64_t open_cost = file_count * 1200; // 文件打开成本，1200ms
      return total_processing_time_ms_ + processing_time + open_cost;
    }
    // 添加文件到当前SQC
    int add_file(int64_t file_size, int64_t file_count = 1) {
      int ret = OB_SUCCESS;
      total_file_size_ += file_size;
      total_file_count_ += file_count;
      total_processing_time_ms_ = calc_total_time_with_file(total_file_size_, total_file_count_); // 重新计算总时间
      if (total_processing_time_ms_ < 0) {
        ret = OB_ERROR_OUT_OF_RANGE;
      }
      return ret;
    }
    TO_STRING_KV(K(total_file_size_), K(total_file_count_), K(total_processing_time_ms_), K(sqc_idx_));
  };

  template <typename SplitContainer>
  static int split_qc_for_odps_to_sqcs_by_line_tunnel_partition(common::ObIAllocator &allocator, ObExecContext &exec_ctx,
      const ObString &properties, common::ObIArray<share::ObExternalFileInfo> &files, ObIArray<SplitContainer> &sqcs,
      int parallel, bool one_partition_per_thread)
  {
    int ret = OB_SUCCESS;
    int64_t sqc_count = sqcs.count();
    if (one_partition_per_thread || parallel == 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
        share::ObExternalFileInfo small_file = files.at(i);
        small_file.file_size_ = INT64_MAX;
        small_file.row_start_ = 0;
        small_file.row_count_ = INT64_MAX;
        OZ(add_odps_file_to_addr(sqcs, i % sqc_count, small_file, allocator));
      }
    } else {
      int64_t sqc_idx = 0;
      common::ObSEArray<FileInfoWithIdx, 20> assigned_idx;
      if (OB_FAIL(ret)) {
        LOG_WARN("Failed to push back file", K(ret));
      } else if (OB_FAIL(calc_assigned_odps_files_to_sqcs_optimized(files, assigned_idx, sqc_count))) {
        LOG_WARN("Failed to use optimized file assignment, fallback to original strategy", K(ret));
        ret = OB_SUCCESS;
        // 回退到原始策略
        for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
          share::ObExternalFileInfo small_file = files.at(i);
          small_file.file_size_ = INT64_MAX;
          small_file.row_start_ = 0;
          small_file.row_count_ = INT64_MAX;
          OZ(add_odps_file_to_addr(sqcs, i % sqc_count, small_file, allocator));
        }
      } else {
        // 使用优化策略的结果
        struct SortByFileIdx {
          bool operator()(const FileInfoWithIdx &l, const FileInfoWithIdx &r) const
          {
            return l.file_idx_ < r.file_idx_ || (l.file_idx_ == r.file_idx_ && l.end_permyriad_ < r.end_permyriad_);  // 同一个分区编号放一起
          }
        };
        lib::ob_sort(assigned_idx.begin(), assigned_idx.end(), SortByFileIdx());
         int64_t cur_row_count = 0;
        int64_t start_row_idx = 0;
        int64_t end_row_idx = -1;

        ObSEArray<ObString, 20> partition_strs;
        for (int64_t i = 0; OB_SUCC(ret) && i < assigned_idx.count(); ++i) {
          const FileInfoWithIdx &file_info_with_idx = assigned_idx.at(i);
          if (file_info_with_idx.should_split_) {
            if (i == 0 || file_info_with_idx.file_idx_ != assigned_idx.at(i - 1).file_idx_) {
              cur_row_count = 0;
              end_row_idx = -1;
              OZ(partition_strs.push_back(file_info_with_idx.file_info_->file_url_));
            }
          }
        }
        ObSEArray<ObString, 20> partition_name_ret;
        ObSEArray<int64_t, 20> partition_file_sizes_ret;
        ObSEArray<ObString, 20> session_ids_ret;
        ObSQLSessionInfo *session = exec_ctx.get_my_session();
        if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is null", K(ret));
        }
        if (OB_SUCC(ret)) {
          OZ(fetch_odps_all_partitions_info(*session, properties, partition_strs, sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_ROW_COUNT
            , sqc_count, MTL_ID(), allocator, partition_name_ret, partition_file_sizes_ret, session_ids_ret));
        }
        int indx_of_partition = 0;
        ObString session_id;
        ObExternalFileInfo info;

        for (int64_t i = 0; OB_SUCC(ret) && i < assigned_idx.count(); ++i) {
          if (assigned_idx.at(i).should_split_) {
            if (i == 0 || assigned_idx.at(i).file_idx_ != assigned_idx.at(i - 1).file_idx_) {
              cur_row_count = 0;
              end_row_idx = -1;
              session_id = session_ids_ret.at(indx_of_partition);
              cur_row_count = partition_file_sizes_ret.at(indx_of_partition);
              indx_of_partition++;
              info = *assigned_idx.at(i).file_info_;
            }
            if (OB_SUCC(ret)) {
              int64_t start_permyriad_ = assigned_idx.at(i).start_permyriad_;
              int64_t end_permyriad_ = assigned_idx.at(i).end_permyriad_;
              start_row_idx = end_row_idx + 1;
              end_row_idx = cur_row_count * (end_permyriad_ / 10000.00) - 1;
              if (end_row_idx < start_row_idx) {
                LOG_WARN("end_row_idx < start_row_idx", K(end_row_idx), K(start_row_idx));
              } else {
                info.session_id_ = session_id;
                info.row_start_ = start_row_idx;
                info.row_count_ = end_row_idx - start_row_idx + 1;
                info.file_size_ = assigned_idx.at(i).file_info_->file_size_ * (end_permyriad_ - start_permyriad_) / 10000.00;
                OZ(add_odps_file_to_addr(sqcs, assigned_idx.at(i).sqc_idx_, info, allocator));
              }
            }
          } else {
            info = *assigned_idx.at(i).file_info_;
            info.file_size_ = INT64_MAX;
            info.row_start_ = 0;
            info.row_count_ = INT64_MAX;
            OZ(add_odps_file_to_addr(sqcs, assigned_idx.at(i).sqc_idx_, *assigned_idx.at(i).file_info_, allocator));
          }
          LOG_INFO("px task info: odps file to machine ", K(i),
              K(info.file_url_),
              K(info.session_id_),
              K(info.row_start_),
              K(info.row_count_),
              K(info.file_size_),
              K(assigned_idx.at(i).start_permyriad_),
              K(assigned_idx.at(i).end_permyriad_),
              K(assigned_idx.at(i).should_split_),
              K(assigned_idx.at(i).process_size_),
              K(assigned_idx.at(i).sqc_idx_));
        }
      }
    }
    return ret;
  }

  template <typename PartRecordType>
  static int fetch_odps_all_partitions_info(
      ObSQLSessionInfo &session,
      const ObString &porperty_str,
      const ObIArray<PartRecordType> &partition_strs,
      const sql::ObOdpsJniConnector::OdpsFetchType fetch_type,
      const int64_t dop_of_collect_external_table_statistics,
      const uint64_t tenant_id, common::ObIAllocator &allocator,
      ObSEArray<ObString, 20> &partition_names,
      ObSEArray<int64_t, 20> &partition_file_sizes,
      ObSEArray<ObString, 20> &session_ids) {
    int ret = OB_SUCCESS;
    ObSqlString query_sql;

    if (OB_SUCC(ret)) {
      // 构建正确的VALUES查询, 存在limit的时候不查询 timeout_us
      // THIS_WORKER.get_timeout_remain()
      // int64_t remain_timeout =
      //     min(THIS_WORKER.get_timeout_remain(), 10 * 1000 * 1000); // 10s
      int64_t remain_timeout = THIS_WORKER.get_timeout_remain();
      OZ(query_sql.assign_fmt(
          "SELECT/*+ query_timeout(%ld) parallel(%ld) no_rewrite "
          "PQ_SUBQUERY(HASH ALL) */ partition_str, (select "
          "CALC_ODPS_SIZE(partition_str, %d, property_str) file_size from "
          "dual) as file_size FROM ",
          remain_timeout, dop_of_collect_external_table_statistics,
          (fetch_type ==
                   sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_ROW_COUNT
               ? 0
               : 1) /* 获取partition size */));
      OZ(query_sql.append_fmt("(SELECT/*+ no_rewrite*/ * FROM "));
      OZ(query_sql.append_fmt(
          "(VALUES ROW('%.*s')) property_name(property_str), ",
          static_cast<int>(porperty_str.length()), porperty_str.ptr()));
      OZ(query_sql.append("(VALUES "));
      int64_t file_size = 0;
      int64_t file_to_collect = 0;

      for (int64_t i = 0; OB_SUCC(ret) && i < partition_strs.count(); ++i) {
        const ObString &odps_partition = get_part_spec(partition_strs.at(i));
        if (0 ==
            odps_partition.compare(ObExternalTableUtils::dummy_file_name())) {
          // do nothing
        } else {
          // ODPSQz IF NON Partition ROW()
          OZ(query_sql.append_fmt("ROW(\"%.*s\")",
                                  static_cast<int>(odps_partition.length()),
                                  odps_partition.ptr()));
          if (i < partition_strs.count() - 1) {
            OZ(query_sql.append(","));
          }
          file_to_collect += 1;
        }
      }
      OZ(query_sql.append(")  partition_name(partition_str)),"));
      OZ(query_sql.append(
          "(select count(*) from internal.oceanbase.__all_dummy);"));

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
                LOG_WARN("get mysql result failed", KR(ret), K(tenant_id),
                         K(query_sql));
              } else {
                if (fetch_type ==
                    sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_SIZE) {
                  while (OB_SUCCESS == (res_ret = res->next())) {
                    int64_t file_size = 0;
                    ObString partition_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "partition_str",
                                                partition_str);
                    ObString file_size_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "file_size",
                                                file_size_str);
                    OZ(ob_write_string(allocator, partition_str,
                                       partition_str));
                    // ObString to int64_t
                    if (OB_SUCC(ret)) {
                      bool valid = false;
                      file_size = ObFastAtoi<int64_t>::atoi(
                          file_size_str.ptr(),
                          file_size_str.ptr() + file_size_str.length(), valid);
                      if (!valid) {
                        ret = OB_ERR_DATA_TRUNCATED;
                      }
                    }
                    OZ(partition_names.push_back(partition_str));
                    OZ(partition_file_sizes.push_back(file_size));
                    LOG_TRACE("ODPS get partition info ",
                              K(fetch_type ==
                                        sql::ObOdpsJniConnector::OdpsFetchType::
                                            GET_ODPS_TABLE_ROW_COUNT
                                    ? "row count"
                                    : "size"),
                              K(partition_str), K(file_size));
                  }
                  if (res_ret != OB_ITER_END) {
                    ret = res_ret;
                    LOG_WARN("failed to get next row", KR(ret), K(tenant_id),
                             K(query_sql));
                  }
                } else { // GET_ODPS_TABLE_ROW_COUNT
                  while (OB_SUCCESS == (res_ret = res->next())) {
                    int64_t row_count = 0;
                    ObString partition_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "partition_str",
                                                partition_str);
                    ObString row_count_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "file_size",
                                                row_count_str);
                    ObString row_count_str_part = row_count_str.split_on('|');
                    ObString session_id = row_count_str;
                    if (OB_SUCC(ret)) {
                      bool valid = false;
                      row_count = ObFastAtoi<int64_t>::atoi(
                          row_count_str_part.ptr(),
                          row_count_str_part.ptr() +
                              row_count_str_part.length(),
                          valid);
                      if (!valid) {
                        ret = OB_ERR_DATA_TRUNCATED;
                      }
                    }
                    ObString partition_str_cp;
                    ObString session_id_cp;
                    OZ(ob_write_string(allocator, partition_str,
                                       partition_str_cp));
                    OZ(partition_names.push_back(partition_str_cp));
                    OZ(partition_file_sizes.push_back(row_count));
                    OZ(ob_write_string(allocator, session_id, session_id_cp));
                    OZ(session_ids.push_back(session_id_cp));
                    LOG_WARN("ODPS get partition info ", K(partition_str),
                             K(row_count), K(session_id));
                    LOG_TRACE("ODPS get partition info ",
                              K(fetch_type ==
                                        sql::ObOdpsJniConnector::OdpsFetchType::
                                            GET_ODPS_TABLE_ROW_COUNT
                                    ? "row count"
                                    : "size"),
                              K(partition_str), K(row_count), K(session_id));
                  }
                  if (res_ret != OB_ITER_END) {
                    ret = res_ret;
                    LOG_WARN("failed to get next row", KR(ret), K(tenant_id),
                             K(query_sql));
                  }
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

  template <typename HandleFunc>
  static int read_data_for_bound(const ObString &external_location,
                                 const ObString &external_access_info,
                                 const ObExternalFileFormat &external_file_format,
                                 const ObString &url,
                                 const int64_t start_pos,
                                 const int64_t end_pos,
                                 HandleFunc &handle_func) {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator_for_read;
    char *buf = nullptr;
    int64_t buf_len = 2 * 1024 * 1024;
    if (OB_ISNULL(buf = (char *)allocator_for_read.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for buffer", K(ret), K(buf_len));
    }
    MEMSET(buf, 0, buf_len);
    ObExternalStreamFileReader file_reader;
    OZ (file_reader.init(external_location, external_access_info,
                         external_file_format.csv_format_.compression_algorithm_,
                         allocator_for_read));
    ObSqlString full_path;
    if (!is_abs_url(url)) {
      OZ (full_path.append_fmt("%.*s%s%.*s", external_location.length(), external_location.ptr(),
                                (external_location.empty() || external_location[external_location.length() - 1] == '/') ? "" : "/",
                                url.length(), url.ptr()));
    } else {
      OZ (full_path.assign(url));
    }
    OZ (file_reader.open(full_path.string()));
    file_reader.advance(start_pos);
    int64_t already_read_size = 0;
    int64_t target_read_size = end_pos - start_pos;
    while (OB_SUCC(ret)
           && !handle_func.is_finished_
           && already_read_size < target_read_size
           && !file_reader.eof()) {
      int64_t read_size = 0;
      OZ (file_reader.read(buf, buf_len, read_size));
      if (read_size > 0) {
        int check_size = min(read_size, target_read_size - already_read_size);
        OZ (handle_func(external_file_format, buf, check_size));
      }
      already_read_size += read_size;
    }
    allocator_for_read.free(buf);
    file_reader.close();
    return ret;
  }

  static int adjust_end_pos_skip_escape(sql::ObExternalStreamFileReader &file_reader,
                                        const char escape,
                                        const int64_t start_pos,
                                        int64_t &end_pos);

  static bool is_abs_url(ObString url)
  {
    ObString dst("://");
    if (0
        == ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                            url.ptr(),
                            url.length(),
                            dst.ptr(),
                            dst.length())) {
      return false;
    } else {
      return true;
    }
  }
  static bool is_satisfied_for_parallel_parse_csv(const common::ObIArray<share::ObExternalFileInfo> &external_table_filesconst,
                                                   const ObCSVGeneralFormat &csv_format);

  static int calc_assigned_odps_files_to_sqcs_optimized(
    const common::ObIArray<ObExternalFileInfo> &files,
    common::ObSEArray<FileInfoWithIdx, 20> &assigned_idx,
    int64_t sqc_count);


  static int split_qc_for_odps_to_sqcs_storage_api_byte(int64_t split_task_count,
      const ObString &session_str, const ObString &new_file_urls, ObIArray<ObPxSqcMeta> &sqcs,
      ObIAllocator &range_allocator);
  static int split_qc_for_odps_to_sqcs_storage_api_row(int64_t table_total_row_count,
      const ObString &session_str, const ObString &new_file_urls, ObIArray<ObPxSqcMeta> &sqcs, int parallel,
      ObIAllocator &range_allocator);


};
}
}
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
