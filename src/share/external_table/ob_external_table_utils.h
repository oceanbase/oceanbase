/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXTERNAL_TABLE_UTILS_H_
#define _OB_EXTERNAL_TABLE_UTILS_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "objit/common/ob_item_type.h"
#include "lib/utility/ob_sort.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/optimizer/file_prune/ob_hive_file_pruner.h"
#include "src/share/external_table/ob_external_table_file_mgr.h"
#include "src/sql/engine/table/ob_external_file_access.h"
#include "sql/optimizer/file_prune/ob_lake_table_file_map.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/engine/connector/ob_odps_jni_connector.h"
#include "sql/engine/table/ob_csv_prefetch_mgr.h"
#include "sql/engine/px/ob_granule_parallel_task_gen.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObNewRange;
class ObAddr;
struct ObCatalogExtPartitionInfo;
}

namespace sql
{
class ObDASTabletLoc;
class ObExecContext;
class ObExternalTableAccessService;
class ObExprRegexContext;
class ObExprRegexpSessionVariables;
struct ObIcebergFileDesc;
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
  TO_STRING_KV(K_(file_size), K_(modify_time));
  int64_t file_size_ = OB_INVALID_SIZE;
  int64_t modify_time_ = 0;
};

class ObCachedExternalFileInfoCollector
{
public:
  int init();
  static ObCachedExternalFileInfoCollector &get_instance();
  int collect_file_size(const common::ObString &url, const common::ObObjectStorageInfo *storage_info, int64_t &file_size);
  int collect_file_modify_time(const common::ObString &url, const common::ObObjectStorageInfo *storage_info, int64_t &modify_time);
private:
  // Shared miss path: loads size + mtime with (two) network calls and caches both,
  // so a lookup for either field warms the other.
  int get_or_load_(const common::ObString &url, const common::ObObjectStorageInfo *storage_info,
                   ObCachedExternalFileInfoValue &value);
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
  common::ObStorageType get_storage_type() { return storage_type_; }
  int get_file_list(const common::ObString &path, const common::ObString &pattern,
                    const share::schema::ObExternalFilePatternType pattern_type,
                    const ObExprRegexpSessionVariables &regexp_vars,
                    common::ObIArray<common::ObString> &file_urls,
                    common::ObIArray<int64_t> &file_sizes, common::ObIArray<int64_t> &modify_times,
                    common::ObIArray<ObString> &content_digests);
  int collect_files_content_digest(const common::ObIArray<common::ObString> &file_urls,
                                   common::ObIArray<ObString> &content_digests);
  int collect_file_content_digest(const common::ObString &url, ObString &content_digest);
  int collect_files_modify_time(const common::ObIArray<common::ObString> &file_urls,
                                common::ObIArray<int64_t> &modify_times);
  int collect_file_modify_time(const common::ObString &url, int64_t &modify_time, bool enable_cache = false);
  // Pure existence check (no file size). Unlike collect_file_size, this works
  // for directories too: OB's get_file_length/get_file_stat reject paths whose
  // length is -1 (directories), but is_exist only checks existence, so a
  // table-root directory correctly reports exist=true here.
  int collect_file_exist(const common::ObString &url, bool &exist);
  int collect_file_size(const common::ObString &url, int64_t &file_size, bool enable_cache = false);
  int collect_dirs_with_spec_level(
      ObIAllocator &allocator,
      const common::ObString &path,
      int64_t spec_level,
      common::ObIArray<common::ObString> &dir_urls,
      common::ObIArray<int64_t> &modify_times);
  int fetch_simple_stats(const common::ObString &path,
                         const common::ObIArray<ObString> &partition_values,
                         ObIArray<int64_t> &file_nums,
                         ObIArray<int64_t> &data_sizes,
                         ObIArray<int64_t> &modify_times);
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
                                   ObIArray<int64_t> &modify_times,
                                   ObIArray<ObString> &content_digests,
                                   const ObString &path,
                                   const ObString &origin_path,
                                   ObExternalPathFilter *filter,
                                   ObIAllocator &array_allocator)
      : name_array_(name_array), file_size_(file_size), modify_times_(modify_times),
        content_digests_(content_digests), path_(path), origin_path_(origin_path), filter_(filter),
        allocator_(array_allocator)
  {
  }
  virtual bool need_get_file_meta() const override { return true; }
  int func(const dirent *entry);
  int finish_current_dir();
private:
  ObIArray <ObString> &name_array_;
  ObIArray <int64_t> &file_size_;
  ObIArray <int64_t> &modify_times_;
  ObIArray <ObString> &content_digests_;
  const ObString &path_;
  const ObString &origin_path_;
  ObExternalPathFilter *filter_;
  ObIAllocator &allocator_;
};

class ObCommonDirStatsCollectorOp : public ObBaseDirEntryOperator
{
public:
  ObCommonDirStatsCollectorOp(ObIArray<int64_t> &file_nums,
                              ObIArray<int64_t> &data_sizes,
                              ObIArray<int64_t> &modify_times)
      : file_nums_(file_nums), data_sizes_(data_sizes), modify_times_(modify_times),
        cur_file_count_(0), cur_total_size_(0), cur_max_modify_time_(0)
  {
  }
  virtual bool need_get_file_meta() const override
  {
    return true;
  }
  int func(const dirent *entry);
  int finish_current_dir();
private:
  ObIArray<int64_t> &file_nums_;
  ObIArray<int64_t> &data_sizes_;
  ObIArray<int64_t> &modify_times_;
  int64_t cur_file_count_;
  int64_t cur_total_size_;
  int64_t cur_max_modify_time_;
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
  // =====================================================================
  // 通用小工具
  // =====================================================================
  static const char *dummy_file_name();
  static int adjust_string_length_for_external_table(const char *data,
                                                     const int64_t src_length,
                                                     const int64_t max_length,
                                                     const bool is_byte_length,
                                                     const bool enable_hive_compat,
                                                     int64_t &out_length);
  static int get_tenant_compat_version(schema::ObSchemaGetterGuard &schema_guard,
                                       const uint64_t tenant_id,
                                       uint64_t &compat_version);
  // Parallel low word:     [parallel(bit0), file_id(1-20), chunk_id(21-34), chunk_line_number(35-63)]
  // Non-parallel low word: [parallel(bit0), file_id(1-20), line_number(21-63)]
  // High word:             [row_start_offset(64-125), reserved(126-127)]
  static constexpr int64_t ROW_METADATA_FLAG_BITS = 1;
  static constexpr int64_t ROW_METADATA_FILE_ID_BITS = 20;
  static constexpr int64_t ROW_METADATA_CHUNK_ID_BITS = 14;
  static constexpr int64_t MAX_PARALLEL_PARSE_CSV_CHUNK_CNT =
      (1LL << ROW_METADATA_CHUNK_ID_BITS); // 16384
  static constexpr int64_t PARALLEL_PARSE_CSV_CHUNK_SIZE = 32 * 1024 * 1024; // 32MB
  static constexpr int64_t ROW_METADATA_FILE_ID_SHIFT = ROW_METADATA_FLAG_BITS;
  static constexpr int64_t ROW_METADATA_CHUNK_ID_SHIFT =
      ROW_METADATA_FILE_ID_SHIFT + ROW_METADATA_FILE_ID_BITS;
  static constexpr int64_t ROW_METADATA_PARALLEL_LINE_NUMBER_SHIFT =
      ROW_METADATA_CHUNK_ID_SHIFT + ROW_METADATA_CHUNK_ID_BITS;
  static constexpr int64_t ROW_METADATA_NON_PARALLEL_LINE_NUMBER_SHIFT =
      ROW_METADATA_CHUNK_ID_SHIFT;
  static constexpr int64_t ROW_METADATA_PARALLEL_LINE_NUMBER_BITS =
      64 - ROW_METADATA_PARALLEL_LINE_NUMBER_SHIFT;
  static constexpr int64_t ROW_METADATA_NON_PARALLEL_LINE_NUMBER_BITS =
      64 - ROW_METADATA_NON_PARALLEL_LINE_NUMBER_SHIFT;
  static constexpr uint64_t ROW_METADATA_FILE_ID_MASK =
      (1ULL << ROW_METADATA_FILE_ID_BITS) - 1;
  static constexpr uint64_t ROW_METADATA_CHUNK_ID_MASK =
      (1ULL << ROW_METADATA_CHUNK_ID_BITS) - 1;
  static constexpr uint64_t ROW_METADATA_PARALLEL_LINE_NUMBER_MASK =
      (1ULL << ROW_METADATA_PARALLEL_LINE_NUMBER_BITS) - 1;
  static constexpr uint64_t ROW_METADATA_NON_PARALLEL_LINE_NUMBER_MASK =
      (1ULL << ROW_METADATA_NON_PARALLEL_LINE_NUMBER_BITS) - 1;
  static constexpr uint64_t ROW_METADATA_PARALLEL_FLAG = 1ULL;
  static constexpr int64_t ROW_METADATA_OFFSET_BITS = 62;
  static constexpr int ROW_METADATA_OFFSET_SHIFT = 64;
  static constexpr uint64_t ROW_METADATA_OFFSET_MASK =
      (1ULL << ROW_METADATA_OFFSET_BITS) - 1;

  static int encode_csv_row_metadata(const bool is_parallel,
                                     const int64_t file_id,
                                     const int64_t chunk_id,
                                     const int64_t line_number,
                                     const int64_t row_start_offset,
                                     common::int128_t &encoded);
  static int decode_csv_row_metadata(const common::int128_t &encoded,
                                     bool &is_parallel,
                                     int64_t &file_id,
                                     int64_t &chunk_id,
                                     int64_t &line_number,
                                     int64_t &row_start_offset);
  static bool is_skipped_insert_column(const schema::ObColumnSchemaV2& column);
  // External table hidden columns (__file_id, __line_number) have no catalog column stats.
  static bool is_hidden_external_column(const common::ObString &column_name);

  static bool is_sub_path_contain_parent_dir(const common::ObString &sub_path);
  static bool is_external_table_psudo_expr(const ObItemType expr_type)
  {
    return T_PSEUDO_EXTERNAL_FILE_COL == expr_type
        || T_PSEUDO_PARTITION_LIST_COL == expr_type
        || T_PSEUDO_EXTERNAL_FILE_URL == expr_type
        || T_PSEUDO_METADATA_ROW_METADATA == expr_type;
  }
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
  static int get_credential_field_name(ObSqlString &str, int64_t opt);
  static int print_obj_json_escaped(common::ObIAllocator &allocator,
                                    const common::ObObj &obj,
                                    common::ObString &value);

  // =====================================================================
  // location / URL 解析
  // =====================================================================
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
  static int resolve_location_url_with_access_info(ObSchemaGetterGuard &schema_guard,
                                                   const ObSQLSessionInfo &session_info,
                                                   common::ObIAllocator &allocator,
                                                   const ParseNode *location_name_node,
                                                   const ParseNode *sub_path_node,
                                                   common::ObString &url,
                                                   bool check_oss_prefix = false);
  static int get_external_file_location(const ObTableSchema &table_schema,
                                        ObSchemaGetterGuard &schema_guard,
                                        ObIAllocator &allocator,
                                        ObString &file_location,
                                        bool *is_shared_external_files_on_disk = nullptr);
  static int get_external_file_location_access_info(const ObTableSchema &table_schema,
                                                    ObSchemaGetterGuard &schema_guard,
                                                    ObString &access_info);
  static int create_external_file_url_info(const common::ObString &file_location,
                                           const common::ObString &access_info,
                                           const common::ObString &full_file_name,
                                           common::ObIAllocator &allocator,
                                           ObExternalFileUrlInfo *&file_info);
  static int remove_external_file_list(const uint64_t tenant_id,
                                       const ObString &location,
                                       const ObString &access_info,
                                       const ObString &pattern,
                                       const share::schema::ObExternalFilePatternType pattern_type,
                                       const sql::ObExprRegexpSessionVariables &regexp_vars,
                                       ObIAllocator &allocator);

  // =====================================================================
  // 文件列表收集与采样
  // =====================================================================
  static int collect_external_file_list(
    const ObSQLSessionInfo* session_ptr_in,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
    const share::schema::ObExternalFilePatternType pattern_type,
    const ObString &properties,
    const bool &is_partitioned_table,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIAllocator &allocator,
    common::ObSqlString &full_path,
    ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);

  static int get_file_list(const common::ObString &path, const common::ObString &pattern,
                           const share::schema::ObExternalFilePatternType pattern_type,
                           const ObString &access_info,
                           const ObExprRegexpSessionVariables &regexp_vars,
                           common::ObIArray<common::ObString> &file_urls,
                           common::ObIArray<int64_t> &file_sizes,
                           common::ObIArray<ObString> &content_digests,
                           common::ObIAllocator &allocator);

  static int collect_local_files_on_servers(
    const uint64_t tenant_id,
    const ObString &location,
    const ObString &pattern,
    const share::schema::ObExternalFilePatternType pattern_type,
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
                                                   const share::schema::ObExternalFilePatternType pattern_type,
                                                   ObIAllocator &allocator,
                                                   int64_t refresh_interval_ms,
                                                   ObIArray<ObHiveFileDesc> &hive_file_desc,
                                                   ObIArray<int64_t> &part_file_count);

  static int filter_files_in_locations(common::ObIArray<share::ObExternalFileInfo> &files,
      common::ObIArray<common::ObAddr> &locations);
  // PRECONDITION: filter_files_in_locations() must have been called first so candidates whose
  // file_addr_ belongs to a departed observer are already removed. Otherwise the murmur hash
  // may pick a departed observer and the file would be silently dropped downstream.
  static int dedup_shared_local_files(common::ObIArray<share::ObExternalFileInfo> &files,
                                      common::ObIAllocator &allocator);

  static int select_external_file_for_sample(
      const ObString &location,
      const ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos,
      ObString &sampled_file_name);
  static int generate_file_sample_indices(const int64_t total_files,
                                          const double sample_percent,
                                          bool &is_file_sample,
                                          int64_t &target_count,
                                          common::ObIArray<int64_t> &indices);


  // =====================================================================
  // scan task 构造与执行准备
  // =====================================================================
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
  // Allocates the dummy (empty) scan task for a lake table scan whose range is
  // always-false or whose tablet has no entry in the lake file map. The
  // concrete type comes from the ObFileScanTask factory keyed by the lake
  // table format (the ODPS iterators downcast to ObOdpsScanTask, so the type
  // must be right for ODPS); every format short-circuits the dummy by
  // file_url_ == dummy_file_name() before any format-specific read.
  static int alloc_empty_lake_table_scan_task(common::ObIAllocator &allocator,
                                              const share::ObLakeTableFormat lake_table_format,
                                              const int64_t part_id,
                                              ObIExtTblScanTask *&scan_task);
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
                                       bool is_shared_external_files_on_disk,
                                       ObExecContext &ctx);
  static int prepare_lake_table_single_scan_task(ObExecContext &exec_ctx,
                                                const ObDASScanCtDef *scan_ctdef,
                                                ObDASTableLoc *tab_loc,
                                                ObDASTabletLoc *tablet_loc,
                                                ObIAllocator &allocator,
                                                ObIArray<ObNewRange *> &ranges,
                                                ObIArray<ObIExtTblScanTask *> &scan_tasks);
  static int make_file_scan_task(const common::ObString &file_url,
                                 const common::ObString &content_digest,
                                 const int64_t file_size,
                                 const int64_t modify_time,
                                 const int64_t file_id,
                                 const uint64_t ref_table_id,
                                 const int64_t first_lineno,
                                 const int64_t last_lineno,
                                 ObFileScanTask *scan_task);

  // =====================================================================
  // PX 分配与选点
  // =====================================================================
  static int64_t calc_parallel_task_chunk_size(const int64_t total_task_cnt,
                                               int64_t worker_cnt,
                                               const int64_t task_cnt_per_worker = 4);
  // definition is after the class (template implementation section)
  template <typename FileInfo>
  static int calc_assigned_files_to_sqcs(
      const common::ObIArray<FileInfo> &files,
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

  static int plugin_split_tasks(
      ObIAllocator &allocator,
      const ObString &external_table_format_str,
      ObDfo &dfo,
      ObIArray<ObPxSqcMeta> &sqcs,
      int64_t parallel);

  // =====================================================================
  // 统计信息收集
  // =====================================================================

  static int fetch_external_table_simple_stats(const ObString &location,
                                               const ObString &access_info,
                                               const ObIArray<ObString> &partition_values,
                                               ObIArray<int64_t> &file_nums,
                                               ObIArray<int64_t> &data_sizes,
                                               ObIArray<int64_t> &modify_times);

  // Fill per-partition simple stats (file_num/data_size/modify_ts/schema_version) into
  // partition_infos by listing files under each partition path. This performs one remote
  // list per partition, so it must only be invoked on the statistics-gathering path, never
  // on the query/partition-pruning path.
  static int fill_catalog_partition_stats(const ObTableSchema &table_schema,
                                          ObSchemaGetterGuard &schema_guard,
                                          const int64_t schema_version,
                                          ObIArray<common::ObCatalogExtPartitionInfo *> &partition_infos);

  static int get_part_col_names(const ObTableSchema &table_schema,
                                ObIArray<ObString> &part_col_names);
  static int collect_partitions_info_with_cache(const ObTableSchema &table_schema,
                                                ObSqlSchemaGuard &sql_schema_guard,
                                                ObIAllocator &allocator,
                                                int64_t refresh_interval_ms,
                                                ObIArray<sql::HivePartitionInfo*> &partition_infos);

private:
  // Rewrite `sfile://<path>` to `file://<path>` in-place via allocator.
  // Returns is_shared=true when the input used the shared prefix; false otherwise (location untouched).
  static int normalize_shared_file_location(common::ObIAllocator &allocator,
                                            common::ObString &location,
                                            bool &is_shared);
  static int check_is_absolute_local_file_path(const common::ObString &location,
                                               bool &is_absolute_path);
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
                                        bool is_shared_external_files_on_disk,
                                        ObExecContext &ctx);
  static bool is_left_edge(const common::ObObj &value);
  static bool is_right_edge(const common::ObObj &value);
  static int64_t get_edge_value(const common::ObObj &edge);
  static int sort_external_files(ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);

};

// =====================================================================
// 模板实现（体量大，移出类外保持声明区紧凑）
// =====================================================================
template <typename FileInfo>
int ObExternalTableUtils::calc_assigned_files_to_sqcs(
    const common::ObIArray<FileInfo> &files,
    common::ObIArray<int64_t> &assigned_idx,
    int64_t sqc_count)
{
  int ret = OB_SUCCESS;

  struct SqcFileSet {
    int64_t total_file_size_;
    int64_t sqc_idx_;
    TO_STRING_KV(K(total_file_size_), K(sqc_idx_));
  };

  struct SqcFileSetCmp {
    bool operator()(const SqcFileSet &l, const SqcFileSet &r)
    {
      return l.total_file_size_ > r.total_file_size_;
    }
    int get_error_code() { return OB_SUCCESS; }
  };

  struct FileInfoWithIdx {
    const FileInfo *file_info_;
    int64_t file_idx_;
    TO_STRING_KV(K(file_idx_));
  };

  if (sqc_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sqc count", K(ret), K(sqc_count));
  }
  SqcFileSetCmp temp_cmp;
  common::ObBinaryHeap<SqcFileSet, SqcFileSetCmp> heap(temp_cmp);
  common::ObArray<FileInfoWithIdx> sorted_files;
  OZ(sorted_files.reserve(files.count()));
  OZ(assigned_idx.prepare_allocate(files.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    FileInfoWithIdx file_info;
    file_info.file_info_ = &(files.at(i));
    file_info.file_idx_ = i;
    OZ(sorted_files.push_back(file_info));
  }
  lib::ob_sort(sorted_files.begin(), sorted_files.end(),
               [](const FileInfoWithIdx &l, const FileInfoWithIdx &r) -> bool {
                 return l.file_info_->file_size_ > r.file_info_->file_size_;
               });

  const int64_t initial_file_count = std::min(sqc_count, sorted_files.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < initial_file_count; ++i) {
    SqcFileSet new_set;
    new_set.total_file_size_ = sorted_files.at(i).file_info_->file_size_;
    new_set.sqc_idx_ = i;
    OZ(heap.push(new_set));
    assigned_idx.at(sorted_files.at(i).file_idx_) = i;
  }

  for (int64_t i = initial_file_count; OB_SUCC(ret) && i < sorted_files.count(); ++i) {
    SqcFileSet cur_min_set = heap.top();
    cur_min_set.total_file_size_ += sorted_files.at(i).file_info_->file_size_;
    assigned_idx.at(sorted_files.at(i).file_idx_) = cur_min_set.sqc_idx_;
    OZ(heap.pop());
    OZ(heap.push(cur_min_set));
  }
  return ret;
}
} // namespace share
} // namespace oceanbase
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
