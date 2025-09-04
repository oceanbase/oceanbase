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
  int collect_file_size(const common::ObString &url, int64_t &file_size);

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
private:
  static int classification_file_basic_info(
    const ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos,
    ObIArray<common::ObString> &file_urls, ObIArray<int64_t> *file_sizes = nullptr,
    ObIArray<common::ObString> *content_digests = nullptr,
    ObIArray<int64_t> *modify_times = nullptr);
private:
  static bool is_left_edge(const common::ObObj &value);
  static bool is_right_edge(const common::ObObj &value);
  static int64_t get_edge_value(const common::ObObj &edge);
  static int sort_external_files(ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos);

};
}
}
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
