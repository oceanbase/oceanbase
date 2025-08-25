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

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "src/share/schema/ob_column_schema.h"
#include "sql/engine/px/ob_dfo.h"
#include "src/share/external_table/ob_external_table_file_mgr.h"

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
    MAX_EXTERNAL_FILE_SCANKEY
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
                                              const int64_t file_id,
                                              const uint64_t ref_table_id,
                                              const common::ObNewRange &range,
                                              common::ObIAllocator &allocator,
                                              common::ObNewRange &new_range,
                                              bool &is_valid);
  static int convert_external_table_empty_range(const common::ObString &file_url,
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
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes);

  static int collect_local_files_on_servers(
    const uint64_t tenant_id,
    const ObString &location,
    const ObString &pattern,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIArray<ObAddr> &all_servers,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes,
    common::ObSqlString &partition_path,
    ObIAllocator &allocator);
  static int make_external_table_scan_range(const common::ObString &file_url,
                                            const int64_t file_id,
                                            const uint64_t ref_table_id,
                                            const int64_t first_lineno,
                                            const int64_t last_lineno,
                                            const common::ObString &session_id,
                                            const int64_t first_split_idx,
                                            const int64_t last_split_idx,
                                            common::ObIAllocator &allocator,
                                            common::ObNewRange &new_range);
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
  static int get_credential_field_name(ObSqlString &str, int64_t opt);
 private:
  static bool is_left_edge(const common::ObObj &value);
  static bool is_right_edge(const common::ObObj &value);
  static int64_t get_edge_value(const common::ObObj &edge);
  static int sort_external_files(ObIArray<ObString> &file_urls,
                          ObIArray<int64_t> &file_sizes);

};
}
}
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
