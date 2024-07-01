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
class ObQueryRange;
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
    MAX_EXTERNAL_FILE_SCANKEY
  };

 public:
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
                                       const uint64_t table_id,
                                       ObIArray<int64_t> &partition_ids,
                                       common::ObIArray<common::ObNewRange *> &ranges,
                                       common::ObIAllocator &range_allocator,
                                       common::ObIArray<common::ObNewRange *> &new_range,
                                       bool is_file_on_disk);

  static int calc_assigned_files_to_sqcs(
    const common::ObIArray<ObExternalFileInfo> &files,
    common::ObIArray<int64_t> &assigned_idx,
    int64_t sqc_count);

  static int filter_files_in_locations(common::ObIArray<share::ObExternalFileInfo> &files,
                                       common::ObIArray<common::ObAddr> &locations);

  static int collect_external_file_list(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
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

 private:
  static bool is_left_edge(const common::ObObj &value);
  static bool is_right_edge(const common::ObObj &value);
  static int64_t get_edge_value(const common::ObObj &edge);
  static int make_external_table_scan_range(const common::ObString &file_url,
                                            const int64_t file_id,
                                            const uint64_t ref_table_id,
                                            const int64_t first_lineno,
                                            const int64_t last_lineno,
                                            common::ObIAllocator &allocator,
                                            common::ObNewRange &new_range);

  static int sort_external_files(ObIArray<ObString> &file_urls,
                          ObIArray<int64_t> &file_sizes);

};
}
}
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
