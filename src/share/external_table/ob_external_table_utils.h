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

namespace oceanbase
{

namespace common
{
class ObObj;
class ObNewRange;
}

namespace sql
{
class ObDASTabletLoc;
class ObExecContext;
class ObExternalTableAccessService;
class ObQueryRange;
}

namespace share
{
class ObExternalTableUtils {
 public:
  enum ExternalTableRangeColumn {
    PARTITION_ID = 0,
    FILE_URL,
    FILE_ID,
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
                                       common::ObIArray<common::ObNewRange *> &ranges,
                                       common::ObIAllocator &range_allocator,
                                       common::ObIArray<common::ObNewRange *> &new_range,
                                       bool is_file_on_disk);

  static int filter_external_table_files(const common::ObString &pattern,
                                         sql::ObExecContext &exec_ctx,
                                         common::ObIArray<common::ObString> &file_urls);
  static int calc_assigned_files_to_sqcs(
    const common::ObIArray<ObExternalFileInfo> &files,
    common::ObIArray<int64_t> &assigned_idx,
    int64_t sqc_count);
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
};
}
}
#endif /* OBDEV_SRC_EXTERNAL_TABLE_UTILS_H_ */
