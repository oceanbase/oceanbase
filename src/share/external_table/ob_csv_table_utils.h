/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_CSV_TABLE_UTILS_H_
#define _OB_CSV_TABLE_UTILS_H_

#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{
namespace share
{

class ObCsvTableUtils {
public:
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
  static int adjust_end_pos_skip_escape(sql::ObExternalStreamFileReader &file_reader,
                                        const char escape,
                                        const int64_t start_pos,
                                        int64_t &end_pos);
  static bool is_satisfied_for_parallel_parse_csv(
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      const ObCSVGeneralFormat &csv_format,
      const int64_t parallelism);
  // definition is after the class (template implementation section)
  template <typename HandleFunc>
  static int read_data_for_bound(const ObString &external_location,
                                 const ObString &external_access_info,
                                 const ObExternalFileFormat &external_file_format,
                                 const ObString &url,
                                 const int64_t start_pos,
                                 const int64_t end_pos,
                                 HandleFunc &handle_func);
};

// =====================================================================
// 模板实现（体量大，移出类外保持声明区紧凑）
// =====================================================================
  template <typename HandleFunc>
int ObCsvTableUtils::read_data_for_bound(const ObString &external_location,
                                 const ObString &external_access_info,
                                 const ObExternalFileFormat &external_file_format,
                                 const ObString &url,
                                 const int64_t start_pos,
                                 const int64_t end_pos,
                                 HandleFunc &handle_func) {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator_for_read;
    char *buf = nullptr;
    bool use_prefetch = false;
    ObSqlString full_path;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      ObStorageType storage_type = OB_STORAGE_MAX_TYPE;
      OZ (get_storage_type_from_path_for_external_table(external_location, storage_type));
      use_prefetch = tenant_config->_enable_external_table_prefetch
                     && !std::is_same<HandleFunc, GamblingFunctor>::value  // 只对全量扫描界定预取
                     && storage_type != OB_STORAGE_FILE;
    }
    int64_t buf_len = use_prefetch ? OB_MALLOC_BIG_BLOCK_SIZE : 2 * 1024 * 1024;
    if (OB_SUCC(ret) && OB_ISNULL(buf = (char *)allocator_for_read.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for buffer", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 0, buf_len);
      if (!ObExternalTableUtils::is_abs_url(url)) {
        OZ (full_path.append_fmt("%.*s%s%.*s", external_location.length(), external_location.ptr(),
                                  (external_location.empty() || external_location[external_location.length() - 1] == '/') ? "" : "/",
                                  url.length(), url.ptr()));
      } else {
        OZ (full_path.assign(url));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!use_prefetch) {
      ObExternalStreamFileReader file_reader;
      OZ (file_reader.init(external_location, external_access_info,
                          external_file_format.csv_format_.compression_algorithm_,
                          allocator_for_read));
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
      file_reader.close();
    } else {
      ObCSVPrefetchMgr prefetch_mgr;
      OZ (prefetch_mgr.init(allocator_for_read, external_file_format.csv_format_.compression_algorithm_, THIS_WORKER.get_timeout_ts()));
      ObExternalFileUrlInfo *file_url_info = nullptr;
      OZ (ObExternalTableUtils::create_external_file_url_info(external_location, external_access_info, full_path.string(), allocator_for_read, file_url_info));
      ObExternalFileCacheOptions cache_options;  // no cache for csv prefetch
      OZ (prefetch_mgr.open(*file_url_info, cache_options, start_pos, end_pos));
      int64_t already_read_size = 0;
      int64_t target_read_size = end_pos - start_pos;
      while (OB_SUCC(ret)
            && !handle_func.is_finished_
            && already_read_size < target_read_size
            && !prefetch_mgr.eof()) {
        int64_t read_size = 0;
        OZ (prefetch_mgr.get_buffer(buf, buf_len, read_size));
        if (read_size > 0) {
          int check_size = min(read_size, target_read_size - already_read_size);
          OZ (handle_func(external_file_format, buf, check_size));
        }
        already_read_size += read_size;
      }
      prefetch_mgr.close();
    }

    allocator_for_read.free(buf);
    return ret;
  }

} // namespace share
} // namespace oceanbase
#endif /* _OB_CSV_TABLE_UTILS_H_ */
