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

#ifndef OCEANBASE_CLOG_OB_LOG_FETCHER_IMPL_
#define OCEANBASE_CLOG_OB_LOG_FETCHER_IMPL_

//#include "lib/allocator/ob_qsync.h"
#include "ob_log_define.h"
#include "ob_log_reader_interface.h"
#include "ob_log_line_cache.h"  // ObLogLineCache
#include "storage/ob_storage_log_type.h"
#include "share/ob_encryption_util.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroup;
}
namespace clog {
class ObILogEngine;
struct ObReadParam;
class ObLogCursorExt;
}  // namespace clog
namespace logservice {
class ObLogFetcherImpl {
public:
  ObLogFetcherImpl() : is_inited_(false), skip_hotcache_(false), log_engine_(NULL), line_cache_(NULL)
  {}
  ~ObLogFetcherImpl()
  {
    destroy();
  }
  int init(clog::ObLogLineCache& line_cache, clog::ObILogEngine* log_engine);
  void destroy()
  {
    if (is_inited_) {
      is_inited_ = false;
      skip_hotcache_ = false;
      line_cache_ = NULL;
      log_engine_ = NULL;
    }
  }

protected:
  int fetch_decrypted_log_entry_(const common::ObPartitionKey& pkey, const clog::ObLogCursorExt& cursor_ext,
      char* log_buf, const int64_t buf_size, const int64_t end_tstamp, clog::ObReadCost& read_cost,
      bool& fetch_log_from_hot_cache, int64_t& log_entry_size);

  int fetch_decrypted_log_entry_(const common::ObPartitionKey& pkey, const clog::ObReadParam& param, char* log_buf,
      const int64_t buf_size, const int64_t end_tstamp, clog::ObReadCost& read_cost, bool& fetch_log_from_hot_cache,
      int64_t& log_entry_size);

  int fetch_log_entry_(const common::ObPartitionKey& pkey, const clog::ObReadParam& param, char* log_buf,
      const int64_t buf_size, const int64_t end_tstamp, clog::ObReadCost& read_cost, bool& fetch_log_from_hot_cache,
      int64_t& log_entry_size);
  int read_data_from_line_cache(clog::file_id_t file_id, clog::offset_t offset, int64_t request_size, char* buf,
      const int64_t end_tstamp, clog::ObReadCost& read_cost);
  int read_data_from_line_cache_(const clog::file_id_t file_id, const clog::offset_t offset, const int64_t read_size,
      char* buf, int64_t& pos, const int64_t end_tstamp, clog::ObReadCost& read_cost);
  int read_line_data_(
      const char* line, const clog::offset_t offset_in_line, char* buf, int64_t& pos, const int64_t read_size);
  int load_line_data_(char* line, const clog::file_id_t file_id, const clog::offset_t offset, clog::ObReadCost& cost);
  // Read the serialized content of the decompressed log entry
  int read_uncompressed_data_from_line_cache_(clog::file_id_t file_id, clog::offset_t offset, int64_t request_size,
      char* buf, int64_t buf_size, int64_t& log_entry_size, const int64_t end_tstamp, clog::ObReadCost& read_cost);

protected:
  bool is_inited_;
  bool skip_hotcache_;  // use for test line cache
  clog::ObILogEngine* log_engine_;
  clog::ObLogLineCache* line_cache_;
};

}  // namespace logservice
}  // namespace oceanbase

#endif
