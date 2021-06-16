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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_IO_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_IO_H_

#include "common/ob_partition_key.h"  // ObPGKey
#include "lib/restore/ob_storage.h"   // ObStorageAppender, ObStorageReader
#include "lib/oblog/ob_log_module.h"
#include "ob_log_archive_struct.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::common;
class ObArchiveIO {
  static const int64_t MAX_PATH_LENGTH = OB_MAX_ARCHIVE_PATH_LENGTH;

public:
  ObArchiveIO(const common::ObString& storage_info);
  ~ObArchiveIO()
  {}

public:
  int push_log(const char* path, const int64_t data_len, char* data, const bool new_file, const bool compatible,
      const bool is_data_file, const int64_t epoch);

  int get_index_file_range(const ObPGKey& pg_key, const int64_t incarnation, const int64_t round, uint64_t& min_file_id,
      uint64_t& max_file_id);

  int get_data_file_range(const ObPGKey& pg_key, const int64_t incarnation, const int64_t round, uint64_t& min_file_id,
      uint64_t& max_file_id);

  int check_and_make_dir(const ObPGKey& pg_key, ObString& uri);

  int check_file_exist(const ObPGKey& pg_key, const char* path, bool& file_exist);

  int get_max_archived_index_info(const ObPGKey& pg_key, const int64_t incarnation, const int64_t round,
      const uint64_t min_index_file_id, const uint64_t max_index_file_id, MaxArchivedIndexInfo& info);

private:
  int get_file_range_(const ObPGKey& pg_key, const LogArchiveFileType file_type, const int64_t incarnation,
      const int64_t round, uint64_t& min_file_id, uint64_t& max_file_id);

  int get_append_param_(const bool is_data_file, const int64_t epoch, ObStorageAppender::AppenderParam& param);

private:
  common::ObString storage_info_;
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_IO_H_ */
