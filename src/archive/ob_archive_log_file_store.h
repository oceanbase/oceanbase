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

#ifndef OCEANBASE_ARCHIVE_LOG_FILE_STORE_
#define OCEANBASE_ARCHIVE_LOG_FILE_STORE_

#include "lib/compress/ob_compressor_pool.h"
#include "clog/ob_log_define.h"             // file_id_t
#include "lib/container/ob_seg_array.h"     // ObSegArray
#include "share/backup/ob_backup_struct.h"  // ObPhysicalRestoreInfo
#include "common/ob_partition_key.h"
#include "ob_log_archive_struct.h"  // *_LENGTH

namespace oceanbase {
namespace clog {
struct ObReadBuf;
struct ObReadRes;
}  // namespace clog
namespace common {
class ObString;
}
namespace share {
struct ObPhysicalRestoreInfo;
}
namespace archive {
struct ObArchiveReadParam;

// used for ob_admin
class ObIArchiveLogFileStore {
public:
  ObIArchiveLogFileStore(){};
  virtual ~ObIArchiveLogFileStore(){};

public:
  virtual int read_data_direct(const ObArchiveReadParam& param, clog::ObReadBuf& rbuf, clog::ObReadRes& res) = 0;
};

class ObArchiveLogFileStore : public ObIArchiveLogFileStore {
  static const int64_t MAX_STORAGE_INFO_LENGTH = OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH;
  static const int64_t MAX_PATH_LENGTH = OB_MAX_ARCHIVE_PATH_LENGTH;

public:
  ObArchiveLogFileStore();
  ~ObArchiveLogFileStore();

public:
  virtual int init_by_restore_info(const share::ObPhysicalRestoreInfo& restore_info);
  virtual int init(const char* root_path, const char* storage_info, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t incarnation, const int64_t archive_round);

public:
  // locate archive data file id by log id
  virtual int locate_file_by_log_id(const common::ObPGKey& pg_key, const uint64_t log_id, uint64_t& file_id);

  virtual int get_index_file_id_range(const common::ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id);
  virtual int get_data_file_id_range(const common::ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id);
  virtual int read_data_direct(const ObArchiveReadParam& param, clog::ObReadBuf& rbuf, clog::ObReadRes& res);

  virtual int get_pg_max_archived_info(const common::ObPGKey& pg_key, uint64_t& max_log_id, int64_t& max_checkpoint_ts);

  virtual int locate_file_by_log_id_for_clear(
      const common::ObPGKey& pg_key, const uint64_t log_id, uint64_t& index_file_id, uint64_t& data_file_id);

  virtual int locate_file_by_log_ts_for_clear(
      const common::ObPGKey& pg_key, const int64_t log_ts, uint64_t& index_file_id, uint64_t& data_file_id);

  TO_STRING_KV(K(inited_), K(storage_info_), K(restore_info_));

private:
  int get_file_id_range_(
      const common::ObPGKey& pg_key, const LogArchiveFileType file_type, uint64_t& min_file_id, uint64_t& max_file_id);

  int get_max_data_file_unrecord_(
      const common::ObPGKey& pg_key, common::ObString& storage_info, uint64_t& file_id, bool& file_exist);

  int search_log_in_index_files_(const common::ObPGKey& pg_key, const uint64_t log_id, uint64_t& data_file_id,
      ObArchiveIndexFileInfo& max_valid_index_info);

  int search_log_in_single_index_file_(const common::ObPGKey& pg_key, const uint64_t log_id,
      const uint64_t index_file_id, uint64_t& data_file_id, bool& log_exist,
      ObArchiveIndexFileInfo& max_valid_index_info);

  int check_file_exist_(const common::ObPGKey& pg_key, const uint64_t file_id, const LogArchiveFileType file_type);

  int get_target_index_file_for_clear_(const common::ObPGKey& pg_key, const uint64_t log_id, const int64_t log_ts,
      const bool by_log_id, const uint64_t min_index_file_id, const uint64_t max_index_file_id,
      uint64_t& target_index_file_id);

  int get_safe_data_file_for_clear_(const common::ObPGKey& pg_key, const uint64_t log_id, const int64_t log_ts,
      const bool by_log_id, const uint64_t index_file_id, uint64_t& data_file_id);

  int get_max_archived_index_info_(const common::ObPGKey& pg_key, ObArchiveIndexFileInfo& info, bool& exist);

  int get_max_archive_info_in_single_index_file_(
      const common::ObPGKey& pg_key, const uint64_t file_id, ObArchiveIndexFileInfo& info, bool& exist);

  int get_max_archived_info_from_data_file_(
      const common::ObPGKey& pg_key, const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts);

  int direct_extract_last_log_(
      const common::ObPGKey& pg_key, const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts);

  int iterate_max_archived_info_(
      const common::ObPGKey& pg_key, const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts);

private:
  bool inited_;
  char storage_info_[MAX_STORAGE_INFO_LENGTH];
  char base_path_[MAX_PATH_LENGTH];
  share::ObPhysicalRestoreInfo restore_info_;
};

}  // end of namespace archive
}  // end of namespace oceanbase
#endif  // OCEANBASE_ARCHIVE_LOG_FILE_STORE_
