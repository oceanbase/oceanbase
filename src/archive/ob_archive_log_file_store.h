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
#include "lib/string/ob_fixed_length_string.h"  //ObFixedLengthString
#include "clog/ob_log_define.h"                 // file_id_t
#include "lib/container/ob_seg_array.h"         // ObSegArray
#include "share/backup/ob_backup_struct.h"      // ObPhysicalRestoreInfo
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
public:
  ObArchiveLogFileStore();
  ~ObArchiveLogFileStore();

public:
  // virtual int init(const share::ObPhysicalRestoreInfo &restore_info);
  virtual int init(const share::ObBackupPiecePath& base_path);
  virtual int init(const char* root_path, const char* storage_info, const char* cluster_name, const int64_t cluster_id,
      const uint64_t tenant_id, const int64_t incarnation, const int64_t archive_round, const int64_t piece_id,
      const int64_t piece_create_date);

public:
  // locate archive data file id by log id
  virtual int locate_file_by_log_id(
      const ObPGKey& pg_key, const uint64_t log_id, uint64_t& file_id, bool& archive_file_exist) const;

  virtual int get_index_file_id_range(
      const common::ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id) const;
  virtual int get_data_file_id_range(const ObPGKey& pg_key, uint64_t& min_file_id, uint64_t& max_file_id);
  // read data file archived files
  // allocate_fail / file not exist / IO ERROR / other errors maybe return
  virtual int read_data_direct(const ObArchiveReadParam& param, clog::ObReadBuf& rbuf, clog::ObReadRes& res);

  // get pg max archived log id and checkpoint ts
  virtual int get_pg_max_archived_info(const common::ObPGKey& pg_key, const uint64_t real_tenant_id,
      uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts);

  // locate by index file
  // first find the index of the data file X contain the log id
  // return X -1 as result
  virtual int locate_file_by_log_id_for_clear(const common::ObPGKey& pg_key, const uint64_t log_id,
      const int64_t retention_timestamp, uint64_t& index_file_id, uint64_t& data_file_id);

  virtual int locate_file_by_log_ts_for_clear(const common::ObPGKey& pg_key, const int64_t log_ts,
      const int64_t retention_timestamp, uint64_t& index_file_id, uint64_t& data_file_id);

  virtual int get_archive_key_content(const ObPGKey& pg_key, ObArchiveKeyContent& archive_key_content);

  TO_STRING_KV(K(inited_), K(storage_info_));

private:
  int get_file_id_range_(const common::ObPGKey& pg_key, const LogArchiveFileType file_type, uint64_t& min_file_id,
      uint64_t& max_file_id) const;

  int get_max_data_file_unrecord_(
      const common::ObPGKey& pg_key, common::ObString& storage_info, uint64_t& file_id, bool& file_exist) const;

  int search_log_in_index_files_(const common::ObPGKey& pg_key, const uint64_t log_id, uint64_t& data_file_id,
      ObArchiveIndexFileInfo& max_valid_index_info, bool& index_file_exist) const;

  int search_log_in_single_index_file_(const common::ObPGKey& pg_key, const uint64_t log_id,
      const uint64_t index_file_id, uint64_t& data_file_id, bool& log_exist,
      ObArchiveIndexFileInfo& max_valid_index_info) const;

  int check_file_exist_(
      const common::ObPGKey& pg_key, const uint64_t file_id, const LogArchiveFileType file_type) const;

  int get_target_index_file_for_clear_(const common::ObPGKey& pg_key, const uint64_t log_id, const int64_t log_ts,
      const bool by_log_id, const int64_t retention_timestamp, const uint64_t min_index_file_id,
      const uint64_t max_index_file_id, uint64_t& target_index_file_id);

  int get_safe_data_file_for_clear_(const common::ObPGKey& pg_key, const uint64_t log_id, const int64_t log_ts,
      const bool by_log_id, const int64_t retention_timestamp, const uint64_t index_file_id, uint64_t& data_file_id);

  // get max invalid index record(is_invalid_ = true in index file) in index file
  int get_max_valid_index_info_in_single_index_file_(
      const common::ObPGKey& pg_key, const uint64_t file_id, ObArchiveIndexFileInfo& info, bool& exist);

  // get max archived info, including max log info and data file info in all index files
  int get_max_archived_info_from_index_files_(const common::ObPGKey& pg_key, MaxArchivedIndexInfo& info);

  int get_max_archived_info_from_single_index_file_(
      const common::ObPGKey& pg_key, const uint64_t file_id, MaxArchivedIndexInfo& info);

  int get_max_archived_info_from_data_file_(const common::ObPGKey& pg_key, const uint64_t real_tenant_id,
      const uint64_t file_id, uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts);

  // return OB_ENTRY_NOT_EXIST in following situation
  // 1. archive_key_file not exist
  // 2. archive_key_file is not complete
  // 3. archive_key_file is for first piece
  int get_max_archived_info_from_archive_key_(
      const ObPGKey& pg_key, uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts);
  int direct_extract_last_log_(const common::ObPGKey& pg_key, const uint64_t real_tenant_id, const uint64_t file_id,
      uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts);

  int iterate_max_archived_info_(const common::ObPGKey& pg_key, const uint64_t real_tenant_id, const uint64_t file_id,
      uint64_t& max_log_id, int64_t& max_checkpoint_ts, int64_t& max_log_ts);

private:
  static const int64_t MAX_PATH_LENGTH = share::OB_MAX_BACKUP_DEST_LENGTH;

private:
  bool inited_;
  common::ObFixedLengthString<OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH> storage_info_;
  common::ObFixedLengthString<MAX_PATH_LENGTH> base_path_;
};

}  // end of namespace archive
}  // end of namespace oceanbase
#endif  // OCEANBASE_ARCHIVE_LOG_FILE_STORE_
