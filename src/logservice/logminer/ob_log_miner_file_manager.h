/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_FILE_MANAGER_H_
#define OCEANBASE_LOG_MINER_FILE_MANAGER_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "share/backup/ob_backup_struct.h"
#include "ob_log_miner_file_index.h"
#include "ob_log_miner_progress_range.h"
#include "ob_log_miner_file_meta.h"
#include "ob_log_miner_analyzer_checkpoint.h"
#include "ob_log_miner_record_file_format.h"


namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerArgs;
class ObLogMinerBatchRecord;

class ILogMinerFileManager
{
public:
  // write operation must satisfy the following order
  // 1. write_config when logminer starts
  // 2. write_checkpoint after append_record
  // expect no parallel append to one file
  virtual int append_records(const ObLogMinerBatchRecord &batch_record) = 0;
  virtual int write_config(const ObLogMinerArgs &args) = 0;
  virtual int write_checkpoint(const ObLogMinerCheckpoint &ckpt) = 0;
  // read operation must satisfy the following order
  // 1. read_config when logminer restarts
  // 2. read_checkpoint before get_file_range to make sure get valid file range
  // 3. read_checkpoint before read_file to make sure read valid file
  virtual int read_file(const int64_t file_id, char *buf, const int64_t buf_len) = 0;
  virtual int get_file_range(const int64_t min_timestamp_us,
              const int64_t max_timestamp_us,
              int64_t &min_file_id,
              int64_t &max_file_id) = 0;
  virtual int read_config(ObLogMinerArgs &args) = 0;
  // read_checkpoint should be used only when logminer restarts
  // there should not be concurrency issue with write_checkpoint()
  virtual int read_checkpoint(ObLogMinerCheckpoint &ckpt) = 0;
};

struct FileIdWrapper {
  FileIdWrapper(const int64_t file_id): fid_(file_id) {}

  int hash(uint64_t &val) const {
    val = fid_;
    return OB_SUCCESS;
  }

  bool operator==(const FileIdWrapper &that) const {
    return fid_ == that.fid_;
  }

  int64_t fid_;
};

typedef ObLinearHashMap<FileIdWrapper, ObLogMinerFileMeta>  FileMetaMap;

class ObLogMinerFileManager: public ILogMinerFileManager
{
public:
  enum class FileMgrMode {
    INVALID = -1,
    ANALYZE = 0,
    FLASHBACK,
    RESTART
  };
  static const char *META_PREFIX;
  static const char *META_EXTENSION;
  static const char *CSV_SUFFIX;
  static const char *SQL_SUFFIX;
  static const char *CONFIG_FNAME;
  static const char *CHECKPOINT_FNAME;
  static const char *INDEX_FNAME;
  static const char *EXITCODE_FNAME;
public:
  virtual int append_records(const ObLogMinerBatchRecord &batch_record);
  virtual int write_config(const ObLogMinerArgs &args);
  virtual int write_checkpoint(const ObLogMinerCheckpoint &ckpt);
  virtual int read_file(const int64_t file_id, char *buf, const int64_t buf_len);
  virtual int get_file_range(const int64_t min_timestamp_us,
              const int64_t max_timestamp_us,
              int64_t &min_file_id,
              int64_t &max_file_id);
  virtual int read_config(ObLogMinerArgs &args);
  virtual int read_checkpoint(ObLogMinerCheckpoint &ckpt);

public:
  ObLogMinerFileManager();
  virtual ~ObLogMinerFileManager();
  int init(const char *path,
           const RecordFileFormat format,
           const FileMgrMode mode);
  void destroy();

private:
  int config_file_uri_(char *buf, const int64_t buf_len, int64_t &length) const;
  int checkpoint_file_uri_(char *buf, const int64_t buf_len, int64_t &length) const;
  int data_file_uri_(const int64_t file_id, char *buf, const int64_t buf_len, int64_t &length) const;
  int meta_file_uri_(const int64_t file_id, char *buf, const int64_t buf_len, int64_t &length) const;
  int index_file_uri_(char *buf, const int64_t buf_len, int64_t &length) const;
  const char *data_file_extension_() const;

  int create_data_file_(const int64_t file_id);
  int generate_data_file_header_(ObIAllocator &alloc, char *&data, int64_t &data_len);
  int append_data_file_(const int64_t file_id, const char *data, const int64_t data_len);
  int append_file_(const ObString &uri, const int64_t offset, const char *data, const int64_t data_len);
  int read_data_file_(const int64_t file_id, char *data, const int64_t data_len);
  // overwrite the whole file with data
  int write_data_file_(const int64_t file_id, const char *data, const int64_t data_len);
  int read_meta_(const int64_t file_id, ObLogMinerFileMeta &meta);
  int write_meta_(const int64_t file_id, const ObLogMinerFileMeta &meta);
  // int file_exists_(const char *file_path) const;

  // only called by write_checkpoint
  int write_index_(const int64_t file_id);
  // only called by write_checkpoint
  int update_last_write_ckpt_(const ObLogMinerCheckpoint &ckpt);

  int init_path_for_analyzer_();

private:
  bool is_inited_;
  share::ObBackupDest output_dest_;
  FileMgrMode mode_;
  RecordFileFormat format_;
  ObLogMinerCheckpoint last_write_ckpt_;
  FileMetaMap meta_map_;
  FileIndex file_index_;
};

}
}

#endif