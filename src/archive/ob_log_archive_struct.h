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

#ifndef OCEANBASE_LOG_ARCHIVE_STRUCT_
#define OCEANBASE_LOG_ARCHIVE_STRUCT_

#include <stdlib.h>
#include "common/ob_partition_key.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_se_array.h"
#include "common/log/ob_log_cursor.h"
#include "clog/ob_log_define.h"
#include "archive/ob_log_archive_define.h"
#include "archive/ob_archive_block.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace archive {
//================ start of structs for backup============//

struct ObArchiveReadBuf {
public:
  ObArchiveReadBuf()
  {
    reset();
  }
  ~ObArchiveReadBuf()
  {
    reset();
  }
  void reset()
  {
    MEMSET(buf_, 0, CLOG_BUF_SIZE);
    data_len_ = 0;
  }
  int64_t get_buf_len() const
  {
    return CLOG_BUF_SIZE;
  }
  TO_STRING_KV(K(data_len_));

public:
  static const int64_t CLOG_BUF_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
  int64_t data_len_;
  char buf_[CLOG_BUF_SIZE];
};

struct ObArchiveLogCursor {
public:
  ObArchiveLogCursor()
  {
    reset();
  }
  ~ObArchiveLogCursor()
  {
    reset();
  }
  int64_t get_submit_timestamp() const
  {
    return log_submit_ts_;
  }
  bool is_batch_committed() const
  {
    return is_batch_committed_;
  }
  bool is_valid() const
  {
    return file_id_ != common::OB_INVALID_FILE_ID && file_id_ != 0 && offset_ >= 0 && size_ > 0 && log_id_ > 0 &&
           log_submit_ts_ > 0;
  }
  void reset()
  {
    file_id_ = common::OB_INVALID_FILE_ID;
    offset_ = 0;
    size_ = 0;
    is_batch_committed_ = false;
    log_id_ = 0;
    log_submit_ts_ = 0;
    accum_checksum_ = 0;
  }

  void set(const file_id_t file_id, const offset_t offset, const int32_t size, const bool is_batch_committed,
      const uint64_t log_id, const int64_t log_ts, const int64_t accum_checksum)
  {
    file_id_ = file_id;
    offset_ = offset;
    size_ = size;
    is_batch_committed_ = is_batch_committed;
    log_id_ = log_id;
    log_submit_ts_ = log_ts;
    accum_checksum_ = accum_checksum;
  }

  int assign(const ObArchiveLogCursor& cursor)
  {
    file_id_ = cursor.file_id_;
    offset_ = cursor.offset_;
    size_ = cursor.size_;
    is_batch_committed_ = cursor.is_batch_committed_;
    log_id_ = cursor.log_id_;
    log_submit_ts_ = cursor.log_submit_ts_;
    accum_checksum_ = cursor.accum_checksum_;
    return common::OB_SUCCESS;
  }

  TO_STRING_KV(
      K_(file_id), K_(offset), K_(size), K_(is_batch_committed), K_(log_id), K_(log_submit_ts), K_(accum_checksum));

public:
  file_id_t file_id_;
  offset_t offset_;
  int32_t size_;
  bool is_batch_committed_;
  uint64_t log_id_;
  int64_t log_submit_ts_;
  int64_t accum_checksum_;
};

struct ObArchiveRoundStartInfo {
public:
  ObArchiveRoundStartInfo()
  {
    reset();
  }
  ~ObArchiveRoundStartInfo()
  {
    reset();
  }
  void reset();
  bool is_valid() const;

public:
  TO_STRING_KV(
      K(start_ts_), K(start_log_id_), K(snapshot_version_), K(log_submit_ts_), K(clog_epoch_id_), K(accum_checksum_));

public:
  int64_t start_ts_;       // round_start_ts_
  uint64_t start_log_id_;  // round_start_log_id_
  int64_t snapshot_version_;
  int64_t log_submit_ts_;   //(start_log_id - 1) log_submit_ts
  int64_t clog_epoch_id_;   //(start_log_id - 1) clog_epoch_id
  int64_t accum_checksum_;  //(start_log_id - 1) accum_checksum
};

struct ObPGArchiveCLogTask : public common::ObLink {
public:
  ObPGArchiveCLogTask() : read_buf_(NULL)
  {
    reset();
  }
  virtual ~ObPGArchiveCLogTask()
  {
    reset();
  }
  void reset();
  bool is_valid() const;
  bool is_archive_checkpoint_task() const
  {
    return OB_ARCHIVE_TASK_TYPE_CHECKPOINT == task_type_;
  }
  bool is_inner_task() const
  {
    return OB_ARCHIVE_TASK_TYPE_KICKOFF == task_type_ || OB_ARCHIVE_TASK_TYPE_CHECKPOINT == task_type_;
    ;
  }

public:
  clog::ObLogType get_log_type() const;
  int64_t get_clog_count() const
  {
    return clog_pos_list_.count();
  }
  uint64_t get_round_start_log_id() const;
  uint64_t get_last_processed_log_id() const;
  bool is_finished() const;
  bool need_compress() const;
  ObPGArchiveCLogTask* get_next()
  {
    return next_;
  }
  void set_next(ObPGArchiveCLogTask* next)
  {
    next_ = next;
  }
  uint64_t get_last_log_id() const;
  int64_t get_last_log_submit_ts() const
  {
    return log_submit_ts_;
  };
  int64_t get_last_checkpoint_ts() const
  {
    return checkpoint_ts_;
  }
  bool need_update_log_ts() const
  {
    return need_update_log_ts_;
  }

  int init_clog_split_task(const common::ObPGKey& pg_key, const int64_t incarnation, const int64_t archive_round,
      const int64_t epoch_id, const uint64_t log_id, const int64_t generate_ts,
      common::ObCompressorType compressor_type);
  int init_kickoff_task(const common::ObPGKey& pg_key, const int64_t incarnation, const int64_t archive_round,
      const int64_t epoch_id, const ObArchiveRoundStartInfo& start_info, const int64_t checkpoint_ts,
      common::ObCompressorType compressor_type);
  int init_checkpoint_task(const common::ObPGKey& pg_key, const int64_t incarnation, const int64_t archive_round,
      const int64_t epoch_id, const uint64_t log_id, const int64_t checkpoint_ts, const int64_t log_submit_ts,
      common::ObCompressorType compressor_type);
  int prepare_read_buf();
  void release_read_buf();

  TO_STRING_KV(K_(epoch_id), K_(need_update_log_ts), K_(round_start_log_id), K_(round_start_ts), K_(checkpoint_ts),
      K_(log_submit_ts), K_(clog_epoch_id), K_(accum_checksum), K_(processed_log_count), K_(incarnation),
      K_(log_archive_round), K_(task_type), K_(compressor_type), K_(pg_key), K_(clog_pos_list));

public:
  int64_t epoch_id_;
  bool need_update_log_ts_;
  union {
    uint64_t round_start_log_id_;
    uint64_t archive_base_log_id_;
    uint64_t archive_checkpoint_log_id_;
  };

  union {
    int64_t round_start_ts_;
    int64_t generate_ts_;  //
  };
  int64_t round_snapshot_version_;
  // valid for archive_checkpoint task and kickoff task
  int64_t checkpoint_ts_;
  // valid for archive_checkpoint task and kickoff task before spliting, after spliting,log_submit_ts may
  // be the submit_timestamp of last log
  int64_t log_submit_ts_;

  // clog_epoch_id_ and accum_checksum_ respond to epoch_id and accum_checksum of the last log in task
  // valid in kickoff and clog_split task, not valid in checkpoint task
  int64_t clog_epoch_id_;   // epoch_id in clog_info
  int64_t accum_checksum_;  // accum_checksum in clog_info

  int64_t processed_log_count_;  //
  int64_t incarnation_;
  int64_t log_archive_round_;
  ObLogArchiveContentType task_type_;
  common::ObCompressorType compressor_type_;  // compression algorithm, NONE_COMPRESSOR means not compress
  common::ObPGKey pg_key_;
  ObArchiveReadBuf* read_buf_;
  ObPGArchiveCLogTask* next_;
  common::ObSEArray<ObArchiveLogCursor, DEFAULT_CLOG_CURSOR_NUM> clog_pos_list_;  // log cursor array
};

struct ObArchiveSendTaskMeta {
public:
  ObArchiveSendTaskMeta()
  {
    reset();
  }
  virtual ~ObArchiveSendTaskMeta()
  {
    reset();
  }
  virtual void reset();
  int assign(const ObArchiveSendTaskMeta& other);

public:
  virtual bool is_valid() const;
  int64_t get_data_len() const
  {
    return pos_;
  }
  void set_data_len(const int64_t data_len)
  {
    pos_ = data_len;
  }
  bool is_kickoff() const
  {
    return OB_ARCHIVE_TASK_TYPE_KICKOFF == task_type_;
  }
  int set_archive_meta_info(const ObPGArchiveCLogTask& clog_task);
  VIRTUAL_TO_STRING_KV(K_(pg_key), K_(task_type), K_(need_update_log_ts), K_(epoch_id), K_(incarnation),
      K_(log_archive_round), K_(start_log_id), K_(start_log_ts), K_(end_log_id), K_(end_log_submit_ts),
      K_(checkpoint_ts), K_(pos), K_(block_meta));

public:
  // attention!!!!:do not forget to modify assign() when adding new members
  common::ObPGKey pg_key_;
  ObLogArchiveContentType task_type_;
  // need update update log_submit_ts and checkpoint_ts in sender callback or not
  // only clog including non-NOP log need update log ts
  bool need_update_log_ts_;
  int64_t epoch_id_;
  int64_t incarnation_;
  int64_t log_archive_round_;

  uint64_t start_log_id_;
  int64_t start_log_ts_;
  uint64_t end_log_id_;
  int64_t end_log_submit_ts_;
  int64_t checkpoint_ts_;

  int64_t pos_;  // offset of valid data
  ObArchiveBlockMeta block_meta_;
};

struct ObTSIArchiveReadBuf : public ObArchiveSendTaskMeta {
public:
  ObTSIArchiveReadBuf()
  {
    reset();
  }
  ~ObTSIArchiveReadBuf()
  {
    reset();
  }
  void reset_log_related_info();

public:
  int64_t get_buf_len() const
  {
    return READ_BUF_SIZE;
  }
  char* get_buf()
  {
    return buf_;
  }
  const char* get_buf() const
  {
    return buf_;
  }

private:
  // max size of each clog read
  static const int64_t READ_BUF_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
  char buf_[READ_BUF_SIZE];
};

struct ObTSIArchiveCompressBuf {
public:
  ObTSIArchiveCompressBuf()
  {
    reset();
  }
  ~ObTSIArchiveCompressBuf()
  {
    reset();
  }
  void reset()
  {
    pos_ = 0;
  }

public:
  int64_t get_buf_len() const
  {
    return COMPRESSED_BUF_SIZE;
  }
  char* get_buf()
  {
    return buf_;
  }
  const char* get_buf() const
  {
    return buf_;
  }
  int64_t get_data_size() const
  {
    return pos_;
  }

public:
  static const int64_t MAX_COMPRESS_OVERFLOW_SIZE = 20 * 1024L;  // 20K reserved
  static const int64_t COMPRESSED_BUF_SIZE = common::OB_MAX_LOG_BUFFER_SIZE + MAX_COMPRESS_OVERFLOW_SIZE;
  int64_t pos_;
  char buf_[COMPRESSED_BUF_SIZE];
};

struct ObArchiveSendTask : public ObArchiveSendTaskMeta, public common::ObLink {
public:
  ObArchiveSendTask()
  {
    reset();
  }
  ~ObArchiveSendTask()
  {
    reset();
  }
  void reset();

public:
  bool has_enough_space(const int64_t data_size) const;
  int assign_meta(const ObTSIArchiveReadBuf& send_buf);
  bool is_valid() const
  {
    return (ObArchiveSendTaskMeta::is_valid() && NULL != buf_ && buf_len_ > 0);
  }
  int64_t get_buf_len() const
  {
    return buf_len_;
  }
  void set_buf_len(int64_t len)
  {
    buf_len_ = len;
  }
  char* get_buf()
  {
    return buf_;
  }
  void set_archive_data_len(int64_t len)
  {
    archive_data_len_ = len;
  }
  INHERIT_TO_STRING_KV("ObArchiveSendTaskMeta", ObArchiveSendTaskMeta, K_(archive_data_len), K_(buf_len), KP(buf_));

public:
  int64_t buf_len_;
  int64_t archive_data_len_;  // data len if archive block
  char* buf_;
};

struct ObArchiveIndexFileInfo {
public:
  ObArchiveIndexFileInfo()
  {
    reset();
  }
  void reset();
  int build_valid_record(const uint64_t data_file_id, const uint64_t min_log_id, const int64_t min_log_ts,
      const uint64_t max_log_id, const int64_t max_log_submit_ts, const int64_t checkpoint_ts,
      const int64_t clog_epoch_id, const int64_t accum_checksum, const ObArchiveRoundStartInfo& round_start_info);
  int build_invalid_record(const uint64_t data_file_id, const uint64_t max_log_id, const int64_t checkpoint_ts,
      const int64_t max_log_submit_ts, const int64_t clog_epoch_id, const int64_t accum_checksum,
      const ObArchiveRoundStartInfo& round_start_info);
  bool is_valid();
  int get_real_record_length(const char* buf, const int64_t buf_len, int64_t& record_len);
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K(magic_), K(record_len_), K(version_), K(is_valid_), K(data_file_id_), K(input_bytes_),
      K(output_bytes_), K(clog_epoch_id_), K(accum_checksum_), K(min_log_id_), K(min_log_ts_), K(max_log_id_),
      K(max_checkpoint_ts_), K(max_log_submit_ts_), K(round_start_ts_), K(round_start_log_id_),
      K(round_snapshot_version_), K(round_log_submit_ts_), K(round_clog_epoch_id_), K(round_accum_checksum_),
      K(checksum_));

private:
  int64_t calc_checksum_() const;

public:
  static const int16_t MAGIC_NUM = share::ObBackupFileType::BACKUP_ARCHIVE_INDEX_FILE;  // AI means ARCHIVE INDEX
  static const int16_t INDEX_FILE_VERSION = 1;

public:
  int16_t magic_;
  int16_t record_len_;
  int16_t version_;
  int16_t is_valid_;
  uint64_t data_file_id_;

  // TODO:stat in
  int64_t input_bytes_;  // for stat
  int64_t output_bytes_;

  int64_t clog_epoch_id_;   // for clog_info, corresponding to max_log_id_
  int64_t accum_checksum_;  // for clog_info, corresponding to max_log_id_

  uint64_t min_log_id_;
  int64_t min_log_ts_;  // obsolete
  uint64_t max_log_id_;
  int64_t max_checkpoint_ts_;
  int64_t max_log_submit_ts_;

  // round start info
  int64_t round_start_ts_;       // round_start_ts_
  uint64_t round_start_log_id_;  // round_start_log_id_
  int64_t round_snapshot_version_;
  int64_t round_log_submit_ts_;   // corresponding to log_submit_ts of (round_start_log_id - 1)
  int64_t round_clog_epoch_id_;   // corresponding to clog_epoch_id of (round_start_log_id - 1)
  int64_t round_accum_checksum_;  // corresponding to accum_checksum of (round_start_log_id - 1)
  // calculate by crc64, members should be 64-bit aligned
  int64_t checksum_;
};

struct MaxArchivedIndexInfo {
public:
  MaxArchivedIndexInfo();
  ~MaxArchivedIndexInfo();
  void reset();
  bool is_index_info_collected_();
  void set_data_file_id(const uint64_t file_id);
  int set_log_info(const uint64_t max_log_id, const int64_t max_checkpoint_ts, const int64_t max_log_submit_ts,
      const int64_t clog_epoch_id, const int64_t accum_checksum);
  int set_round_start_info(const ObArchiveIndexFileInfo& index_info);
  // flag of index record searched or not
  bool data_file_collect_;
  bool archived_log_collect_;
  bool round_start_info_collect_;
  uint64_t max_record_data_file_id_;
  uint64_t max_record_log_id_;
  int64_t max_record_checkpoint_ts_;
  int64_t max_record_log_submit_ts_;
  int64_t clog_epoch_id_;
  int64_t accum_checksum_;

  ObArchiveRoundStartInfo round_start_info_;
  // round_start info

  TO_STRING_KV(K(data_file_collect_), K(archived_log_collect_), K(round_start_info_collect_),
      K(max_record_data_file_id_), K(max_record_log_id_), K(max_record_checkpoint_ts_), K(max_record_log_submit_ts_),
      K(clog_epoch_id_), K(accum_checksum_), K(round_start_info_));
};

struct ObArchiveStartTimestamp {
public:
  ObArchiveStartTimestamp() : archive_round_(-1), timestamp_(-1), checksum_(0)
  {}
  int set(const int64_t archive_round, const int64_t timestamp);
  bool is_valid();
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K(archive_round_), K(timestamp_), K(checksum_));

private:
  int64_t calc_checksum_() const;

public:
  int64_t archive_round_;
  int64_t timestamp_;
  int64_t checksum_;
};

class ObArchiveCompressedChunkHeader {
public:
  ObArchiveCompressedChunkHeader()
  {
    reset();
  }
  virtual ~ObArchiveCompressedChunkHeader()
  {
    reset();
  }
  void reset();

public:
  bool is_valid() const;
  int generate_header(
      const common::ObCompressorType compressor_type, const int32_t orig_data_len, const int32_t compress_data_len);
  int16_t get_magic_num() const
  {
    return magic_;
  }
  int16_t get_version() const
  {
    return version_;
  }
  int32_t get_data_len() const
  {
    return compressed_data_len_;
  }
  int32_t get_compressed_data_len() const
  {
    return compressed_data_len_;
  }
  int32_t get_orig_data_len() const
  {
    return orig_data_len_;
  }

  common::ObCompressorType get_compressor_type() const
  {
    return compressor_type_;
  }
  TO_STRING_KV(K(magic_), K(version_), K(orig_data_len_), K(compressed_data_len_), K(compressor_type_));
  NEED_SERIALIZE_AND_DESERIALIZE;
  static bool check_magic_number(const int16_t magic_number)
  {
    return ARCHIVE_COMPRESSED_CHUNK_MAGIC == magic_number;
  }

private:
  static const int16_t ARCHIVE_COMPRESSED_CHUNK_MAGIC = 0x4343;  // CC means compressed chunk
  static const int16_t ARCHIVE_COMPRESSED_CHUNK_VERSION = 1;     // for compat
private:
  int16_t magic_;    // COMPRESSED_LOG_CHUNK_MAGIC
  int16_t version_;  // for compactibility
  common::ObCompressorType compressor_type_;
  int32_t orig_data_len_;  // original data len, that is buffer len before compression
  // buffer len after compression, log uncompressed if compressed_data_len_ == orig_data_len_
  int32_t compressed_data_len_;
};

class ObArchiveCompressedChunk {
public:
  ObArchiveCompressedChunk() : header_(), buf_(NULL)
  {}
  virtual ~ObArchiveCompressedChunk();

public:
  const ObArchiveCompressedChunkHeader& get_header() const
  {
    return header_;
  }
  const char* get_buf() const
  {
    return buf_;
  }
  TO_STRING_KV(K(header_));
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveCompressedChunk);

private:
  ObArchiveCompressedChunkHeader header_;
  const char* buf_;
};
//================  end of structs for backup============//

//================ start of structs for restoring============//
struct ObArchiveReadParam {
  uint64_t file_id_;  // read file id
  int64_t offset_;    // read file offset
  int64_t read_len_;  // read data len
  int64_t timeout_;   // read file cost
  common::ObPGKey pg_key_;

  ObArchiveReadParam()
  {
    reset();
  }
  ~ObArchiveReadParam()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K(pg_key_), K(file_id_), K(offset_), K(read_len_), K(timeout_));

private:
  static const int64_t OB_ARCHIVE_READ_TIMEOUT = 5000000;  // 5s
};

//================  end of structs for restoring============//

}  // end of namespace archive
}  // end of namespace oceanbase
#endif  // OCEANBASE_LOG_ARCHIVE_STRUCT_
