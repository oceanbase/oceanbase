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

#ifndef OCEANBASE_CLOG_OB_LOG_ENTRY_
#define OCEANBASE_CLOG_OB_LOG_ENTRY_

#include "lib/allocator/ob_allocator.h"
#include "lib/compress/ob_compressor_pool.h"
#include "ob_log_entry_header.h"

namespace oceanbase {
namespace clog {
class ObLogEntry {
public:
  ObLogEntry();
  ~ObLogEntry();
  int generate_entry(const ObLogEntryHeader& header, const char* buf);
  // the memory allocation space of the log body is maintained by allocator
  // Complete the memory allocation inside the function, and the user releases the memory by himself
  int deep_copy_to(ObLogEntry& entry) const;
  bool check_integrity() const;

  // Whether to ignore the batch commit mark in the verification process
  // The batch commit mark is not recorded by default in clog.
  // In order to pass the batch commit mark to liboblog, the batch commit mark will be artificially added in the log
  // RPC. But the header checksum value will not be updated, so when checking the checksum, you need to clear the batch
  // commit mark first
  bool check_integrity(const bool ignore_batch_commit_flag);

  void reset();
  void destroy();
  bool operator==(const ObLogEntry& entry) const;
  const ObLogEntryHeader& get_header() const
  {
    return header_;
  }
  const char* get_buf() const
  {
    return buf_;
  }
  bool is_batch_committed() const
  {
    return header_.is_batch_committed();
  }
  int64_t get_total_len() const
  {
    return header_.get_total_len();
  }
  int update_header_proposal_id(const common::ObProposalID& new_proposal_id)
  {
    return header_.update_proposal_id(new_proposal_id);
  }
  int get_next_replay_ts_for_rg(int64_t& next_replay_ts) const;
  TO_STRING_KV(N_HEADER, header_);
  NEED_SERIALIZE_AND_DESERIALIZE;

protected:
  int deep_copy_to_(ObLogEntry& entry) const;

private:
  ObLogEntryHeader header_;
  const char* buf_;
  DISALLOW_COPY_AND_ASSIGN(ObLogEntry);
};

// ilog entry
class ObIndexEntry {
  OB_UNIS_VERSION(1);

public:
  ObIndexEntry();
  ~ObIndexEntry()
  {
    reset();
  }
  int init(const common::ObPartitionKey& partition_key, const uint64_t log_id, const file_id_t file_id,
      const offset_t offset, const int32_t size, const int64_t submit_timestamp, const int64_t accum_checksum,
      const bool batch_committed);
  void reset();
  // copy to this
  int shallow_copy(const ObIndexEntry& index);
  bool operator==(const ObIndexEntry& index) const;
  bool check_magic_num() const
  {
    return INDEX_MAGIC == magic_;
  }
  bool check_integrity() const
  {
    return check_magic_num();
  }
  // get member variable
  int16_t get_magic_num() const
  {
    return magic_;
  }
  int16_t get_version() const
  {
    return version_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  file_id_t get_file_id() const;
  int32_t get_offset() const
  {
    return offset_;
  }
  int32_t get_size() const
  {
    return size_;
  }
  int64_t get_submit_timestamp() const
  {
    return submit_timestamp_ & (MASK - 1);
  }
  bool is_batch_committed() const
  {
    bool bool_ret = false;
    if (common::OB_INVALID_TIMESTAMP != submit_timestamp_) {
      bool_ret = submit_timestamp_ & MASK;
    }
    return bool_ret;
  }
  int64_t get_accum_checksum() const
  {
    return accum_checksum_;
  }
  int64_t get_total_len() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(N_MAGIC, magic_, N_VERSION, version_, N_PARTITION_KEY, partition_key_, N_LOG_ID, log_id_, N_FILE_ID,
      file_id_, N_OFFSET, offset_, N_SIZE, size_, N_SUBMIT_TIMESTAMP, get_submit_timestamp(), "is_batch_committed",
      is_batch_committed(), K_(accum_checksum));

private:
  static const int16_t INDEX_MAGIC = 0x494E;  // IN means index
  static const int16_t INDEX_VERSION = 1;
  static const uint64_t MASK = 1ull << 63;

private:
  int16_t magic_;
  int16_t version_;
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;
  file_id_t file_id_;
  offset_t offset_;
  int32_t size_;
  // To facilitate compatibility processing, use the highest bit of submit_timestmap_ to indicate whether
  // batch_committed The remaining 63 bits are still used to represent timestamp information
  int64_t submit_timestamp_;
  int64_t accum_checksum_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexEntry);
};

class ObPaddingEntry {
public:
  ObPaddingEntry();
  ~ObPaddingEntry()
  {}
  static uint32_t get_padding_size(const int64_t offset, const uint32_t align_size = CLOG_DIO_ALIGN_SIZE);
  int64_t get_entry_size();
  int set_entry_size(int64_t entry_size);
  TO_STRING_KV(K(magic_), K(version_), K(entry_size_));
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  static const int16_t PADDING_MAGIC = 0x5044;  // "PD"
  static const int16_t PADDING_VERSION = 1;

private:
  int16_t magic_;
  int16_t version_;
  int64_t entry_size_;  // total len of ObPaddingEntry,
  DISALLOW_COPY_AND_ASSIGN(ObPaddingEntry);
};

class ObCompressedLogEntryHeader {
public:
  ObCompressedLogEntryHeader();
  ~ObCompressedLogEntryHeader();
  int shallow_copy(const ObCompressedLogEntryHeader& other);
  void set_magic(int16_t magic)
  {
    magic_ = magic;
  }
  void set_meta_len(int32_t original_len, int32_t compressed_len);
  int64_t get_orig_data_len() const
  {
    return (int64_t)orig_data_len_;
  }
  int64_t get_compressed_data_len() const
  {
    return (int64_t)compressed_data_len_;
  }
  int set_compressor_type(common::ObCompressorType compressor_type);
  int get_compressor_type(common::ObCompressorType& compressor_type) const;
  int64_t get_total_len() const
  {
    return (get_serialize_size() + compressed_data_len_);
  }
  TO_STRING_KV(K(magic_), K(orig_data_len_), K(compressed_data_len_));
  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  // attention!!!!: you should modify is_compress_log() in "ob_log_compress.h" and
  // parse_log_item_type() in "ob_raw_entry_iterator.h" when modifing following magic numbers
  // , and also ObCLogItemType in "ob_raw_entry_iterator.h".
  static const int16_t COMPRESS_INVALID_MAGIC = 0x4300;  // invalid
  static const int16_t COMPRESS_ZSTD_MAGIC = 0x4301;
  static const int16_t COMPRESS_LZ4_MAGIC = 0x4302;
  static const int16_t COMPRESS_ZSTD_1_3_8_MAGIC = 0x4303;

private:
  // Attention!!! Note that the serialization size of this structure should not exceed ObLogEntryHeader
  int16_t magic_;  // The first byte indicates whether to use compression, and the second byte indicates the compression
                   // algorithm
  int32_t orig_data_len_;
  int32_t compressed_data_len_;
  DISALLOW_COPY_AND_ASSIGN(ObCompressedLogEntryHeader);
};

class ObCompressedLogEntry {
public:
  ObCompressedLogEntry();
  ~ObCompressedLogEntry();
  void destroy();
  const ObCompressedLogEntryHeader& get_header() const
  {
    return header_;
  }
  const char* get_buf() const
  {
    return buf_;
  }
  TO_STRING_KV(N_HEADER, header_);
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  ObCompressedLogEntryHeader header_;
  const char* buf_;  // ObLogEntry's content after compressed
  DISALLOW_COPY_AND_ASSIGN(ObCompressedLogEntry);
};

}  // namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_ENTRY_
