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

#ifndef OCEANBASE_SLOG_OB_STORAGE_LOG_STRUCT_H_
#define OCEANBASE_SLOG_OB_STORAGE_LOG_STRUCT_H_

#include "common/log/ob_log_entry.h"
#include "common/log/ob_log_cursor.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace blocksstable {
enum ObRedoLogMainType {
  OB_REDO_LOG_SYS = 0,
  OB_REDO_LOG_PARTITION = 1,
  OB_REDO_LOG_MACROBLOCK = 2,
  OB_REDO_LOG_TABLE_MGR = 3,      // added in 2.0
  OB_REDO_LOG_TENANT_CONFIG = 4,  // added in 2.0
  OB_REDO_LOG_TENANT_FILE = 5,    // added in 3.0
  OB_REDO_LOG_MAX
};

enum ObRedoLogSysType { OB_REDO_LOG_BEGIN = 0, OB_REDO_LOG_COMMIT = 1 };

class ObIBaseStorageLogEntry {
public:
  ObIBaseStorageLogEntry()
  {}
  virtual ~ObIBaseStorageLogEntry()
  {}
  virtual bool is_valid() const = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;

  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObBaseStorageLogHeader {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  static const int64_t BASE_STORAGE_LOG_HEADER_VERSION = 1;
  int64_t trans_id_;
  int64_t log_seq_;
  int64_t subcmd_;
  int64_t log_len_;
  uint64_t tenant_id_;
  int64_t data_file_id_;
  ObBaseStorageLogHeader();
  virtual ~ObBaseStorageLogHeader();
  TO_STRING_KV(K_(trans_id), K_(log_seq), K_(subcmd), K_(log_len), K_(tenant_id), K_(data_file_id));
  OB_UNIS_VERSION_V(BASE_STORAGE_LOG_HEADER_VERSION);
};

struct ObStorageLogAttribute {
public:
  ObStorageLogAttribute() : tenant_id_(common::OB_INVALID_TENANT_ID), data_file_id_(common::OB_INVALID_DATA_FILE_ID)
  {}
  ObStorageLogAttribute(const uint64_t tenant_id, const int64_t data_file_id)
      : tenant_id_(tenant_id), data_file_id_(data_file_id)
  {}
  ~ObStorageLogAttribute() = default;
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && common::OB_INVALID_ID != tenant_id_ &&
           common::OB_INVALID_DATA_FILE_ID != data_file_id_;
  }
  uint64_t tenant_id_;
  int64_t data_file_id_;
  TO_STRING_KV(K_(tenant_id), K_(data_file_id));
};

class ObBaseStorageLogBuffer : public ObBufferHolder {
public:
  ObBaseStorageLogBuffer();
  virtual ~ObBaseStorageLogBuffer();
  int assign(char* buf, const int64_t buf_len);
  int append_log(const int64_t trans_id, const int64_t log_seq, const int64_t subcmd,
      const ObStorageLogAttribute& log_attr, ObIBaseStorageLogEntry& data);
  int read_log(ObBaseStorageLogHeader& header, char*& log_data);
  void reset();
  void reuse();
  bool is_empty() const;

public:
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(KP_(data), K_(capacity), K_(pos));

private:
  int append_log_head(const int64_t trans_id, const int64_t log_seq, const int64_t subcmd, const int64_t log_len,
      const ObStorageLogAttribute& log_attr);
};

struct ObStorageLogValidRecordEntry : public ObIBaseStorageLogEntry {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  struct Extent {
    int64_t start_;
    int64_t end_;
  };
  static const int64_t VALID_RECORD_VERSION = 1;
  static const int64_t MAX_EXTENT_COUNT = common::OB_MAX_INDEX_PER_TABLE;
  static const int64_t MAX_SAVEPOINT_COUNT = common::OB_MAX_INDEX_PER_TABLE;
  // Serialize variables
  Extent extent_list_[MAX_EXTENT_COUNT];
  int64_t extent_count_;
  int64_t rollback_count_;

  // Not serialize variables
  int64_t savepoint_list_[MAX_SAVEPOINT_COUNT];
  int64_t savepoint_count_;
  ObStorageLogValidRecordEntry();
  ~ObStorageLogValidRecordEntry();
  bool is_valid() const;
  void reset();
  int add_extent(int64_t start, int64_t end);
  int find_extent(int64_t log_seq, int64_t& index);
  int find_savepoint(int64_t savepoint, int64_t& index);
  int add_log_entry(int64_t log_seq);
  int add_savepoint(int64_t log_seq);
  int rollback(const int64_t savepoint);
  TO_STRING_KV(K_(extent_count), K_(rollback_count), K_(savepoint_count));
  NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObStorageLogActiveTrans {
  enum common::LogCommand cmd_;
  int64_t log_count_;
  common::ObLogCursor start_cursor_;
  ObStorageLogValidRecordEntry valid_record_;
  ObBaseStorageLogBuffer log_buffer_;

  ObStorageLogActiveTrans();
  ~ObStorageLogActiveTrans();
  bool is_valid() const;

  int append_log(const int64_t trans_id, const int64_t subcmd, const ObStorageLogAttribute& log_attr,
      ObIBaseStorageLogEntry& data);
  int assign(char* buf, const int64_t buf_size);
  void reuse();
  TO_STRING_KV(K_(log_count), K_(start_cursor), K_(valid_record), K_(log_buffer));
};
}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_SLOG_OB_STORAGE_LOG_STRUCT_H_
