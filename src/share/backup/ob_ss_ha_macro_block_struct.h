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

#ifndef OCEANBASE_SHARE_BACKUP_OB_SS_HA_MACRO_BLOCK_STRUCT_H_
#define OCEANBASE_SHARE_BACKUP_OB_SS_HA_MACRO_BLOCK_STRUCT_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_addr.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"

namespace oceanbase
{
namespace obrpc
{
struct ObSSHAMacroBlocksArg;
}

namespace share
{

// Macro block task type definition
struct ObSSHAMacroTaskType
{
  enum Type : uint8_t
  {
    BACKUP = 0,        // Backup task type
    RESTORE = 1,           // Restore task type
    BACKUP_CLEAN = 2,      // Backup clean task type
    MAX_TYPE,
  };
  ObSSHAMacroTaskType(): type_(Type::MAX_TYPE) {}
  ObSSHAMacroTaskType(const Type &type): type_(type) {}
  ~ObSSHAMacroTaskType() = default;
  bool is_valid() const;
  const char* get_str() const;
  bool is_backup() const { return Type::BACKUP == type_; }
  bool is_restore() const { return Type::RESTORE == type_; }
  bool is_backup_clean() const { return Type::BACKUP_CLEAN == type_; }
  void operator=(const ObSSHAMacroTaskType &task_type)
  {
    type_ = task_type.type_;
  }

  // Static method to convert string to enum
  static int from_string(const common::ObString &task_type_str, ObSSHAMacroTaskType &task_type);

  TO_STRING_KV(K_(type));
  Type type_;
};

// Payload type enumeration for macro_id_list field (4-bit, 0-15)
enum ObSSHAMacroIdListDataType : uint8_t
{
  MACRO_BLOCK_BATCH = 0,      // ObSSHAMacroBlockBatch serialized data (for BACKUP/RESTORE)
  MACRO_LIST_ADDR = 1,        // ObBackupBlockFileAddr serialized data (for BACKUP_CLEAN)
};

// Macro block information structure
struct ObSSHAMacroBlockInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObSSHAMacroBlockInfo();
  ~ObSSHAMacroBlockInfo() { reset(); }

  bool is_valid() const;
  void reset();
  int assign(const ObSSHAMacroBlockInfo &other);
  int hash(uint64_t &hash_val) const;
  bool operator ==(const ObSSHAMacroBlockInfo &other) const
  {
    return other.is_valid() && ls_id_ == other.ls_id_ && macro_block_id_ == other.macro_block_id_;
  }

  TO_STRING_KV(K_(ls_id), K_(macro_block_id), K_(occupy_size));

  share::ObLSID ls_id_;
  blocksstable::MacroBlockId macro_block_id_;
  int64_t occupy_size_;
};

// Macro block batch information
struct ObSSHAMacroBlockBatch final
{
  OB_UNIS_VERSION(1);
public:
  ObSSHAMacroBlockBatch();
  ~ObSSHAMacroBlockBatch() { reset(); }

  bool is_valid() const;
  void reset();
  int assign(const ObSSHAMacroBlockBatch &other);
  void reuse()
  {
    macro_blocks_.reuse();
    total_bytes_ = 0;
  }
  int64_t count() const { return macro_blocks_.count(); }
  bool is_empty() const { return macro_blocks_.empty(); }
  int get_ls_id(share::ObLSID &ls_id) const;
  int add_one(const ObSSHAMacroBlockInfo &macro_block_info);
  int64_t get_total_bytes() const;
  const ObSSHAMacroBlockInfo& at(int64_t idx) const { return macro_blocks_.at(idx); }
  const common::ObSArray<ObSSHAMacroBlockInfo>& get_macro_blocks() const { return macro_blocks_; }

  TO_STRING_KV(K_(macro_blocks), K_(total_bytes));
private:
  common::ObSArray<ObSSHAMacroBlockInfo> macro_blocks_;
  int64_t total_bytes_;  // Cached total bytes of all macro blocks
};

// Macro ID list: type + two payload members
//
// type_ indicates which member is active. Only the active member's data is meaningful.
// Serialization writes type_ followed by the active member's data.
struct ObSSHAMacroIdList final
{
  OB_UNIS_VERSION(1);
public:
  ObSSHAMacroIdList();
  ~ObSSHAMacroIdList() = default;
  ObSSHAMacroIdListDataType get_type() const { return type_; }
  bool is_macro_block_batch() const { return get_type() == MACRO_BLOCK_BATCH; }
  bool is_macro_list_addr() const { return get_type() == MACRO_LIST_ADDR; }
  void reset();
  int assign(const ObSSHAMacroIdList &other);

  int set_macro_block_batch(const ObSSHAMacroBlockBatch &batch);
  void set_macro_list_addr(const backup::ObBackupBlockFileAddr &addr);

  const ObSSHAMacroBlockBatch &get_macro_block_batch() const { return macro_block_batch_; }
  const backup::ObBackupBlockFileAddr &get_macro_list_addr() const { return macro_list_addr_; }
  TO_STRING_KV(K_(type), K_(macro_block_batch), K_(macro_list_addr));
private:
  ObSSHAMacroIdListDataType type_;
  ObSSHAMacroBlockBatch macro_block_batch_;
  backup::ObBackupBlockFileAddr macro_list_addr_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroIdList);
};


// Macro block task attribute structure - corresponds to new internal table schema
struct ObSSHAMacroBlockTaskAttr final
{
  static constexpr const int64_t TRACE_ID_STR_LENGTH = 1024;
  using TraceIdStr = common::ObFixedLengthString<TRACE_ID_STR_LENGTH>;
  static constexpr const int64_t COMMENT_STR_LENGTH = 1024;
  using CommentStr = common::ObFixedLengthString<COMMENT_STR_LENGTH>;
  ObSSHAMacroBlockTaskAttr();
  ~ObSSHAMacroBlockTaskAttr() { reset(); }
  bool operator==(const ObSSHAMacroBlockTaskAttr &other) const
  {
    return tenant_id_ == other.tenant_id_
        && ls_id_ == other.ls_id_
        && task_id_ == other.task_id_
        && retry_cnt_ == other.retry_cnt_;
  }

  bool operator!=(const ObSSHAMacroBlockTaskAttr &other) const
  {
    return !(*this == other);
  }

  uint64_t hash() const
  {
    uint64_t hash_value = 0;
    hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
    hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
    hash_value = common::murmurhash(&task_id_, sizeof(task_id_), hash_value);
    hash_value = common::murmurhash(&retry_cnt_, sizeof(retry_cnt_), hash_value);
    return hash_value;
  }

  bool is_valid() const;
  void reset();
  int assign(const ObSSHAMacroBlockTaskAttr &other);
  int init_from_rpc_arg(const obrpc::ObSSHAMacroBlocksArg &arg);
  int set_task_type(const common::ObString &task_type);
  int set_task_status(const common::ObString &task_status);
  const char* get_task_type_str() const;

  // Serialize macro_id_list to binary data
  // Caller should wrap result with ObHexEscapeSqlStr, which triggers sql_append_hex_escape_str()
  // to generate X'...' hex format. SQL parser will auto-convert it back to binary when storing.
  int serialize_macro_id_list(
      common::ObIAllocator &allocator,
      common::ObString &binary_data) const;

  // Deserialize macro_id_list from binary data
  // SQL parser already converted X'...' to binary, just deserialize directly
  int deserialize_macro_id_list(
      const common::ObString &binary_data);

  TO_STRING_KV(K_(tenant_id), K_(parent_task_id), K_(ls_id), K_(task_id), K_(task_type),
      K_(macro_block_cnt), K_(status),
      K_(start_time), K_(end_time), K_(svr_addr), K_(trace_id),
      K_(result), K_(comment), K_(retry_cnt), K_(total_bytes), K_(finish_bytes),
      K_(macro_id_list));

  uint64_t tenant_id_;              // Tenant ID (primary key)
  int64_t parent_task_id_;          // Parent task ID (primary key)
  share::ObLSID ls_id_;             // LS ID (primary key)
  int64_t task_id_;                 // Task ID (primary key)
  ObSSHAMacroTaskType task_type_;
  ObSSHAMacroIdList macro_id_list_; // Macro ID list (union of macro_block_batch and macro_list_addr)
  int64_t macro_block_cnt_;
  ObBackupTaskStatus status_;       // Task status
  int64_t start_time_;              // Start time
  int64_t end_time_;                // End time
  common::ObAddr svr_addr_;         // Execution server address
  TraceIdStr trace_id_;             // Trace ID
  int64_t result_;                  // Execution result
  CommentStr comment_;              // Comment information
  int64_t retry_cnt_;               // Current retry count
  int64_t total_bytes_;             // Total bytes to copy
  int64_t finish_bytes_;            // Finished bytes
};

// Macro block task manager state information (for state machine state persistence)
struct ObSSHAMacroTaskMgrState final
{
  static constexpr const int64_t COMMENT_STR_LENGTH = 1024;
  using CommentStr = common::ObFixedLengthString<COMMENT_STR_LENGTH>;

  ObSSHAMacroTaskMgrState();
  ~ObSSHAMacroTaskMgrState() { reset(); }

  void reset();
  int assign(const ObSSHAMacroTaskMgrState &other);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(parent_task_id), K_(task_status), K_(task_type),
      K_(total_task_count), K_(finish_task_count),
      K_(total_macro_block_count), K_(finish_macro_block_count),
      K_(total_bytes), K_(finish_bytes),
      K_(result), K_(comment));

  uint64_t tenant_id_;
  int64_t parent_task_id_;
  int64_t task_status_;    // Corresponds to ObSSHAMacroTaskMgrStatus enum value
  ObSSHAMacroTaskType task_type_;  // Task type enum
  int64_t total_task_count_;
  int64_t finish_task_count_;
  int64_t total_macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t total_bytes_;    // Total bytes to copy
  int64_t finish_bytes_;   // Finished bytes
  int64_t result_;         // Execution result
  CommentStr comment_;     // Comment information
};

// Macro block task scheduling parameters
struct ObMacroBlockTaskScheduleParam final
{
  ObMacroBlockTaskScheduleParam();
  ~ObMacroBlockTaskScheduleParam() {}

  bool is_valid() const;
  int assign(const ObMacroBlockTaskScheduleParam &other);

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(job_id), K_(backup_dest),
      K_(batch_size), K_(max_parallel_degree));

  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t job_id_;
  share::ObBackupDest backup_dest_;
  int64_t batch_size_;              // Number of macro blocks per batch, default 1024
  int64_t max_parallel_degree_;     // Maximum parallelism, default 8
  share::SCN snapshot_scn_;         // Backup snapshot point
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_BACKUP_OB_SS_HA_MACRO_BLOCK_STRUCT_H_
