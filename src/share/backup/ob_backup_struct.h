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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_STRUCT_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_STRUCT_H_

#include "lib/container/ob_array.h"
#include "lib/restore/ob_storage.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/utility.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/worker.h"
#include "common/ob_role.h"
#include "common/ob_timeout_ctx.h"
#include "common/ob_region.h"
#include "share/ob_define.h"
#include "share/ob_force_print_log.h"
#include "share/ob_encryption_util.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
namespace share {

enum ObBackupCompatibleVersion {
  OB_BACKUP_COMPATIBLE_VERSION_V1 = 1,  // since 2.2.60
  OB_BACKUP_COMPATIBLE_VERSION_V2 = 2,  // since 3.1
  OB_BACKUP_COMPATIBLE_VERSION_MAX,
};

const int64_t OB_ALL_BACKUP_SET_ID = 0;
const int64_t OB_BACKUP_DEFAULT_PG_NUM = 10000;
const int64_t OB_MAX_BACKUP_DEST_LENGTH = common::OB_MAX_URI_LENGTH;
const int64_t OB_MAX_BACKUP_PATH_LENGTH = common::MAX_PATH_SIZE;
const int64_t OB_MAX_BACKUP_ID_LENGTH = 128;
const int64_t OB_MAX_BACKUP_KEY_LENGTH = 128;
const int64_t OB_MAX_BACKUP_HOSTNAME_LENGTH = 128;
const int64_t OB_MAX_BACKUP_STORAGE_INFO_LENGTH = 256;
const int64_t OB_MAX_KEPT_LOG_ARCHIVE_BACKUP_ROUND = 10 * 10000;  // 10w
const int64_t OB_MAX_VALIDATE_LOG_INFO_LENGTH = 256;
const int64_t OB_START_BACKUP_SET_ID = 0;
const int64_t OB_BACKUP_COMPATIBLE_VERSION = OB_BACKUP_COMPATIBLE_VERSION_V2;
const int64_t OB_DEFAULT_BACKUP_LOG_INTERVAL = 60 * 1000 * 1000;  // 60s
const int64_t OB_MAX_LOG_ARCHIVE_THREAD_NUM = 20;
const int64_t OB_GROUP_BACKUP_CONCURRENCY = 1;
const int64_t OB_GROUP_VALIDATE_CONCURRENCY = 1;
const int64_t OB_MAX_INCREMENTAL_BACKUP_NUM = 64;
const int64_t OB_MAX_LOG_ARCHIVE_CONCURRENCY = 128;

// add by physical backup and restore
const char* const OB_STR_INCARNATION = "incarnation";
const char* const OB_STRING_BACKUP_DATA = "data";
const char* const OB_STRING_BACKUP_CLOG = "clog";
const char* const OB_STRING_BACKUP_INDEX = "index";
const char* const OB_STRING_BACKUP_CLOG_INFO = "clog_info";
const char* const OB_STRING_BACKUP_ARCHIVE_KEY = "archive_key";
const char* const OB_STRING_BACKUP_FULL_BACKUP_SET = "backup_set";
const char* const OB_STRING_BACKUP_INC_BACKUP_SET = "backup";
const char* const OB_STRING_BACKUP_META_INDEX = "meta_index_file";
const char* const OB_STRING_BACKUP_META_FILE = "meta_file";
const char* const OB_STRING_BACKUP_SSTABLE_MACRO_INDEX = "sstable_macro_index";
const char* const OB_STRING_BACKUP_MACRO_BLOCK_INDEX = "macro_block_index";
const char* const OB_STRING_BACKUP_MACRO_BLOCK_FILE = "macro_block";
const char* const OB_STRING_CLUSTER_DATA_BACKUP_INFO = "cluster_data_backup_info";
const char* const OB_STRING_TENANT_DATA_BACKUP_INFO = "tenant_data_backup_info";
const char* const OB_STRING_BACKUP_SET_INFO = "backup_set_info";
const char* const OB_STRING_SYS_PG_LIST = "sys_pg_list";
const char* const OB_STRING_NORMAL_PG_LIST = "normal_pg_list";
const char* const OB_STRING_TENANT_INFO = "tenant_info";
const char* const OB_STRING_TENANT_NAME_INFO = "tenant_name_info";
const char* const OB_STRING_TENANT_LOCALITY_INFO = "tenant_locality_info";
const char* const OB_STRING_TENANT_DIAGNOSE_INFO = "tenant_diagnose_info";
const char* const OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION = "tenant_name_backup_schema_version";
const char* const OB_STR_BACKUP_SCHEDULER_LEADER = "backup_scheduler_leader";
const char* const OB_STR_CLUSTER_CLOG_BACKUP_INFO = "cluster_clog_backup_info";
const char* const OB_STR_TENANT_CLOG_BACKUP_INFO = "tenant_clog_backup_info";

const char* const OB_STRING_MJAOR_DATA = "major_data";
const char* const OB_STRING_MINOR_DATA = "minor_data";

const char* const OB_STR_LOG_ARCHIVE_STATUS = "log_archive_status";
const char* const OB_STR_BACKUP_DEST = "backup_dest";
const char* const OB_STR_ENABLE_LOG_ARCHIVE = "enable_log_archive";
const char* const OB_STR_TENANT_ID = "tenant_id";
const char* const OB_STR_LOG_ARCHIVE_ROUND = "log_archive_round";
const char* const OB_STR_MIN_FIRST_TIME = "min_first_time";
const char* const OB_STR_MAX_NEXT_TIME = "max_next_time";
const char* const OB_STR_STATUS = "status";
const char* const OB_STR_BACKUP_ENCRYPTION_MODE = "encryption_mode";
const char* const OB_STR_BACKUP_PASSWD = "passwd";
const char* const OB_STR_COPY_ID = "copy_id";
const char* const OB_STR_IS_MARK_DELETED = "is_mark_deleted";
const char* const OB_STR_IS_MOUNT_FILE_CREATED = "is_mount_file_created";
const char* const OB_STR_COMPATIBLE = "compatible";

const char* const OB_BACKUP_ENCRYPTION_MODE_SESSION_STR = "__ob_backup_encryption_mode__";
const char* const OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR = "__ob_backup_encryption_passwd__";
const char* const OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR = "__ob_backup_decryption_passwd_array__";

enum ObBackupFileType {
  BACKUP_META = 0,
  BACKUP_MACRO_DATA = 1,
  BACKUP_META_INDEX = 2,
  BACKUP_MACRO_DATA_INDEX = 3,
  BACKUP_SSTABLE_MACRO_INDEX = 4,      // not used
  BACKUP_LOG_ARCHIVE_BACKUP_INFO = 5,  //
  BACKUP_INFO = 6,
  BACKUP_SET_INFO = 7,
  BACKUP_PG_LIST = 8,
  BACKUP_TENANT_INFO = 9,
  BACKUP_FILE_END_MARK = 10,
  BACKUP_FILE_SWITCH_MARK = 11,
  BACKUP_TENANT_LOCALITY_INFO = 12,
  BACKUP_TENANT_DIAGNOSE_INFO = 13,
  BACKUP_TENANT_NAME_INFO = 14,
  // type <=255 is write header struct to disk directly
  // type > 255 is use serialization to disk
  BACKUP_MAX_DIRECT_WRITE_TYPE = 255,
  BACKUP_ARCHIVE_BLOCK_META = 0x4142,  // 16706
  BACKUP_ARCHIVE_INDEX_FILE = 0x4149,  // 16713 AI means ARCHIVE INDEX
  BACKUP_TYPE_MAX
};

enum ObBackupMetaType {
  PARTITION_GROUP_META = 0,
  PARTITION_META = 1,
  SSTABLE_METAS = 2,
  TABLE_KEYS = 3,
  PARTITION_GROUP_META_INFO = 4,
  META_TYPE_MAX
};

struct ObBackupCommonHeader {
  static const uint8_t COMMON_HEADER_VERSION = 1;
  static const uint8_t MACRO_DATA_HEADER_VERSION = 1;
  ObBackupCommonHeader();
  void reset();
  void set_header_checksum();
  int set_checksum(const char* buf, const int64_t len);
  int check_header_checksum() const;
  int16_t calc_header_checksum() const;
  int check_valid() const;
  bool is_compresssed_data() const;
  int check_data_checksum(const char* buf, const int64_t len) const;
  int assign(const ObBackupCommonHeader& header);
  TO_STRING_KV(K_(header_version), K_(compressor_type), K_(data_type), K_(data_version), K_(header_length),
      K_(header_checksum), K_(data_length), K_(data_zlength), K_(data_checksum), K_(align_length));
  uint16_t data_type_;
  uint16_t header_version_;  // header version
  uint16_t data_version_;
  uint16_t compressor_type_;  // data compression type
  int16_t header_length_;     // = sizeof(ObBackupCommonHeader)
  int16_t header_checksum_;
  int64_t data_length_;    // length of data before compression
  int64_t data_zlength_;   // length of data after compression
  int64_t data_checksum_;  // checksum of data part
  int64_t align_length_;
};

struct ObBackupMetaHeader {
  static const uint8_t META_HEADER_VERSION = 1;
  ObBackupMetaHeader();
  void reset();
  void set_header_checksum();
  int set_checksum(const char* buf, const int64_t len);
  int check_header_checksum() const;
  int check_data_checksum(const char* buf, const int64_t len) const;
  int16_t calc_header_checksum() const;
  int check_valid() const;
  TO_STRING_KV(K_(header_version), K_(meta_type), K_(header_checksum), K_(data_length), K_(data_checksum));
  uint16_t header_version_;
  uint16_t meta_type_;  // ObBackupMetaType;
  int16_t header_checksum_;
  int16_t reserved_[3];
  int32_t data_length_;
  int64_t data_checksum_;
};

struct ObBackupMetaIndex {
  static const uint8_t META_INDEX_VERSION = 1;
  OB_UNIS_VERSION(META_INDEX_VERSION);

public:
  ObBackupMetaIndex();
  void reset();
  int check_valid() const;
  TO_STRING_KV(K_(meta_type), K_(table_id), K_(partition_id), K_(offset), K_(data_length), K_(task_id));
  uint16_t meta_type_;  // ObBackupMetaType
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t offset_;
  int64_t data_length_;  // =sizeof(ObBackupMetaHeader) + ObBackupMetaHeader.data_length_
  int64_t task_id_;
};

struct ObBackupMacroIndex {
  static const int64_t MACRO_INDEX_VERSION = 1;
  OB_UNIS_VERSION(MACRO_INDEX_VERSION);

public:
  ObBackupMacroIndex();
  void reset();
  int check_valid() const;
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(index_table_id), K_(sstable_macro_index), K_(data_version),
      K_(data_seq), K_(backup_set_id), K_(sub_task_id), K_(offset), K_(data_length));

  uint64_t table_id_;
  int64_t partition_id_;
  uint64_t index_table_id_;
  int64_t sstable_macro_index_;
  int64_t data_version_;
  int64_t data_seq_;
  int64_t backup_set_id_;
  int64_t sub_task_id_;
  int64_t offset_;
  int64_t data_length_;  //=ObBackupDataHeader(header_length_+macro_meta_length_ + macro_data_length_)
};

struct ObBackupSStableMacroIndex {
  static const uint8_t SSTABLE_MACRO_INDEX_VERSION = 1;
  ObBackupSStableMacroIndex();
  void reset();
  void set_header_checksum();
  int check_header_checksum() const;
  int16_t calc_header_checksum() const;
  int is_valid() const;
  TO_STRING_KV(K_(header_version), K_(data_type), K_(header_length), K_(header_checksum), K_(reserved), K_(table_id),
      K_(partition_id), K_(index_table_id), K_(offset), K_(count));

  uint8_t header_version_;
  uint8_t data_type_;  // ObBackupFileType;
  int16_t header_length_;
  int16_t header_checksum_;
  int16_t reserved_;  // for future extension
  int64_t table_id_;
  int64_t partition_id_;
  int64_t index_table_id_;
  int32_t offset_;
  int32_t count_;
};

struct ObMetaIndexKey {
  ObMetaIndexKey();
  ObMetaIndexKey(const int64_t table_id, const int64_t partition_id, const uint8_t meta_type);
  void reset();
  bool operator==(const ObMetaIndexKey& other) const;
  bool operator!=(const ObMetaIndexKey& other) const;
  uint64_t hash() const;
  bool is_valid() const;

  TO_STRING_KV(K_(table_id), K_(partition_id), K_(meta_type));
  int64_t table_id_;
  int64_t partition_id_;
  uint16_t meta_type_;  // ObBackupMetaType;
};

struct ObLogArchiveStatus final {
  enum STATUS { INVALID = 0, STOP = 1, BEGINNING = 2, DOING = 3, STOPPING = 4, INTERRUPTED = 5, MIXED = 6, MAX };

  static const char* get_str(const STATUS& status);
  static STATUS get_status(const char* status_str);
  static OB_INLINE bool is_valid(const STATUS& status)
  {
    return status >= 0 && status < MAX;
  }
};

struct ObGetTenantLogArchiveStatusArg final {
  OB_UNIS_VERSION(1);

public:
  uint64_t incarnation_;
  uint64_t round_;
  bool is_valid() const
  {
    return true;
  }
  ObGetTenantLogArchiveStatusArg() : incarnation_(0), round_(0)
  {}
  TO_STRING_KV(K_(incarnation), K(round_));
};

struct ObTenantLogArchiveStatus final {
  enum COMPATIBLE : int64_t { NONE = 0, COMPATIBLE_VERSION_1 = 1, MAX };

  OB_UNIS_VERSION(1);

public:
  ObTenantLogArchiveStatus();
  void reset();
  bool is_valid() const;
  bool is_compatible_valid(COMPATIBLE compatible);
  int update(const ObTenantLogArchiveStatus& new_status);

  uint64_t tenant_id_;
  int64_t start_ts_;
  int64_t checkpoint_ts_;
  int64_t incarnation_;
  int64_t round_;
  ObLogArchiveStatus::STATUS status_;
  bool is_mark_deleted_;
  bool is_mount_file_created_;  // used to check if backup dest is mount properly
  COMPATIBLE compatible_;
  TO_STRING_KV(K_(tenant_id), K_(start_ts), K_(checkpoint_ts), K_(status), K_(incarnation), K_(round), "status_str",
      ObLogArchiveStatus::get_str(status_), K_(is_mark_deleted), K_(is_mount_file_created), K_(compatible));

private:
  int update_stop_(const ObTenantLogArchiveStatus& new_status);
  int update_beginning_(const ObTenantLogArchiveStatus& new_status);
  int update_doing_(const ObTenantLogArchiveStatus& new_status);
  int update_stopping_(const ObTenantLogArchiveStatus& new_status);
  int update_interrupted_(const ObTenantLogArchiveStatus& new_status);
};

struct ObTenantLogArchiveStatusWrapper final {
  OB_UNIS_VERSION(1);

public:
  ObTenantLogArchiveStatusWrapper();
  int32_t result_code_;
  common::ObSArray<ObTenantLogArchiveStatus> status_array_;
  TO_STRING_KV(K_(result_code), K_(status_array));
};

struct ObLogArchiveBackupInfo final {
  ObLogArchiveBackupInfo();

  ObTenantLogArchiveStatus status_;
  char backup_dest_[OB_MAX_BACKUP_DEST_LENGTH];

  void reset();
  bool is_valid() const;
  bool is_same(const ObLogArchiveBackupInfo& other) const;
  TO_STRING_KV(K_(status), K_(backup_dest));
};

struct ObBackupDest final {
  ObBackupDest();
  int set(const char* backup_dest);
  int set(const char* root_path, const char* storage_info);
  const char* get_type_str() const;
  void reset();
  bool is_valid() const;
  int get_backup_dest_str(char* buf, const int64_t buf_size) const;
  bool operator==(const ObBackupDest& backup_dest) const;
  uint64_t hash() const;
  // TODO(): delete storage_info from to_string fun later
  TO_STRING_KV(K_(device_type), K_(root_path), K_(storage_info), "type", get_type_str());

  common::ObStorageType device_type_;
  char root_path_[OB_MAX_BACKUP_PATH_LENGTH];
  char storage_info_[OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
};

struct ObBackupInfoStatus final {
  enum BackupStatus {
    STOP = 0,
    PREPARE = 1,
    SCHEDULE = 2,
    DOING = 3,
    CANCEL = 4,
    CLEANUP = 5,
    MAX,
  };
  ObBackupInfoStatus() : status_(MAX)
  {}
  virtual ~ObBackupInfoStatus() = default;
  void reset()
  {
    status_ = MAX;
  }
  bool is_valid() const
  {
    return status_ >= 0 && status_ < MAX;
  }
  static const char* get_status_str(const BackupStatus& status);
  const char* get_info_backup_status_str() const;
  int set_info_backup_status(const char* buf);
  bool is_stop_status() const
  {
    return STOP == status_;
  }
  bool is_prepare_status() const
  {
    return PREPARE == status_;
  }
  bool is_scheduler_status() const
  {
    return SCHEDULE == status_;
  }
  bool is_doing_status() const
  {
    return DOING == status_;
  }
  bool is_cleanup_status() const
  {
    return CLEANUP == status_;
  }
  bool is_cancel_status() const
  {
    return CANCEL == status_;
  }
  void set_backup_status_scheduler()
  {
    status_ = SCHEDULE;
  }
  void set_backup_status_doing()
  {
    status_ = DOING;
  }
  void set_backup_status_cleanup()
  {
    status_ = CLEANUP;
  }
  void set_backup_status_stop()
  {
    status_ = STOP;
  }
  void set_backup_status_cancel()
  {
    status_ = CANCEL;
  }
  TO_STRING_KV(K_(status), "status_str", get_status_str(status_));
  BackupStatus status_;
};

struct ObBackupType final {
  enum BackupType {
    EMPTY = 0,
    FULL_BACKUP = 1,
    INCREMENTAL_BACKUP = 2,
    MAX,
  };

  ObBackupType() : type_(EMPTY)
  {}
  virtual ~ObBackupType() = default;
  void reset()
  {
    type_ = EMPTY;
  }
  bool is_valid() const
  {
    return type_ >= FULL_BACKUP && type_ < MAX;
  }
  const char* get_backup_type_str() const;
  int set_backup_type(const char* buf);
  static OB_INLINE bool is_full_backup(const BackupType& type)
  {
    return FULL_BACKUP == type;
  }
  bool is_full_backup() const
  {
    return ObBackupType::is_full_backup(type_);
  }

  TO_STRING_KV(K_(type));
  BackupType type_;
};

// TODO() backup use lib backupdevicetype, fix it later
struct ObBackupDeviceType final {
  enum BackupDeviceType {
    NFS = 0,
    OSS = 1,
    OFS = 2,
    COS = 3,
    MAX,
  };
  ObBackupDeviceType() : type_(MAX)
  {}
  virtual ~ObBackupDeviceType() = default;
  void reset()
  {
    type_ = MAX;
  }
  const char* get_backup_device_str() const;
  int set_backup_device_type(const char* buf);
  TO_STRING_KV(K_(type));

  BackupDeviceType type_;
};

struct ObBaseBackupInfoStruct {
public:
  typedef common::ObFixedLengthString<common::OB_MAX_URI_LENGTH> BackupDest;
  ObBaseBackupInfoStruct();
  virtual ~ObBaseBackupInfoStruct() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObBaseBackupInfoStruct& backup_info_struct);
  bool has_cleaned() const;
  ObBaseBackupInfoStruct& operator=(const ObBaseBackupInfoStruct& info);
  int check_backup_info_match(const ObBaseBackupInfoStruct& info) const;

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(incarnation), K_(backup_dest), K_(backup_snapshot_version),
      K_(backup_schema_version), K_(backup_data_version), K_(detected_backup_region), K_(backup_type),
      K_(backup_status), K_(backup_task_id), K_(encryption_mode));

  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t incarnation_;
  BackupDest backup_dest_;
  int64_t backup_snapshot_version_;
  int64_t backup_schema_version_;
  int64_t backup_data_version_;
  common::ObRegion detected_backup_region_;
  ObBackupType backup_type_;
  ObBackupInfoStatus backup_status_;
  int64_t backup_task_id_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
};

// TODO() add compatible and cluster_version
// to TenantBackupTaskItem
struct ObTenantBackupTaskItem {
public:
  enum BackupStatus {
    GENERATE = 0,
    DOING = 1,
    FINISH = 2,
    CANCEL = 3,
    MAX,
  };

public:
  ObTenantBackupTaskItem();
  virtual ~ObTenantBackupTaskItem() = default;
  void reset();
  bool is_key_valid() const;
  bool is_valid() const;
  bool is_same_task(const ObTenantBackupTaskItem& other) const;
  bool is_result_succeed() const;
  const char* get_backup_task_status_str() const;
  int set_backup_task_status(const char* buf);

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(incarnation), K_(snapshot_version), K_(prev_full_backup_set_id),
      K_(prev_inc_backup_set_id), K_(prev_backup_data_version), K_(pg_count), K_(macro_block_count),
      K_(finish_pg_count), K_(finish_macro_block_count), K_(input_bytes), K_(output_bytes), K_(start_time),
      K_(end_time), K_(compatible), K_(cluster_version), K_(backup_type), K_(status), K_(device_type), K_(result),
      K_(cluster_id), K_(backup_dest), K_(backup_data_version), K_(backup_schema_version), K_(partition_count),
      K_(finish_partition_count), K_(encryption_mode), K_(passwd), K_(is_mark_deleted));

  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t incarnation_;
  int64_t snapshot_version_;
  int64_t prev_full_backup_set_id_;
  int64_t prev_inc_backup_set_id_;
  int64_t prev_backup_data_version_;
  int64_t pg_count_;
  int64_t macro_block_count_;
  int64_t finish_pg_count_;
  int64_t finish_macro_block_count_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t compatible_;
  uint64_t cluster_version_;
  ObBackupType backup_type_;
  BackupStatus status_;
  common::ObStorageType device_type_;
  int32_t result_;
  int64_t cluster_id_;
  ObBackupDest backup_dest_;
  int64_t backup_data_version_;
  int64_t backup_schema_version_;
  int64_t partition_count_;
  int64_t finish_partition_count_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
  bool is_mark_deleted_;
};

typedef ObTenantBackupTaskItem ObTenantBackupTaskInfo;

// pg backup task item
struct ObPGBackupTaskItem {
public:
  enum BackupStatus {
    PENDING = 0,
    DOING = 1,
    FINISH = 2,
    CANCEL = 3,
    MAX,
  };

public:
  ObPGBackupTaskItem();
  virtual ~ObPGBackupTaskItem() = default;
  void reset();
  bool is_key_valid() const;
  bool is_valid() const;
  bool is_same_task(const ObPGBackupTaskItem& other) const;
  static const char* get_status_str(const BackupStatus& status);
  const char* get_backup_task_status_str() const;
  int set_backup_task_status(const char* buf);
  int set_trace_id(const char* buf, const int64_t buf_size);
  int get_trace_id(char* buf, const int64_t buf_size) const;
  static int get_trace_id(const ObTaskId& trace_id, const int64_t buf_size, char* buf);

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(partition_id), K_(incarnation), K_(backup_set_id), K_(backup_type),
      K_(snapshot_version), K_(partition_count), K_(macro_block_count), K_(finish_partition_count),
      K_(finish_macro_block_count), K_(input_bytes), K_(output_bytes), K_(start_time), K_(end_time), K_(retry_count),
      K_(role), K_(replica_type), K_(server), K_(status), K_(result), K_(task_id), K_(trace_id));
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  ObBackupType backup_type_;
  int64_t snapshot_version_;
  int64_t partition_count_;
  int64_t macro_block_count_;
  int64_t finish_partition_count_;
  int64_t finish_macro_block_count_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t retry_count_;
  common::ObRole role_;
  common::ObReplicaType replica_type_;
  common::ObAddr server_;
  BackupStatus status_;
  int32_t result_;
  int64_t task_id_;
  share::ObTaskId trace_id_;
};

typedef ObPGBackupTaskItem ObPGBackupTaskInfo;

struct ObBackupValidateTenant {
  ObBackupValidateTenant();
  virtual ~ObBackupValidateTenant();
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(is_dropped));

  uint64_t tenant_id_;
  bool is_dropped_;
};

struct ObBackupValidateTaskItem {
public:
  enum ValidateStatus {
    SCHEDULE = 0,
    DOING = 1,
    FINISHED = 2,
    CANCEL = 3,
    MAX,
  };

public:
  ObBackupValidateTaskItem();
  virtual ~ObBackupValidateTaskItem();
  void reset();
  bool is_valid() const;
  bool is_same_task(const ObBackupValidateTaskItem& other);
  static const char* get_status_str(const ValidateStatus& status);
  int set_status(const char* status_str);

  typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> TenantName;
  TO_STRING_KV(
      K_(job_id), K_(tenant_id), K_(tenant_name), K_(incarnation), K_(backup_set_id), K_(progress_percent), K_(status));

  int64_t job_id_;
  uint64_t tenant_id_;
  TenantName tenant_name_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t progress_percent_;
  ValidateStatus status_;
};

typedef ObBackupValidateTaskItem ObBackupValidateTaskInfo;

struct ObTenantValidateTaskItem {
public:
  enum ValidateStatus {
    SCHEDULE = 0,
    DOING = 1,
    FINISHED = 2,
    CANCEL = 3,
    MAX,
  };

public:
  ObTenantValidateTaskItem();
  virtual ~ObTenantValidateTaskItem();
  void reset();
  bool is_valid() const;
  bool is_same_task(const ObTenantValidateTaskItem& other);
  static const char* get_status_str(const ValidateStatus& status);
  int set_status(const char* status_str);

  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(incarnation), K_(backup_set_id), K_(status), K_(start_time), K_(end_time),
      K_(total_pg_count), K_(finish_pg_count), K_(total_partition_count), K_(finish_partition_count),
      K_(total_macro_block_count), K_(finish_macro_block_count), K_(result));

  typedef common::ObFixedLengthString<common::MAX_TABLE_COMMENT_LENGTH> Comment;

  int64_t job_id_;
  int64_t task_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  ValidateStatus status_;
  char backup_dest_[OB_MAX_BACKUP_DEST_LENGTH];
  int64_t start_time_;
  int64_t end_time_;
  int64_t total_pg_count_;
  int64_t finish_pg_count_;
  int64_t total_partition_count_;
  int64_t finish_partition_count_;
  int64_t total_macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t log_size_;
  int32_t result_;
  Comment comment_;
};

typedef ObTenantValidateTaskItem ObTenantValidateTaskInfo;

struct ObPGValidateTaskItem {
public:
  enum ValidateStatus {
    PENDING = 0,
    DOING = 1,
    FINISHED = 2,
    MAX,
  };

public:
  ObPGValidateTaskItem();
  virtual ~ObPGValidateTaskItem();
  void reset();
  bool is_valid() const;
  bool is_same_task(const ObPGValidateTaskItem& other);
  static const char* get_status_str(const ValidateStatus& status);
  int set_status(const char* status_str);

  TO_STRING_KV(K_(job_id), K_(task_id), K_(tenant_id), K_(table_id), K_(partition_id), K_(partition_id),
      K_(incarnation), K_(backup_set_id), K_(archive_round), K_(total_partition_count), K_(finish_partition_count),
      K_(total_macro_block_count), K_(finish_macro_block_count), K_(server), K_(log_size), K_(result), K_(trace_id),
      K_(status), K_(comment));

  typedef common::ObFixedLengthString<common::MAX_TABLE_COMMENT_LENGTH> Comment;

  int64_t job_id_;
  int64_t task_id_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t archive_round_;
  int64_t total_partition_count_;
  int64_t finish_partition_count_;
  int64_t total_macro_block_count_;
  int64_t finish_macro_block_count_;
  common::ObAddr server_;
  char log_info_[OB_MAX_VALIDATE_LOG_INFO_LENGTH];
  int64_t log_size_;
  int32_t result_;
  share::ObTaskId trace_id_;
  ValidateStatus status_;
  Comment comment_;
};

typedef ObPGValidateTaskItem ObPGValidateTaskInfo;

struct ObPGValidateTaskRowKey {
public:
  ObPGValidateTaskRowKey();
  virtual ~ObPGValidateTaskRowKey();
  int set(const ObPGValidateTaskInfo* pg_task);

  TO_STRING_KV(
      K_(tenant_id), K_(job_id), K_(task_id), K_(incarnation), K_(backup_set_id), K_(table_id), K_(partition_id));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t task_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t table_id_;
  int64_t partition_id_;
};

struct ObBackupBackupsetType final {
  enum BackupBackupsetType {
    ALL_BACKUP_SET = 0,
    SINGLE_BACKUP_SET = 1,
    MAX,
  };

  ObBackupBackupsetType() : type_(MAX)
  {}
  ~ObBackupBackupsetType() = default;
  void reset()
  {
    type_ = MAX;
  }
  bool is_valid() const
  {
    return type_ >= ALL_BACKUP_SET && type_ < MAX;
  }
  const char* get_type_str() const;
  int set_type(const char* buf);

  TO_STRING_KV(K_(type));
  BackupBackupsetType type_;
};

struct ObBackupBackupsetJobItem {
public:
  enum JobStatus {
    SCHEDULE,
    BACKUP,
    FINISH,
    CANCEL,
    MAX,
  };

public:
  ObBackupBackupsetJobItem();
  virtual ~ObBackupBackupsetJobItem();

  void reset();
  bool is_valid() const;
  int set_status(const char* buf);
  static const char* get_status_str(const JobStatus& status);

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(incarnation), K_(backup_set_id), K_(copy_id), K_(type), K_(tenant_name),
      K_(job_status));
  typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> TenantName;

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t copy_id_;
  ObBackupBackupsetType type_;
  TenantName tenant_name_;
  JobStatus job_status_;
};

typedef ObBackupBackupsetJobItem ObBackupBackupsetJobInfo;

struct ObTenantBackupBackupsetTaskItem {
public:
  enum TaskStatus {
    GENERATE,
    BACKUP,
    FINISH,
    CANCEL,
    MAX,
  };

public:
  ObTenantBackupBackupsetTaskItem();
  virtual ~ObTenantBackupBackupsetTaskItem();

  void reset();
  bool is_valid() const;
  int set_status(const char* buf);
  static const char* get_status_str(const TaskStatus& status);
  int convert_to_backup_task_info(ObTenantBackupTaskInfo& info) const;

  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(backup_set_id), K_(task_status), K_(src_backup_dest), K_(dst_backup_dest),
      K_(start_ts), K_(end_ts), K_(total_pg_count), K_(finish_pg_count), K_(total_partition_count),
      K_(finish_partition_count), K_(total_macro_block_count), K_(finish_macro_block_count), K_(result),
      K_(is_mark_deleted));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t copy_id_;
  ObBackupType backup_type_;
  int64_t snapshot_version_;
  int64_t prev_full_backup_set_id_;
  int64_t prev_inc_backup_set_id_;
  int64_t prev_backup_data_version_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t start_ts_;
  int64_t end_ts_;
  int64_t compatible_;
  int64_t cluster_id_;
  int64_t cluster_version_;
  TaskStatus task_status_;
  ObBackupDest src_backup_dest_;
  ObBackupDest dst_backup_dest_;
  int64_t backup_data_version_;
  int64_t backup_schema_version_;
  int64_t total_pg_count_;
  int64_t finish_pg_count_;
  int64_t total_partition_count_;
  int64_t finish_partition_count_;
  int64_t total_macro_block_count_;
  int64_t finish_macro_block_count_;
  int result_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
  bool is_mark_deleted_;
};

typedef ObTenantBackupBackupsetTaskItem ObTenantBackupBackupsetTaskInfo;

struct ObPGBackupBackupsetTaskRowKey {
public:
  ObPGBackupBackupsetTaskRowKey();
  ~ObPGBackupBackupsetTaskRowKey();

  int set(const uint64_t tenant_id, const int64_t job_id, const int64_t incarnation, const int64_t backup_set_id,
      const int64_t copy_id, const int64_t table_id, const int64_t partition_id);
  void reset();
  bool is_valid() const;

  TO_STRING_KV(
      K_(tenant_id), K_(job_id), K_(incarnation), K_(backup_set_id), K_(copy_id), K_(table_id), K_(partition_id));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t copy_id_;
  int64_t table_id_;
  int64_t partition_id_;
};

struct ObPGBackupBackupsetTaskStat {
public:
  ObPGBackupBackupsetTaskStat();
  ~ObPGBackupBackupsetTaskStat();

  int set(const int64_t total_partition_cnt, const int64_t total_macro_block_cnt, const int64_t finish_partition_cnt,
      const int64_t finish_macro_block_cnt);
  void reset();
  bool is_valid() const;

  TO_STRING_KV(
      K_(total_partition_count), K_(total_macro_block_count), K_(finish_partition_count), K_(finish_macro_block_count));

  int64_t total_partition_count_;
  int64_t total_macro_block_count_;
  int64_t finish_partition_count_;
  int64_t finish_macro_block_count_;
};

struct ObPGBackupBackupsetTaskItem {
public:
  enum TaskStatus {
    PENDING,
    DOING,
    FINISH,
    MAX,
  };

public:
  ObPGBackupBackupsetTaskItem();
  virtual ~ObPGBackupBackupsetTaskItem();

  void reset();
  bool is_valid() const;
  int set_status(const char* buf);
  static const char* get_status_str(const TaskStatus& status);
  int set_trace_id(const char* buf, const int64_t buf_size);
  int get_trace_id(char* buf, const int64_t buf_size) const;
  static int get_trace_id(const ObTaskId& trace_id, const int64_t buf_size, char* buf);

  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(backup_set_id), K_(backup_set_id), K_(copy_id), K_(table_id),
      K_(partition_id), K_(status), K_(total_partition_count), K_(finish_partition_count), K_(total_macro_block_count),
      K_(finish_macro_block_count));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t copy_id_;
  int64_t table_id_;
  int64_t partition_id_;
  TaskStatus status_;
  share::ObTaskId trace_id_;
  common::ObAddr server_;
  int64_t total_partition_count_;
  int64_t finish_partition_count_;
  int64_t total_macro_block_count_;
  int64_t finish_macro_block_count_;
  int result_;
};

typedef ObPGBackupBackupsetTaskItem ObPGBackupBackupsetTaskInfo;

class ObBackupPath;
class ObIBackupLeaseService;
class ObBackupUtils {
public:
  ObBackupUtils()
  {}
  virtual ~ObBackupUtils()
  {}
  static int get_backup_info_default_timeout_ctx(common::ObTimeoutCtx& ctx);
  static bool is_need_retry_error(const int err);
  static bool is_extern_device_error(const int err);
  static int retry_get_tenant_schema_guard(const uint64_t tenant_id,
      schema::ObMultiVersionSchemaService& schema_service, const int64_t tenant_schema_version,
      schema::ObSchemaGetterGuard& schema_guard);

private:
  static const int64_t RETRY_INTERVAL = 10 * 1000 * 1000;
  static const int64_t MAX_RETRY_TIMES = 3;
};

struct ObClusterBackupDest final {
  ObClusterBackupDest();
  void reset();
  int set(const char* backup_dest, const int64_t incarnation);  // use GCONF cluster_name, cluster_id
  int set(const char* backup_dest, const char* cluster_name, const int64_t cluster_id, const int64_t incarnation);
  int set(const ObBackupDest& backup_dest, const int64_t incarnation);
  const char* get_storage_info() const
  {
    return dest_.storage_info_;
  }
  bool is_valid() const;
  bool is_same(const ObClusterBackupDest& other) const;
  uint64_t hash() const;
  bool operator==(const ObClusterBackupDest& other) const;

  TO_STRING_KV(K_(dest), K_(cluster_name), K_(cluster_id), K_(incarnation));

  ObBackupDest dest_;
  char cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH];
  int64_t cluster_id_;
  int64_t incarnation_;
};

struct ObBackupBaseDataPathInfo final {
  ObBackupBaseDataPathInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(dest), K_(tenant_id), K_(full_backup_set_id), K_(inc_backup_set_id));
  ObClusterBackupDest dest_;
  int64_t tenant_id_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
};

struct ObPhysicalRestoreInfo final {
  OB_UNIS_VERSION(1);

public:
  char backup_dest_[OB_MAX_BACKUP_DEST_LENGTH];
  char cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH];
  int64_t cluster_id_;
  int64_t incarnation_;
  int64_t tenant_id_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  int64_t log_archive_round_;
  int64_t restore_snapshot_version_;
  int64_t restore_start_ts_;
  int64_t compatible_;
  int64_t cluster_version_;  // restore tenant cluster version

  ObPhysicalRestoreInfo();
  bool is_valid() const;
  int assign(const ObPhysicalRestoreInfo& other);
  TO_STRING_KV(K_(backup_dest), K_(cluster_name), K_(cluster_id), K_(incarnation), K_(tenant_id),
      K_(full_backup_set_id), K_(inc_backup_set_id), K_(log_archive_round), K_(restore_snapshot_version),
      K_(restore_start_ts), K_(compatible), K_(cluster_version));
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreInfo);
};

struct ObRestoreBackupInfo final {
  lib::Worker::CompatMode compat_mode_;  // mysql or oracle mode
  char locality_[common::MAX_LOCALITY_LENGTH];
  char primary_zone_[common::MAX_ZONE_LIST_LENGTH];
  int64_t snapshot_version_;
  int64_t schema_version_;
  int64_t frozen_data_version_;
  int64_t frozen_snapshot_version_;
  int64_t frozen_schema_version_;
  ObPhysicalRestoreInfo physical_restore_info_;
  common::ObArray<common::ObPGKey> sys_pg_key_list_;

  ObRestoreBackupInfo();
  bool is_valid() const;
  TO_STRING_KV(K_(compat_mode), K_(locality), K_(primary_zone), K_(snapshot_version), K_(schema_version),
      K_(frozen_data_version), K_(frozen_snapshot_version), K_(frozen_schema_version), K_(physical_restore_info),
      K_(sys_pg_key_list));
};

struct ObPhysicalRestoreArg final {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalRestoreArg();
  ObPhysicalRestoreArg(const ObPhysicalRestoreArg& other);
  virtual ~ObPhysicalRestoreArg()
  {}
  ObPhysicalRestoreArg& operator=(const ObPhysicalRestoreArg& other);
  bool is_valid() const;
  int assign(const ObPhysicalRestoreArg& other);
  int trans_schema_id(const uint64_t schema_id, const uint64_t trans_tenant_id_, uint64_t& trans_schema_id) const;
  int get_backup_pgkey(common::ObPartitionKey& pg_key) const;
  int change_dst_pkey_to_src_pkey(const common::ObPartitionKey& dst_pkey, common::ObPartitionKey& src_pkey) const;
  int change_src_pkey_to_dst_pkey(const common::ObPartitionKey& src_pkey, common::ObPartitionKey& dst_pkey) const;
  int trans_to_backup_schema_id(const uint64_t schema_id, uint64_t& backup_schema_id) const;
  int trans_from_backup_schema_id(const uint64_t backup_schema_id, uint64_t& schema_id) const;
  int get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const;
  uint64_t get_tenant_id() const
  {
    return pg_key_.get_tenant_id();
  }
  TO_STRING_KV(K_(restore_info), K_(pg_key), K_(restore_data_version));

public:
  ObPhysicalRestoreInfo restore_info_;
  common::ObPartitionKey pg_key_;
  int64_t restore_data_version_;
};

struct ObPhysicalValidateArg final {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalValidateArg();
  virtual ~ObPhysicalValidateArg()
  {}
  ObPhysicalValidateArg& operator=(const ObPhysicalValidateArg& other);
  int assign(const ObPhysicalValidateArg& other);
  bool is_valid() const;
  int get_validate_pgkey(common::ObPartitionKey& pg_key) const;
  uint64_t get_tenant_id() const
  {
    return pg_key_.get_tenant_id();
  }
  int get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const;
  TO_STRING_KV(K_(backup_dest), K_(cluster_name), K_(uri_header), K_(storage_info), K_(job_id), K_(task_id),
      K_(trace_id), K_(server), K_(cluster_id), K_(pg_key), K_(table_id), K_(partition_id), K_(tenant_id),
      K_(incarnation), K_(archive_round), K_(backup_set_id), K_(total_partition_count), K_(total_macro_block_count),
      K_(clog_end_timestamp), K_(start_log_id), K_(end_log_id), K_(log_size), K_(is_dropped_tenant),
      K_(need_validate_clog), K_(full_backup_set_id), K_(inc_backup_set_id), K_(cluster_version));

public:
  char backup_dest_[OB_MAX_BACKUP_DEST_LENGTH];
  char cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH];
  char uri_header_[common::OB_MAX_URI_HEADER_LENGTH];
  char storage_info_[OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  int64_t job_id_;
  int64_t task_id_;
  share::ObTaskId trace_id_;
  common::ObAddr server_;
  int64_t cluster_id_;
  common::ObPartitionKey pg_key_;
  int64_t table_id_;
  int64_t partition_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  int64_t archive_round_;
  int64_t backup_set_id_;
  int64_t total_partition_count_;
  int64_t total_macro_block_count_;
  int64_t clog_end_timestamp_;
  int64_t start_log_id_;
  int64_t end_log_id_;
  int64_t log_size_;
  bool is_dropped_tenant_;
  bool need_validate_clog_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  int64_t cluster_version_;
};

struct ObPhysicalBackupArg final {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalBackupArg();
  void reset();
  bool is_valid() const;
  bool is_incremental_backup() const;
  int get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const;
  int get_prev_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const;
  TO_STRING_KV(K_(uri_header), K_(storage_info), K_(incarnation), K_(tenant_id), K_(backup_set_id),
      K_(backup_data_version), K_(backup_schema_version), K_(prev_full_backup_set_id), K_(prev_inc_backup_set_id),
      K_(prev_data_version), K_(task_id), K_(backup_type));
  char uri_header_[common::OB_MAX_URI_HEADER_LENGTH];
  char storage_info_[common::OB_MAX_URI_LENGTH];
  int64_t incarnation_;
  int64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t backup_data_version_;
  int64_t backup_schema_version_;
  int64_t prev_full_backup_set_id_;
  int64_t prev_inc_backup_set_id_;
  int64_t prev_data_version_;
  int64_t task_id_;
  ObBackupType::BackupType backup_type_;
  int64_t backup_snapshot_version_;
};

struct ObBackupBackupsetArg final {
  OB_UNIS_VERSION(1);

public:
  ObBackupBackupsetArg();
  void reset();
  bool is_valid() const;
  int get_src_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const;
  int get_dst_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const;

  TO_STRING_KV(K_(src_uri_header), K_(src_storage_info), K_(dst_uri_header), K_(dst_storage_info), K_(cluster_name),
      K_(server), K_(copy_id), K_(job_id), K_(tenant_id), K_(cluster_id), K_(incarnation), K_(pg_key),
      K_(backup_set_id), K_(prev_full_backup_set_id), K_(prev_inc_backup_set_id), K_(delete_input), K_(backup_type),
      K_(cluster_version));

  char src_uri_header_[common::OB_MAX_URI_HEADER_LENGTH];
  char src_storage_info_[common::OB_MAX_URI_LENGTH];
  char dst_uri_header_[common::OB_MAX_URI_HEADER_LENGTH];
  char dst_storage_info_[common::OB_MAX_URI_LENGTH];
  char cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH];
  int64_t cluster_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t copy_id_;
  int64_t backup_data_version_;
  int64_t backup_schema_version_;
  int64_t prev_full_backup_set_id_;
  int64_t prev_inc_backup_set_id_;
  int64_t prev_data_version_;
  common::ObPartitionKey pg_key_;
  common::ObAddr server_;
  ObBackupType::BackupType backup_type_;
  bool delete_input_;
  int64_t cluster_version_;
};

struct ObExternBackupInfo {
  OB_UNIS_VERSION(1);

public:
  enum ExternBackupInfoStatus {
    SUCCESS = 0,
    DOING = 1,
    FAILED = 2,
  };

public:
  ObExternBackupInfo();
  virtual ~ObExternBackupInfo()
  {}
  void reset();
  bool is_valid() const;
  bool is_equal(const ObExternBackupInfo& extern_backup_info) const;
  bool is_equal_without_status(const ObExternBackupInfo& extern_backup_info) const;

  TO_STRING_KV(K_(full_backup_set_id), K_(inc_backup_set_id), K_(backup_data_version), K_(backup_snapshot_version),
      K_(backup_schema_version), K_(frozen_snapshot_version), K_(frozen_schema_version), K_(prev_full_backup_set_id),
      K_(prev_inc_backup_set_id), K_(prev_backup_data_version), K_(compatible), K_(cluster_version), K_(backup_type),
      K_(status), K_(encryption_mode), K_(passwd), K_(is_mark_deleted));
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  int64_t backup_data_version_;
  int64_t backup_snapshot_version_;
  int64_t backup_schema_version_;
  int64_t frozen_snapshot_version_;
  int64_t frozen_schema_version_;
  int64_t prev_full_backup_set_id_;
  int64_t prev_inc_backup_set_id_;
  int64_t prev_backup_data_version_;
  int64_t compatible_;
  int64_t cluster_version_;
  ObBackupType::BackupType backup_type_;
  ExternBackupInfoStatus status_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
  bool is_mark_deleted_;
};

struct ObExternBackupSetInfo {
  OB_UNIS_VERSION(1);

public:
  ObExternBackupSetInfo();
  virtual ~ObExternBackupSetInfo() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(backup_set_id), K_(backup_snapshot_version), K_(compatible), K_(pg_count), K_(macro_block_count),
      K_(input_bytes), K_(output_bytes), K_(cluster_version), K_(compress_type));

  int64_t backup_set_id_;
  int64_t backup_snapshot_version_;
  int64_t compatible_;
  int64_t pg_count_;
  int64_t macro_block_count_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  uint64_t cluster_version_;
  common::ObCompressorType compress_type_;
  // tag_info
};

// TODO () condsider change tenant name
struct ObExternTenantInfo {
  OB_UNIS_VERSION(1);

public:
  ObExternTenantInfo();
  virtual ~ObExternTenantInfo() = default;
  void reset();
  bool is_valid() const;
  typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> TenantName;
  TO_STRING_KV(K_(tenant_id), K_(create_timestamp), K_(delete_timestamp), K_(compat_mode), K_(tenant_name),
      K_(backup_snapshot_version));

  uint64_t tenant_id_;
  int64_t create_timestamp_;
  int64_t delete_timestamp_;
  lib::Worker::CompatMode compat_mode_;  // mysql or oracle mode
  TenantName tenant_name_;
  int64_t backup_snapshot_version_;
};

struct ObExternTenantLocalityInfo {
  OB_UNIS_VERSION(1);

public:
  ObExternTenantLocalityInfo();
  virtual ~ObExternTenantLocalityInfo() = default;
  void reset();
  bool is_valid() const;
  bool is_equal(const ObExternTenantLocalityInfo& extern_tenant_locality_info) const;
  typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> TenantName;
  typedef common::ObFixedLengthString<common::MAX_LOCALITY_LENGTH> Locality;
  typedef common::ObFixedLengthString<common::MAX_ZONE_LIST_LENGTH> PrimaryZone;
  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(backup_snapshot_version), K_(compat_mode), K_(tenant_name),
      K_(locality), K_(primary_zone));

  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t backup_snapshot_version_;
  lib::Worker::CompatMode compat_mode_;
  TenantName tenant_name_;
  Locality locality_;
  PrimaryZone primary_zone_;
};

struct ObExternBackupDiagnoseInfo {
public:
  ObExternBackupDiagnoseInfo();
  virtual ~ObExternBackupDiagnoseInfo() = default;
  void reset();
  uint64_t tenant_id_;
  ObExternTenantLocalityInfo tenant_locality_info_;
  ObExternBackupSetInfo backup_set_info_;
  ObExternBackupInfo extern_backup_info_;
  TO_STRING_KV(K_(tenant_id), K_(tenant_locality_info), K_(backup_set_info), K_(extern_backup_info));
};

struct ObBackupCleanInfoStatus final {
  enum STATUS {
    STOP = 0,
    PREPARE = 1,
    DOING = 2,
    CANCEL = 3,
    MAX,
  };

  static const char* get_str(const STATUS& status);
  static STATUS get_status(const char* status_str);
  static OB_INLINE bool is_valid(const STATUS& status)
  {
    return status >= 0 && status < MAX;
  }
};

struct ObBackupCleanType final {
  enum TYPE {
    EMPTY_TYPE = 0,
    DELETE_OBSOLETE_BACKUP = 1,
    DELETE_BACKUP_SET = 2,
    MAX,
  };

  static const char* get_str(const TYPE& type);
  static TYPE get_type(const char* type_str);
  static OB_INLINE bool is_valid(const TYPE& type)
  {
    return type >= 0 && type < MAX;
  }
};

struct ObBackupCleanInfo {
public:
  ObBackupCleanInfo();
  virtual ~ObBackupCleanInfo() = default;
  void reset();
  bool is_valid() const;
  bool is_same_info(const ObBackupCleanInfo& clean_info);
  bool is_empty_clean_type() const
  {
    return ObBackupCleanType::EMPTY_TYPE == type_;
  }
  bool is_expired_clean() const
  {
    return ObBackupCleanType::DELETE_OBSOLETE_BACKUP == type_;
  }
  bool is_backup_set_clean() const
  {
    return ObBackupCleanType::DELETE_BACKUP_SET == type_;
  }
  int get_clean_parameter(int64_t& parameter) const;
  int set_clean_parameter(const int64_t parameter);
  int check_backup_clean_info_match(const ObBackupCleanInfo& clean_info) const;

  typedef common::ObFixedLengthString<common::OB_MAX_ERROR_MSG_LEN> ErrorMsg;
  typedef common::ObFixedLengthString<common::MAX_TABLE_COMMENT_LENGTH> Comment;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(start_time), K_(end_time), K_(incarnation), K_(type), K_(status),
      K_(expired_time), K_(backup_set_id), K_(error_msg), K_(comment), K_(clog_gc_snapshot), K_(result));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t incarnation_;
  ObBackupCleanType::TYPE type_;
  ObBackupCleanInfoStatus::STATUS status_;
  int64_t expired_time_;
  int64_t backup_set_id_;
  ErrorMsg error_msg_;
  Comment comment_;
  int64_t clog_gc_snapshot_;
  int32_t result_;
};

struct ObBackupDataType final {
  enum BackupDataType {
    BACKUP_MAJOR = 0,
    BACKUP_MINOR = 1,
    MAX,
  };

  ObBackupDataType() : type_(MAX)
  {}
  virtual ~ObBackupDataType() = default;
  void reset()
  {
    type_ = MAX;
  }
  bool is_valid() const
  {
    return type_ >= BACKUP_MAJOR && type_ < MAX;
  }
  static OB_INLINE bool is_major_backup(const BackupDataType& type)
  {
    return BACKUP_MAJOR == type;
  }
  static OB_INLINE bool is_minor_backup(const BackupDataType& type)
  {
    return BACKUP_MINOR == type;
  }
  bool is_major_backup() const
  {
    return ObBackupDataType::is_major_backup(type_);
  }
  bool is_minor_backup() const
  {
    return ObBackupDataType::is_minor_backup(type_);
  }
  void set_major_data_backup()
  {
    type_ = BACKUP_MAJOR;
  }
  void set_minor_data_backup()
  {
    type_ = BACKUP_MINOR;
  }

  TO_STRING_KV(K_(type));
  BackupDataType type_;
};

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_STRUCT_H_ */
