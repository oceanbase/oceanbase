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

enum ObBackupBackupCopyIdLevel {
  OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST = 1,
  OB_BB_COPY_ID_LEVEL_CLUSTER_USER_DEST = 2,
  OB_BB_COPY_ID_LEVEL_TENANT_GCONF_DEST = 3,
  OB_BB_COPY_ID_LEVEL_TENANT_USER_DEST = 4,
  OB_BB_COPY_ID_LEVEL_MAX,
};

int get_backup_backup_start_copy_id(const ObBackupBackupCopyIdLevel& copy_id_level, int64_t& copy_id);
int get_backup_copy_id_range_by_copy_level(
    const ObBackupBackupCopyIdLevel& copy_id_level, int64_t& left_copy_id, int64_t& right_copy_id);

enum ObBackupCompatibleVersion          // used for data backup
{ OB_BACKUP_COMPATIBLE_VERSION_V1 = 1,  // since 2.2.60
  OB_BACKUP_COMPATIBLE_VERSION_V2 = 2,  // since 2.2.77
  OB_BACKUP_COMPATIBLE_VERSION_V3 = 3,  // since 3.1 before BP6
  OB_BACKUP_COMPATIBLE_VERSION_V4 = 4,  // since 3.1 after BP6
  OB_BACKUP_COMPATIBLE_VERSION_MAX,
};

const static ObBackupCompatibleVersion OB_BACKUP_CURRENT_COMPAITBLE_VERSION =
    ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V4;

bool has_independ_inc_backup_set(const int64_t version);
// for log archive and data backup, exclude backup lease service inner table
enum ObBackupInnerTableVersion {
  OB_BACKUP_INNER_TABLE_V1 = 1,  // since 2.2.60
  OB_BACKUP_INNER_TABLE_V2 = 2,  // upgrade to 2.2.77 with upgrade exist tenants backup inner table
  OB_BACKUP_INNER_TABLE_V3 =
      3,  // new bootstrap cluster for 2.2.77; or upgrade to 2.2.77 with upgrade all tenants backup inner table
  OB_BACKUP_INNER_TABLE_VMAX,
};
bool is_valid_backup_inner_table_version(const ObBackupInnerTableVersion& version);

const int64_t OB_START_COPY_ID = 1;
const int64_t OB_CLUSTER_USER_DEST_START_COPY_ID = 10001;  // different from gconf.backup_backup_dest
const int64_t OB_TENANT_GCONF_DEST_START_COPY_ID = 20001;
const int64_t OB_TENANT_USER_DEST_START_COPY_ID = 30001;
const int64_t OB_MAX_COPY_ID = 50000;
const int64_t OB_START_ROUND_ID = 1;
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
const int64_t OB_DEFAULT_BACKUP_LOG_INTERVAL = 60 * 1000 * 1000;  // 60s
const int64_t OB_MAX_LOG_ARCHIVE_THREAD_NUM = 20;
const int64_t OB_GROUP_BACKUP_CONCURRENCY = 1;
const int64_t OB_GROUP_VALIDATE_CONCURRENCY = 1;
const int64_t OB_MAX_INCREMENTAL_BACKUP_NUM = 64;
const int64_t OB_MAX_LOG_ARCHIVE_CONCURRENCY = 128;
const int64_t OB_BACKUP_PIECE_DIR_NAME_LENGTH = 128;
const int64_t OB_BACKUP_NO_SWITCH_PIECE_ID = 0;
const int64_t OB_BACKUP_INVALID_PIECE_ID = -1;
const int64_t OB_BACKUP_SWITCH_BASE_PIECE_ID = 1;
const int64_t OB_MIN_LOG_ARCHIVE_CHECKPOINT_INTERVAL = 5 * 1000LL * 1000LL;   // 5s
const int64_t OB_MIN_LOG_ARCHIVE_PIECE_SWITH_INTERVAL = 5 * 1000LL * 1000LL;  // 5s
const int64_t OB_MAX_BACKUP_ZONE_LENGTH = 1024;

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
const char* const OB_STR_CLUSTER_CLOG_BACKUP_PIECE_INFO = "cluster_backup_piece_info";
const char* const OB_STR_CLUSTER_CLOG_BACKUP_BACKUP_PIECE_INFO = "cluster_backup_backup_piece_info";
const char* const OB_STR_TENANT_CLOG_BACKUP_PIECE_INFO = "backup_piece_info";
const char* const OB_STR_TENANT_CLOG_BACKUP_BACKUP_PIECE_INFO = "backup_backup_piece_info";
const char* const OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO = "single_piece_info";
const char* const OB_STR_CLUSTER_BACKUP_SET_FILE_INFO = "cluster_backup_set_file_info";
const char* const OB_STR_TENANT_BACKUP_SET_FILE_INFO = "tenant_backup_set_file_info";
const char* const OB_STR_CLUSTER_BACKUP_BACKUP_SET_FILE_INFO = "cluster_backup_backup_set_file_info";
const char* const OB_STR_TENANT_BACKUP_BACKUP_SET_FILE_INFO = "tenant_backup_backup_set_file_info";
const char* const OB_STR_SINGLE_BACKUP_SET_INFO = "single_backup_set_info";
const char* const OB_STR_TMP_FILE_MARK = ".tmp.";
const char* const Ob_STR_BACKUP_REGION = "backup_region";
const char* const OB_STR_BACKUP_ZONE = "backup_zone";

const char* const OB_STRING_MJAOR_DATA = "major_data";
const char* const OB_STRING_MINOR_DATA = "minor_data";

const char* const OB_STR_LOG_ARCHIVE_STATUS = "log_archive_status";
const char* const OB_STR_BACKUP_DEST = "backup_dest";
const char* const OB_STR_BACKUP_BACKUP_DEST = "backup_backup_dest";
const char* const OB_STR_BACKUP_DEST_OPT = "backup_dest_option";
const char* const OB_STR_BACKUP_BACKUP_DEST_OPT = "backup_backup_dest_option";
const char* const OB_STR_ENABLE_LOG_ARCHIVE = "enable_log_archive";
const char* const OB_STR_TENANT_ID = "tenant_id";
const char* const OB_STR_LOG_ARCHIVE_ROUND = "log_archive_round";
const char* const OB_STR_ROUND_ID = "round_id";  // used in piece
const char* const OB_STR_MIN_FIRST_TIME = "min_first_time";
const char* const OB_STR_MAX_NEXT_TIME = "max_next_time";
const char* const OB_STR_STATUS = "status";
const char* const OB_STR_FILE_STATUS = "file_status";
const char* const OB_STR_BACKUP_ENCRYPTION_MODE = "encryption_mode";
const char* const OB_STR_BACKUP_PASSWD = "passwd";
const char* const OB_STR_COPY_ID = "copy_id";
const char* const OB_STR_IS_MARK_DELETED = "is_mark_deleted";
const char* const OB_STR_IS_MOUNT_FILE_CREATED = "is_mount_file_created";
const char* const OB_STR_COMPATIBLE = "compatible";
const char* const OB_STR_CREATE_DATE = "create_date";
const char* const OB_STR_START_TS = "start_ts";
const char* const OB_STR_CHECKPOINT_TS = "checkpoint_ts";
const char* const OB_STR_MAX_TS = "max_ts";
const char* const OB_STR_BACKUP_PIECE_ID = "backup_piece_id";
const char* const OB_STR_START_PIECE_ID = "start_piece_id";
const char* const OB_STR_MAX_BACKUP_PIECE_ID = "max_backup_piece_id";
const char* const OB_STR_MAX_BACKUP_PIECE_CREATE_DATE = "max_backup_create_date";
const char* const OB_STR_FULL_BACKUP = "full";
const char* const OB_STR_INC_BACKUP = "inc";
const char* const OB_STR_AUTO_DELETE_EXPIRED_BACKUP = "auto_delete_expired_backup";
const char* const OB_STR_AUTO_UPDATE_RESERVED_BACKUP_TIMESTAMP = "_auto_update_reserved_backup_timestamp";
const char *const OB_STR_BACKUP_RECORVERTY_WINDOW = "backup_recovery_window";
const char* const OB_STR_BACKUP_INNER_TABLE_VERSION = "inner_table_version";

const char* const OB_BACKUP_ENCRYPTION_MODE_SESSION_STR = "__ob_backup_encryption_mode__";
const char* const OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR = "__ob_backup_encryption_passwd__";
const char* const OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR = "__ob_backup_decryption_passwd_array__";

const char* const OB_RESTORE_SOURCE_NAME_SESSION_STR = "__ob_restore_source_name__";
const char* const OB_RESTORE_PREVIEW_TENANT_ID_SESSION_STR = "__ob_restore_preview_tenant_id__";
const char* const OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR = "__ob_restore_preview_backup_dest__";
const char* const OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR = "__ob_restore_preview_timestamp__";
const char* const OB_RESTORE_PREVIEW_BACKUP_CLUSTER_NAME_SESSION_STR = "__ob_restore_preview_backup_cluster_name__";
const char* const OB_RESTORE_PREVIEW_BACKUP_CLUSTER_ID_SESSION_STR = "__ob_restore_preview_backup_cluster_id__";
const char* const MULTI_BACKUP_SET_PATH_PREFIX = "BACKUPSET";
const char* const MULTI_BACKUP_PIECE_PATH_PREFIX = "BACKUPPIECE";

enum ObBackupFileType {
  BACKUP_META = 0,
  BACKUP_MACRO_DATA = 1,
  BACKUP_META_INDEX = 2,
  BACKUP_MACRO_DATA_INDEX = 3,
  BACKUP_SSTABLE_MACRO_INDEX = 4,  // not used
  BACKUP_LOG_ARCHIVE_BACKUP_INFO = 5,
  BACKUP_INFO = 6,
  BACKUP_SET_INFO = 7,
  BACKUP_PG_LIST = 8,
  BACKUP_TENANT_INFO = 9,
  BACKUP_FILE_END_MARK = 10,
  BACKUP_FILE_SWITCH_MARK = 11,
  BACKUP_TENANT_LOCALITY_INFO = 12,
  BACKUP_TENANT_DIAGNOSE_INFO = 13,
  BACKUP_TENANT_NAME_INFO = 14,
  BACKUP_PIECE_INFO = 15,
  BACKUP_SINGLE_PIECE_INFO = 16,
  BACKUP_SET_FILE_INFO = 17,
  // type <=255 is write header struct to disk directly
  // type > 255 is use serialization to disk
  BACKUP_MAX_DIRECT_WRITE_TYPE = 255,
  BACKUP_ARCHIVE_BLOCK_META = 0x4142,  // 16706
  BACKUP_ARCHIVE_INDEX_FILE = 0x4149,  // 16713 AI means ARCHIVE INDEX
  BACKUP_ARCHIVE_KEY_FILE = 0x414B,    // 16713 AK means ARCHIVE  KEY
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

typedef common::ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> ObBackupPathString;

typedef ObBackupPathString ObBackupSetPath;
typedef ObBackupPathString ObBackupPiecePath;

struct ObBackupBaseDataPathInfo;

struct ObBackupFileStatus final {
  enum STATUS {
    BACKUP_FILE_AVAILABLE = 0,
    BACKUP_FILE_COPYING = 1,
    BACKUP_FILE_INCOMPLETE = 2,
    BACKUP_FILE_DELETING = 3,
    BACKUP_FILE_EXPIRED = 4,
    BACKUP_FILE_BROKEN = 5,
    BACKUP_FILE_DELETED = 6,
    BACKUP_FILE_MAX
  };

  static const char* get_str(const STATUS& status);
  static STATUS get_status(const char* status_str);
  static OB_INLINE bool is_valid(const STATUS& status)
  {
    return status >= 0 && status < BACKUP_FILE_MAX;
  }
  static int check_can_change_status(
      const ObBackupFileStatus::STATUS& src_file_status, const ObBackupFileStatus::STATUS& dest_file_status);
  static bool can_show_in_preview(const ObBackupFileStatus::STATUS& status);
};
struct ObSimpleBackupSetPath {
  OB_UNIS_VERSION(1);

public:
  ObSimpleBackupSetPath();
  ~ObSimpleBackupSetPath();
  void reset();
  int set(const common::ObString& uri);
  int set(const common::ObString& path, const common::ObString& storage_info);
  int set(const share::ObBackupBaseDataPathInfo& path_info);
  bool is_valid() const;
  common::ObString get_simple_path() const;
  common::ObString get_storage_info() const;
  TO_STRING_KV(K_(backup_set_id), K_(copy_id), K_(backup_dest), K_(file_status));
  int64_t backup_set_id_;
  int64_t copy_id_;
  ObBackupSetPath backup_dest_;
  ObBackupFileStatus::STATUS file_status_;
};

struct ObSimpleBackupPiecePath {
  OB_UNIS_VERSION(1);

public:
  ObSimpleBackupPiecePath();
  ~ObSimpleBackupPiecePath();
  void reset();
  int set(const common::ObString& uri);
  int set(const common::ObString& path, const common::ObString& storage_info);
  bool is_valid() const;
  common::ObString get_simple_path() const;
  common::ObString get_storage_info() const;
  TO_STRING_KV(K_(round_id), K_(backup_piece_id), K_(copy_id), K_(backup_dest), K_(file_status));
  int64_t round_id_;
  int64_t backup_piece_id_;
  int64_t copy_id_;
  ObBackupPiecePath backup_dest_;
  ObBackupFileStatus::STATUS file_status_;
};

struct ObCompareSimpleBackupSetPath {
  bool operator()(const share::ObSimpleBackupSetPath& lhs, const share::ObSimpleBackupSetPath& rhs);
};
struct ObCompareSimpleBackupPiecePath {
  bool operator()(const share::ObSimpleBackupPiecePath& lhs, const share::ObSimpleBackupPiecePath& rhs);
};

class ObPhysicalRestoreBackupDestList {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalRestoreBackupDestList();
  virtual ~ObPhysicalRestoreBackupDestList();

  int assign(const ObPhysicalRestoreBackupDestList& list);
  int set(const common::ObArray<share::ObSimpleBackupSetPath>& backup_set_list,
      const common::ObArray<share::ObSimpleBackupPiecePath>& backup_piece_list);
  void reset();

  int get_backup_set_list_format_str(common::ObIAllocator& allocator, common::ObString& str) const;
  int get_backup_piece_list_format_str(common::ObIAllocator& allocator, common::ObString& str) const;

  int backup_set_list_assign_with_hex_str(const common::ObString& str);
  int backup_piece_list_assign_with_hex_str(const common::ObString& str);

  int get_backup_set_list_hex_str(common::ObIAllocator& allocator, common::ObString& str) const;
  int get_backup_piece_list_hex_str(common::ObIAllocator& allocator, common::ObString& str) const;

  common::ObSArray<share::ObSimpleBackupSetPath>& get_backup_set_path_list()
  {
    return backup_set_path_list_;
  }
  common::ObSArray<share::ObSimpleBackupPiecePath>& get_backup_piece_path_list()
  {
    return backup_piece_path_list_;
  }
  const common::ObSArray<share::ObSimpleBackupSetPath>& get_backup_set_path_list() const
  {
    return backup_set_path_list_;
  }
  const common::ObSArray<share::ObSimpleBackupPiecePath>& get_backup_piece_path_list() const
  {
    return backup_piece_path_list_;
  }
  bool is_compat_backup_path() const
  {
    return backup_set_path_list_.empty();
  }

  TO_STRING_KV(K_(backup_set_path_list), K_(backup_piece_path_list));

private:
  int64_t get_backup_set_list_format_str_len() const;
  int64_t get_backup_piece_list_format_str_len() const;

private:
  common::ObArenaAllocator allocator_;
  common::ObSArray<share::ObSimpleBackupSetPath> backup_set_path_list_;
  common::ObSArray<share::ObSimpleBackupPiecePath> backup_piece_path_list_;
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
  bool operator==(const ObBackupMetaIndex& other) const;
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
  enum STATUS {
    INVALID = 0,
    STOP = 1,
    BEGINNING = 2,
    DOING = 3,
    STOPPING = 4,
    INTERRUPTED = 5,
    MIXED = 6,
    PAUSED = 7,  // only used for backup backup log archive
    MAX
  };

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
  uint64_t backup_piece_id_;
  bool is_valid() const;
  ObGetTenantLogArchiveStatusArg() : incarnation_(0), round_(0), backup_piece_id_(0)
  {}
  TO_STRING_KV(K_(incarnation), K(round_), K_(backup_piece_id));
};

struct ObBackupPieceInfoKey;
struct ObServerTenantLogArchiveStatus;
struct ObTenantLogArchiveStatus final {
  enum COMPATIBLE : int64_t { NONE = 0, COMPATIBLE_VERSION_1 = 1, COMPATIBLE_VERSION_2 = 2, MAX };

  OB_UNIS_VERSION(1);

public:
  ObTenantLogArchiveStatus();
  void reset();
  bool is_valid() const;
  static bool is_compatible_valid(COMPATIBLE compatible);
  int update(const ObTenantLogArchiveStatus& new_status);
  int get_piece_key(ObBackupPieceInfoKey& key) const;
  bool need_switch_piece() const;

  uint64_t tenant_id_;
  int64_t copy_id_;
  int64_t start_ts_;
  int64_t checkpoint_ts_;
  int64_t incarnation_;
  int64_t round_;
  ObLogArchiveStatus::STATUS status_;
  bool is_mark_deleted_;
  bool is_mount_file_created_;  // used to check if backup dest is mount properly
  COMPATIBLE compatible_;
  int64_t backup_piece_id_;
  int64_t start_piece_id_;
  TO_STRING_KV(K_(tenant_id), K_(copy_id), K_(start_ts), K_(checkpoint_ts), K_(status), K_(incarnation), K_(round),
      "status_str", ObLogArchiveStatus::get_str(status_), K_(is_mark_deleted), K_(is_mount_file_created),
      K_(compatible), K_(backup_piece_id), K_(start_piece_id));

private:
  int update_stop_(const ObTenantLogArchiveStatus& new_status);
  int update_beginning_(const ObTenantLogArchiveStatus& new_status);
  int update_doing_(const ObTenantLogArchiveStatus& new_status);
  int update_stopping_(const ObTenantLogArchiveStatus& new_status);
  int update_interrupted_(const ObTenantLogArchiveStatus& new_status);
  int update_paused_(const ObTenantLogArchiveStatus& new_status);
};

struct ObTenantLogArchiveStatusWrapper final {
  OB_UNIS_VERSION(1);

public:
  ObTenantLogArchiveStatusWrapper();
  int32_t result_code_;
  common::ObSArray<ObTenantLogArchiveStatus> status_array_;
  TO_STRING_KV(K_(result_code), K_(status_array));
};

struct ObServerTenantLogArchiveStatus final {
  OB_UNIS_VERSION(1);

public:
  uint64_t tenant_id_;
  int64_t incarnation_;
  ObLogArchiveStatus::STATUS status_;
  int64_t round_;
  int64_t min_backup_piece_id_;  // the min piece of all partitions on server
  int64_t start_ts_;
  int64_t checkpoint_ts_;
  int64_t max_log_ts_;  // the max log ts of all partitions on server
  TO_STRING_KV(K_(tenant_id), K_(incarnation), K_(status), K_(round), K_(min_backup_piece_id), K_(start_ts),
      K_(checkpoint_ts), K_(max_log_ts), "status_str", ObLogArchiveStatus::get_str(status_));

  ObServerTenantLogArchiveStatus();
  bool is_valid() const;
  void reset();
  int get_compat_status(ObTenantLogArchiveStatus& status) const;
  int set_status(const ObTenantLogArchiveStatus& status);
};

struct ObServerTenantLogArchiveStatusWrapper final {
  OB_UNIS_VERSION(1);

public:
  ObServerTenantLogArchiveStatusWrapper();
  int32_t result_code_;
  common::ObSArray<ObServerTenantLogArchiveStatus> status_array_;
  TO_STRING_KV(K_(result_code), K_(status_array));
};

struct ObBackupDest;

struct ObLogArchiveBackupInfo final {
  ObLogArchiveBackupInfo();

  ObTenantLogArchiveStatus status_;
  char backup_dest_[OB_MAX_BACKUP_DEST_LENGTH];

  void reset();
  bool is_valid() const;
  bool is_same(const ObLogArchiveBackupInfo& other) const;
  int get_piece_key(ObBackupPieceInfoKey& key) const;
  bool is_oss() const;
  int get_backup_dest(ObBackupDest& backup_dest) const;
  TO_STRING_KV(K_(status), K_(backup_dest));
};

struct ObBackupDestOpt final {
  ObBackupDestOpt();
  int init(const bool is_backup_backup);
  int init(const bool is_backup_backup, const char* opt_str, const bool global_auto_delete_obsolete_backup,
      const int64_t global_backup_recovery_window, const int64_t global_log_archive_checkount_interval,
      const bool auto_touch);
  bool is_valid() const
  {
    return is_valid_;
  }
  void reset();
  bool is_switch_piece() const
  {
    return 0 != piece_switch_interval_;
  }

  TO_STRING_KV(K_(log_archive_checkpoint_interval), K_(recovery_window), K_(piece_switch_interval), K_(backup_copies),
      K_(auto_delete_obsolete_backup), K_(auto_touch_reserved_backup));
  int64_t log_archive_checkpoint_interval_;
  int64_t recovery_window_;
  int64_t piece_switch_interval_;
  int64_t backup_copies_;
  bool auto_delete_obsolete_backup_;
  bool auto_touch_reserved_backup_;  // for option auto_update_reserved_backup_timestamp
  bool is_valid_;

private:
  int parse_opt_(const char* opt_str, const bool*& delete_obsolete_ptr, const int64_t*& recovery_window_ptr,
      const int64_t*& checkount_interval_ptr, const bool*& auto_touch);
  int parse_time_interval_(const char* str, int64_t& val);
  int parse_int_(const char* str, int64_t& val);
  int parse_bool_(const char* str, bool& val);
  int fill_global_opt_(const bool* delete_obsolete_ptr, const int64_t* recovery_window_ptr,
      const int64_t* checkpoint_interval_ptr, const bool* auto_touch_ptr);
  int check_valid_();
};

struct ObBackupPieceInfoKey {
  int64_t incarnation_;
  uint64_t tenant_id_;
  int64_t round_id_;
  int64_t backup_piece_id_;  // 0 means piece not swtich in one round
  int64_t copy_id_;

  ObBackupPieceInfoKey();
  // ObBackupPieceInfoKey is serialized in ObBackupPieceInfo
  bool is_valid() const;
  void reset();
  bool operator==(const ObBackupPieceInfoKey& o) const;
  bool operator<(const ObBackupPieceInfoKey& o) const;
  TO_STRING_KV(K_(incarnation), K_(tenant_id), K_(round_id), K_(backup_piece_id), K_(copy_id));
};

// the first piece of a log archive round, init state is ACTIVE
// freeze piece step:
// 1. Change cur as previous and its state as FREEZING; Create new piece with ACTIVE state
// 2. RS check if all observers are using new piece. If true, change the previous piece state as FROZEN.
struct ObBackupPieceStatus final {
  enum STATUS {
    BACKUP_PIECE_ACTIVE = 0,
    BACKUP_PIECE_FREEZING = 1,
    BACKUP_PIECE_FROZEN = 2,
    BACKUP_PIECE_INACTIVE = 3,
    BACKUP_PIECE_MAX
  };

  static const char* get_str(const STATUS& status);
  static STATUS get_status(const char* status_str);
  static OB_INLINE bool is_valid(const STATUS& status)
  {
    return status >= 0 && status < BACKUP_PIECE_MAX;
  }
};

struct ObBackupDest;

struct ObBackupPieceInfo {
  OB_UNIS_VERSION(1);

public:
  ObBackupPieceInfoKey key_;
  int64_t create_date_;
  int64_t start_ts_;       // filled by backup round start or previous piece frozen.
  int64_t checkpoint_ts_;  // filled by trigger freeze piece
  int64_t max_ts_;         // filled by frozen
  ObBackupPieceStatus::STATUS status_;
  ObBackupFileStatus::STATUS file_status_;
  ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> backup_dest_;
  ObTenantLogArchiveStatus::COMPATIBLE compatible_;
  int64_t start_piece_id_;

  ObBackupPieceInfo();
  bool is_valid() const;
  void reset();
  int get_backup_piece_path(char* buf, const int64_t buf_len) const;
  int init_piece_info(const ObBackupPieceInfo& sys_piece, const uint64_t tenant_id);
  const char* get_status_str() const
  {
    return ObBackupPieceStatus::get_str(status_);
  }
  const char* get_file_status_str() const
  {
    return ObBackupFileStatus::get_str(file_status_);
  }
  int get_backup_dest(ObBackupDest& backup_dest) const;
  bool operator==(const ObBackupPieceInfo& o) const;
  bool operator!=(const ObBackupPieceInfo& o) const
  {
    return !(operator==(o));
  }
  TO_STRING_KV(K_(key), K_(create_date), K_(status), K_(file_status), K_(start_ts), K_(checkpoint_ts), K_(max_ts),
      K_(backup_dest), "status_str", get_status_str(), "file_status_str", get_file_status_str(), K_(compatible),
      K_(start_piece_id));
};

struct ObNonFrozenBackupPieceInfo final {
  bool has_prev_piece_info_;
  ObBackupPieceInfo prev_piece_info_;  // is valid only when is_freezing_ is true
  ObBackupPieceInfo cur_piece_info_;

  ObNonFrozenBackupPieceInfo();
  void reset();
  bool is_valid() const;
  int get_backup_piece_id(int64_t& active_piece_id) const;
  int get_backup_piece_info(int64_t& active_piece_id, int64_t& active_piece_create_date) const;
  DECLARE_TO_STRING;
};

struct ObBackupPieceResponse {
  ObBackupPieceInfoKey key_;
  ObBackupPieceStatus status_;
  int64_t checkpoint_ts_;
  int64_t max_ts_;
  int64_t pg_count_;
};

struct ObBackupDest final {
  ObBackupDest();
  int set(const common::ObString& backup_dest);
  int set(const char* backup_dest);
  int set(const char* root_path, const char* storage_info);
  const char* get_type_str() const;
  void reset();
  bool is_valid() const;
  bool is_nfs_storage() const;
  bool is_oss_storage() const;
  bool is_cos_storage() const;
  bool is_root_path_equal(const ObBackupDest& backup_dest) const;
  int get_backup_dest_str(char* buf, const int64_t buf_size) const;
  bool operator==(const ObBackupDest& backup_dest) const;
  bool operator!=(const ObBackupDest& backup_dest) const;
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

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(incarnation), K_(backup_dest), K_(backup_backup_dest),
      K_(backup_snapshot_version), K_(backup_schema_version), K_(backup_data_version), K_(detected_backup_region),
      K_(backup_type), K_(backup_status), K_(backup_task_id), K_(copy_id), K_(encryption_mode));

  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t incarnation_;
  BackupDest backup_dest_;
  BackupDest backup_backup_dest_;
  int64_t backup_snapshot_version_;
  int64_t backup_schema_version_;
  int64_t backup_data_version_;
  common::ObFixedLengthString<OB_INNER_TABLE_DEFAULT_VALUE_LENTH> detected_backup_region_;
  ObBackupType backup_type_;
  ObBackupInfoStatus backup_status_;
  int64_t backup_task_id_;
  int64_t copy_id_;
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
  int set_backup_task_status(const char *buf);
  uint64_t hash() const;
  bool operator==(const ObTenantBackupTaskItem &other) const;

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(incarnation), K_(snapshot_version), K_(prev_full_backup_set_id),
      K_(prev_inc_backup_set_id), K_(prev_backup_data_version), K_(pg_count), K_(macro_block_count),
      K_(finish_pg_count), K_(finish_macro_block_count), K_(input_bytes), K_(output_bytes), K_(start_time),
      K_(end_time), K_(compatible), K_(cluster_version), K_(backup_type), K_(status), K_(device_type), K_(result),
      K_(cluster_id), K_(backup_dest), K_(backup_data_version), K_(backup_schema_version), K_(partition_count),
      K_(finish_partition_count), K_(encryption_mode), K_(passwd), K_(is_mark_deleted), K_(start_replay_log_ts),
      K_(date), K_(copy_id));

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
  // default is 0, compatible is v1 start_log_ts is useless
  int64_t start_replay_log_ts_;
  // default is 0, compatible is v1 date is useless, format like 20210201
  int64_t date_;
  // default is 0, duration in inner table
  int64_t copy_id_;
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
    CLEAN,
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
  bool is_tenant_level() const
  {
    return OB_SYS_TENANT_ID != tenant_id_;
  }

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(incarnation), K_(backup_set_id), K_(type), K_(tenant_name), K_(job_status),
      K_(backup_dest), K_(max_backup_times), K_(result));
  typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> TenantName;
  typedef common::ObFixedLengthString<common::MAX_TABLE_COMMENT_LENGTH> Comment;

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  ObBackupBackupsetType type_;
  TenantName tenant_name_;
  JobStatus job_status_;
  share::ObBackupDest backup_dest_;
  int64_t max_backup_times_;
  int64_t result_;
  Comment comment_;
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
      K_(is_mark_deleted), K_(date));

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
  int64_t start_replay_log_ts_;
  int64_t date_;
};

typedef ObTenantBackupBackupsetTaskItem ObTenantBackupBackupsetTaskInfo;

struct ObPGBackupBackupsetTaskRowKey {
public:
  ObPGBackupBackupsetTaskRowKey();
  ~ObPGBackupBackupsetTaskRowKey();

  int set(const uint64_t tenant_id, const int64_t job_id, const int64_t incarnation, const int64_t backup_set_id,
      const int64_t copy_id, const int64_t table_id, const int64_t partition_id);
  void reset();
  void reuse();
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

struct ObBackupBackupPieceJobInfo {
public:
  enum JobType {
    ONLY_FROZEN = 0,
    WITH_ACTIVE_PIECE = 1,
    JOB_TYPE_MAX = 2,
  };
  enum JobStatus {
    SCHEDULE = 0,
    DOING = 1,
    CANCEL = 2,
    FINISH = 3,
    MAX,
  };

public:
  ObBackupBackupPieceJobInfo();
  void reset();
  bool is_valid() const;
  int set_status(const char* buf);
  const char* get_status_str() const;
  bool is_tenant_level() const
  {
    return OB_SYS_TENANT_ID != tenant_id_;
  }
  bool with_active_piece() const;

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(incarnation), K_(piece_id), K_(status), K_(backup_dest), K_(result));
  typedef common::ObFixedLengthString<common::MAX_TABLE_COMMENT_LENGTH> Comment;

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t piece_id_;
  JobStatus status_;
  share::ObBackupDest backup_dest_;
  int64_t max_backup_times_;  // -1 no limit
  int result_;
  Comment comment_;
  int64_t type_;
};

struct ObBackupBackupPieceTaskInfo {
  enum TaskStatus {
    DOING = 0,
    FINISH = 1,
    MAX,
  };

public:
  ObBackupBackupPieceTaskInfo();

  void reset();
  bool is_valid() const;
  int set_status(const char* buf);
  const char* get_status_str() const;
  int get_backup_piece_key(share::ObBackupPieceInfoKey& key) const;

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(incarnation), K_(round_id), K_(piece_id), K_(copy_id), K_(task_status),
      K_(backup_dest), K_(start_ts), K_(end_ts), K_(result));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t incarnation_;
  int64_t round_id_;
  int64_t piece_id_;
  int64_t copy_id_;
  TaskStatus task_status_;
  share::ObBackupDest backup_dest_;
  int64_t start_ts_;
  int64_t end_ts_;
  int result_;
};

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
  // format input string split with ',' or ';'
  template <class T>
  static int parse_backup_format_input(
      const ObString& format_input, const int64_t max_length, common::ObIArray<T>& array);
  static int get_snapshot_to_time_date(const int64_t snapshot_version, int64_t& date);
  static int check_user_tenant_gts(
      schema::ObMultiVersionSchemaService& schema_service, const ObIArray<uint64_t>& tenant_ids, bool& is_gts);
  static int check_gts(schema::ObMultiVersionSchemaService& schema_service, const uint64_t tenant_id, bool& is_gts);
  static bool can_backup_pieces_be_deleted(const ObBackupPieceStatus::STATUS& status);
  static int check_passwd(const char* passwd_array, const char* passwd);
  static int check_is_tmp_file(const common::ObString& file_name, bool& is_tmp_file);

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
  int set(const ObClusterBackupDest& dest, const uint64_t tenant_id, const int64_t full_backup_set_id,
      const int64_t inc_backup_set_id, const int64_t backup_date, const int64_t compatible);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(dest), K_(tenant_id), K_(full_backup_set_id), K_(inc_backup_set_id), K_(backup_date), K_(compatible));
  ObClusterBackupDest dest_;
  int64_t tenant_id_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  int64_t backup_date_;
  int64_t compatible_;
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
  ObPhysicalRestoreBackupDestList multi_restore_path_list_;
  int64_t backup_date_;  //backup base date, RS only backup data after the date
  int64_t restore_schema_version_;

  common::ObSArray<share::ObSimpleBackupSetPath>& get_backup_set_path_list()
  {
    return multi_restore_path_list_.get_backup_set_path_list();
  }
  common::ObSArray<share::ObSimpleBackupPiecePath>& get_backup_piece_path_list()
  {
    return multi_restore_path_list_.get_backup_piece_path_list();
  }
  const common::ObSArray<share::ObSimpleBackupSetPath>& get_backup_set_path_list() const
  {
    return multi_restore_path_list_.get_backup_set_path_list();
  }
  const common::ObSArray<share::ObSimpleBackupPiecePath>& get_backup_piece_path_list() const
  {
    return multi_restore_path_list_.get_backup_piece_path_list();
  }
  bool is_compat_backup_path() const
  {
    return multi_restore_path_list_.is_compat_backup_path();
  }

  ObPhysicalRestoreInfo();
  ~ObPhysicalRestoreInfo()
  {
    reset();
  }
  void reset();
  bool is_valid() const;
  bool is_switch_piece_mode() const;
  void set_array_label(const char* lable);
  int assign(const ObPhysicalRestoreInfo& other);
  TO_STRING_KV(K_(backup_dest), K_(cluster_name), K_(cluster_id), K_(incarnation), K_(tenant_id),
      K_(full_backup_set_id), K_(inc_backup_set_id), K_(log_archive_round), K_(restore_snapshot_version),
      K_(restore_start_ts), K_(compatible), K_(cluster_version), K_(restore_schema_version), K_(backup_date),
      K_(multi_restore_path_list));
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
  int get_largest_backup_set_path(share::ObSimpleBackupSetPath& simple_path) const;
  uint64_t get_tenant_id() const
  {
    return pg_key_.get_tenant_id();
  }
  int get_restore_set_list(common::ObIArray<share::ObSimpleBackupSetPath>& path_list) const;
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
      K_(need_validate_clog), K_(full_backup_set_id), K_(inc_backup_set_id), K_(cluster_version), K_(backup_date),
      K_(compatible));

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
  int64_t backup_date_;
  int64_t compatible_;
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
      K_(prev_data_version), K_(task_id), K_(backup_type), K_(backup_date), K_(prev_backup_date), K_(compatible));
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
  int64_t backup_date_;
  int64_t prev_backup_date_;
  int64_t compatible_;
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
      K_(cluster_version), K_(backup_date), K_(compatible));

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
  bool tenant_dropped_;
  int64_t backup_date_;
  int64_t compatible_;
};

struct ObBackupArchiveLogArg final {
  OB_UNIS_VERSION(1);

public:
  ObBackupArchiveLogArg();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(pg_key), K_(log_archive_round), K_(piece_id), K_(create_date), K_(job_id), K_(src_backup_dest),
      K_(dst_backup_dest));
  common::ObPGKey pg_key_;
  uint64_t tenant_id_;
  int64_t log_archive_round_;
  int64_t piece_id_;
  int64_t create_date_;
  int64_t job_id_;
  int64_t rs_checkpoint_ts_;
  char src_backup_dest_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char src_storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  char dst_backup_dest_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char dst_storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
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
      K_(status), K_(encryption_mode), K_(passwd), K_(is_mark_deleted), K_(date));
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
  int64_t date_;
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
    DELETE_BACKUP_PIECE = 3,
    DELETE_BACKUP_ALL = 4,
    DELETE_OBSOLETE_BACKUP_BACKUP = 5,
    DELETE_BACKUP_ROUND = 6,
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
  bool is_clean_copy() const
  {
    return copy_id_ > 0;
  }
  bool is_empty_clean_type() const
  {
    return ObBackupCleanType::EMPTY_TYPE == type_;
  }
  bool is_delete_obsolete_backup() const
  {
    return ObBackupCleanType::DELETE_OBSOLETE_BACKUP == type_;
  }
  bool is_delete_obsolete_backup_backup() const
  {
    return ObBackupCleanType::DELETE_OBSOLETE_BACKUP_BACKUP == type_;
  }
  bool is_backup_set_clean() const
  {
    return ObBackupCleanType::DELETE_BACKUP_SET == type_;
  }
  bool is_delete_obsolete() const
  {
    return is_delete_obsolete_backup() || is_delete_obsolete_backup_backup();
  }
  bool is_delete_backup_set() const
  {
    return ObBackupCleanType::DELETE_BACKUP_SET == type_;
  }
  bool is_delete_backup_piece() const
  {
    return ObBackupCleanType::DELETE_BACKUP_PIECE == type_;
  }
  bool is_delete_backup_round() const
  {
    return ObBackupCleanType::DELETE_BACKUP_ROUND == type_;
  }

  int get_clean_parameter(int64_t& parameter) const;
  int set_clean_parameter(const int64_t parameter);
  int check_backup_clean_info_match(const ObBackupCleanInfo& clean_info) const;
  int set_copy_id(const int64_t copy_id);

  typedef common::ObFixedLengthString<common::OB_MAX_ERROR_MSG_LEN> ErrorMsg;
  typedef common::ObFixedLengthString<common::MAX_TABLE_COMMENT_LENGTH> Comment;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(start_time), K_(end_time), K_(incarnation), K_(copy_id), K_(type),
      K_(status), K_(expired_time), K_(backup_set_id), K_(error_msg), K_(comment), K_(clog_gc_snapshot), K_(result),
      K_(backup_piece_id), K_(backup_round_id));

  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t incarnation_;
  int64_t copy_id_;
  ObBackupCleanType::TYPE type_;
  ObBackupCleanInfoStatus::STATUS status_;
  int64_t expired_time_;
  int64_t backup_set_id_;
  ErrorMsg error_msg_;
  Comment comment_;
  int64_t clog_gc_snapshot_;
  int32_t result_;
  int64_t backup_piece_id_;
  int64_t backup_round_id_;
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

struct SimpleBackupBackupsetTenant {
  SimpleBackupBackupsetTenant() : is_dropped_(false), tenant_id_(OB_INVALID_ID)
  {}
  ~SimpleBackupBackupsetTenant()
  {}
  void reset()
  {
    tenant_id_ = OB_INVALID_ID;
    is_dropped_ = false;
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != tenant_id_;
  }
  TO_STRING_KV(K_(is_dropped), K_(tenant_id));
  bool is_dropped_;
  uint64_t tenant_id_;
};

struct ObBackupSetFileInfo {
  OB_UNIS_VERSION(1);

public:
  enum BackupSetStatus {
    DOING = 0,
    SUCCESS = 1,
    FAILED,
    MAX,
  };

public:
  ObBackupSetFileInfo();
  virtual ~ObBackupSetFileInfo() = default;
  void reset();
  bool is_key_valid() const;
  bool is_valid() const;
  bool is_same_task(const ObBackupSetFileInfo& other) const;
  bool is_equal(const ObBackupSetFileInfo& other) const;
  const char* get_backup_set_status_str() const;
  int set_backup_set_status(const char* buf);
  int extract_from_backup_task_info(const ObTenantBackupTaskInfo& task_info);
  int extract_from_backup_info(const ObBaseBackupInfoStruct& info, const ObExternBackupInfo& extern_backup_info);
  bool is_backup_finish() const
  {
    return SUCCESS == status_ || FAILED == status_;
  }
  int check_passwd(const char* passwd_array, const char* passwd);
  int convert_to_backup_backup_task_info(const int64_t job_id, const int64_t copy_id, const int64_t result,
      const share::ObBackupDest& dst_backup_dest, ObTenantBackupBackupsetTaskInfo& bb_task);
  int get_backup_dest(share::ObBackupDest& backup_dest) const;

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(incarnation), K_(copy_id), K_(snapshot_version),
      K_(prev_full_backup_set_id), K_(prev_inc_backup_set_id), K_(prev_backup_data_version), K_(pg_count),
      K_(macro_block_count), K_(finish_pg_count), K_(finish_macro_block_count), K_(input_bytes), K_(output_bytes),
      K_(start_time), K_(end_time), K_(compatible), K_(cluster_version), K_(backup_type), K_(status), K_(result),
      K_(cluster_id), K_(backup_dest), K_(backup_data_version), K_(backup_schema_version), K_(partition_count),
      K_(finish_partition_count), K_(encryption_mode), K_(passwd), K_(file_status), K_(start_replay_log_ts), K_(date));

  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t incarnation_;
  int64_t copy_id_;
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
  BackupSetStatus status_;
  int32_t result_;
  int64_t cluster_id_;
  common::ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> backup_dest_;
  int64_t backup_data_version_;
  int64_t backup_schema_version_;
  int64_t partition_count_;
  int64_t finish_partition_count_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<OB_MAX_PASSWORD_LENGTH> passwd_;
  ObBackupFileStatus::STATUS file_status_;
  int64_t start_replay_log_ts_;
  int64_t date_;
};

struct ObBackupStatistics {
  ObBackupStatistics();
  virtual ~ObBackupStatistics() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(pg_count), K_(partition_count), K_(finish_pg_count), K_(finish_partition_count),
      K_(macro_block_count), K_(finish_macro_block_count), K_(input_bytes), K_(output_bytes));
  int64_t pg_count_;
  int64_t finish_pg_count_;
  int64_t partition_count_;
  int64_t finish_partition_count_;
  int64_t macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t input_bytes_;
  int64_t output_bytes_;
};

struct ObBackupRegion {
  ObBackupRegion();
  virtual ~ObBackupRegion();
  void reset();
  int set(const ObString& region, const int64_t priority);

  bool is_valid() const
  {
    return !region_.is_empty() && priority_ >= 0;
  }
  TO_STRING_KV(K_(region), K_(priority));
  ObRegion region_;
  int64_t priority_;
};

struct ObBackupZone {
  ObBackupZone();
  virtual ~ObBackupZone();
  void reset();
  int set(const ObString& zone, const int64_t priority);

  bool is_valid() const
  {
    return !zone_.is_empty() && priority_ >= 0;
  }
  TO_STRING_KV(K_(zone), K_(priority));
  ObZone zone_;
  int64_t priority_;
};

struct ObBackupSetIdPair {
  ObBackupSetIdPair();
  virtual ~ObBackupSetIdPair() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(backup_set_id), K_(copy_id));
  int64_t backup_set_id_;
  int64_t copy_id_;
};

struct ObBackupPieceIdPair {
  ObBackupPieceIdPair();
  virtual ~ObBackupPieceIdPair() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(backup_piece_id), K_(copy_id));
  int64_t backup_piece_id_;
  int64_t copy_id_;
};

//-----------------------------ObBackupUtils---------------------------
template <class T>
int ObBackupUtils::parse_backup_format_input(const ObString& format_input, const int64_t max_length, ObIArray<T>& array)
{
  int ret = OB_SUCCESS;
  array.reset();
  int64_t pos = 0;
  int64_t length = 0;
  const char split_commma = ',';
  const char split_semicolon = ';';
  T object;
  int64_t priority = 0;

  if (max_length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "parse backup format input get invalid argument", K(ret), K(max_length));
  } else if (0 == format_input.length()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < format_input.length(); ++i) {

      if (format_input.ptr()[i] == split_commma || format_input.ptr()[i] == split_semicolon) {
        length = i - pos;
        if (length <= 0 || length > max_length) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "format input value is unexpcted", K(ret), K(format_input), K(length), K(max_length));
        } else {
          ObString tmp_string;
          object.reset();
          tmp_string.assign_ptr(format_input.ptr() + pos, length);
          if (OB_FAIL(object.set(tmp_string, priority))) {
            OB_LOG(WARN, "failed to set object", K(ret), K(tmp_string), K(priority));
          } else if (OB_FAIL(array.push_back(object))) {
            OB_LOG(WARN, "failed to push object into array", K(ret), K(object));
          } else {
            pos = i + 1;
            length = 0;
          }
        }

        if (OB_SUCC(ret) && format_input.ptr()[i] == split_semicolon) {
          ++priority;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (pos < format_input.length()) {
        length = format_input.length() - pos;
        ObString tmp_string;
        object.reset();
        tmp_string.assign_ptr(format_input.ptr() + pos, length);
        if (OB_FAIL(object.set(tmp_string, priority))) {
          OB_LOG(WARN, "failed to set object", K(ret), K(tmp_string), K(priority));
        } else if (OB_FAIL(array.push_back(object))) {
          OB_LOG(WARN, "failed to push object into array", K(ret), K(object));
        }
      }

      if (OB_SUCC(ret)) {
        OB_LOG(INFO, "succeed get objects", K(array));
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_STRUCT_H_ */
