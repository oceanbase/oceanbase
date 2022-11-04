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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_TYPE_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_TYPE_

#include <stdint.h>

namespace oceanbase
{
namespace storage
{
enum ObStorageLogType
{
  //attention:!!!
  //you should modify storage_log_type_to_string() below at the same when adding new log type
  //In addition, if you add new log types, please consider liboblog, archive and other consumption log applications outside
  //OB as supporting support
  OB_LOG_UNKNOWN = 0,

  OB_LOG_TRANS_REDO = 0x1,
  OB_LOG_TRANS_PREPARE = 0x2,
  OB_LOG_TRANS_REDO_WITH_PREPARE = 0x3,
  OB_LOG_TRANS_COMMIT = 0x4,
  OB_LOG_TRANS_PREPARE_WITH_COMMIT = 0x6,
  OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT = 0x7,
  OB_LOG_TRANS_ABORT = 0x8,
  OB_LOG_TRANS_CLEAR = 0x10,
  OB_LOG_TRANS_PREPARE_WITH_COMMIT_WITH_CLEAR = 0x16,
  OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR = 0x17,
  OB_LOG_SP_TRANS_REDO = 0x20,
  OB_LOG_SP_TRANS_COMMIT = 0x40,
  OB_LOG_SP_TRANS_ABORT = 0x80,
 
  OB_LOG_SP_ELR_TRANS_COMMIT = 0x100,

  //for OB_START_MEMBERSHIP
  OB_LOG_START_MEMBERSHIP_STORAGE = 0x148,

  OB_LOG_MUTATOR = 0x200,
  OB_LOG_TRANS_STATE = 0x400,
  OB_LOG_MUTATOR_WITH_STATE = 0x600,
  OB_LOG_MUTATOR_ABORT = 0x800,
  OB_LOG_TRANS_PRE_COMMIT = 0x1000,
  OB_LOG_TRANS_RECORD = 0x2000,

  OB_LOG_TRANS_AGGRE = 9999,
  OB_LOG_TRANS_MAX = 10000,

  OB_LOG_FREEZE_PREPARE = 10001,
  OB_LOG_FREEZE_COMMIT = 10002,
  OB_LOG_FREEZE_ABORT = 10003,
  OB_LOG_MINOR_FREEZE = 10004,
  OB_LOG_MAJOR_FREEZE = 10005,

  OB_LOG_SPLIT_SOURCE_PARTITION = 11001,
  OB_LOG_SPLIT_DEST_PARTITION = 11002,

  OB_LOG_STORAGE_SCHEMA = 11005,

  OB_LOG_TRANS_CHECKPOINT = 12000,

  // do offline log
  OB_LOG_OFFLINE_PARTITION = 20001,
  OB_LOG_OFFLINE_PARTITION_V2= 20002,// added since 2.2.6
  // for test
  OB_LOG_TEST = 30000,

  OB_LOG_TRANSFER_ADD_SSTORE = 40001,
  OB_LOG_TRANSFER_PREPARE = 40002,
  OB_LOG_TRANSFER_COMMIT = 40003,
  OB_LOG_TRANSFER_ABORT = 40004,
  OB_LOG_TRANSFER_CLEAR = 40005,
  OB_LOG_TRANS_LITE = 40006,
  OB_LOG_ADD_PARTITION_TO_PG = 40007,
  OB_LOG_REMOVE_PARTITION_FROM_PG = 40008,
  OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG = 40009,

  OB_LOG_FLASHBACK_PARTITION = 50001,
  OB_LOG_DDL_REDO_LOG = 60001,
  OB_LOG_DDL_COMMIT_LOG = 60002,
};

class ObStorageLogTypeToString
{
public:
  static const char *storage_log_type_to_string(const int64_t log_type) {
    const char *log_type_str = nullptr;
    switch (log_type) {
      case storage::OB_LOG_UNKNOWN:
        log_type_str = "UNKNOWN";
        break;
      case OB_LOG_TRANS_REDO:
        log_type_str = "TRANS_REDO";
        break;
      case OB_LOG_TRANS_PREPARE:
        log_type_str = "TRANS_PREPARE";
        break;
      case OB_LOG_TRANS_REDO_WITH_PREPARE:
        log_type_str = "TRANS_REDO_WITH_PREPARE";
        break;
      case OB_LOG_TRANS_COMMIT:
        log_type_str = "TRANS_COMMIT";
        break;
      case OB_LOG_TRANS_PREPARE_WITH_COMMIT:
        log_type_str = "TRANS_PREPARE_WITH_COMMIT";
        break;
      case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT:
        log_type_str = "TRANS_REDO_WITH_PREPARE_WITH_COMMIT";
        break;
      case OB_LOG_TRANS_ABORT:
        log_type_str = "TRANS_ABORT";
        break;
      case OB_LOG_TRANS_CLEAR:
        log_type_str = "TRANS_CLEAR";
        break;
      case OB_LOG_TRANS_PREPARE_WITH_COMMIT_WITH_CLEAR:
        log_type_str = "TRANS_PREPARE_WITH_COMMIT_WITH_CLEAR";
        break;
      case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR:
        log_type_str = "TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR";
        break;
      case OB_LOG_SP_TRANS_REDO:
        log_type_str = "SP_TRANS_REDO";
        break;
      case OB_LOG_SP_TRANS_COMMIT:
        log_type_str = "SP_TRANS_COMMIT";
        break;
      case OB_LOG_SP_TRANS_ABORT:
        log_type_str = "SP_TRANS_ABORT";
        break;
      case OB_LOG_SP_ELR_TRANS_COMMIT:
        log_type_str = "SP_ELR_TRANS_COMMIT";
        break;
      case OB_LOG_START_MEMBERSHIP_STORAGE:
        log_type_str = "START_MEMBERSHIP_STORAGE";
        break;
      case OB_LOG_MUTATOR:
        log_type_str = "MUTATOR";
        break;
      case OB_LOG_TRANS_STATE:
        log_type_str = "TRANS_STATE";
        break;
      case OB_LOG_MUTATOR_WITH_STATE:
        log_type_str = "MUTATOR_WITH_STATE";
        break;
      case OB_LOG_MUTATOR_ABORT:
        log_type_str = "MUTATOR_ABORT";
        break;
      case OB_LOG_TRANS_PRE_COMMIT:
        log_type_str = "TRANS_PRE_COMMIT";
        break;
      case OB_LOG_TRANS_AGGRE:
        log_type_str = "OB_LOG_TRANS_AGGRE";
        break;
      case OB_LOG_TRANS_MAX:
        log_type_str = "TRANS_MAX";
        break;
      case OB_LOG_FREEZE_PREPARE:
        log_type_str = "FREEZE_PREPARE";
        break;
      case OB_LOG_FREEZE_COMMIT:
        log_type_str = "FREEZE_COMMIT";
        break;
      case OB_LOG_FREEZE_ABORT:
        log_type_str = "FREEZE_ABORT";
        break;
      case OB_LOG_MINOR_FREEZE:
        log_type_str = "LOG_MINOR_FREEZE";
        break;
      case OB_LOG_MAJOR_FREEZE:
        log_type_str = "MAJOR_FREEZE";
        break;
      case OB_LOG_SPLIT_SOURCE_PARTITION:
        log_type_str = "SPLIT_SOURCE_PARTITION";
        break;
      case OB_LOG_SPLIT_DEST_PARTITION:
        log_type_str = "SPLIT_DEST_PARTITION";
        break;
      case OB_LOG_STORAGE_SCHEMA:
        log_type_str = "OB_LOG_STORAGE_SCHEMA";
        break;
      case OB_LOG_TRANS_CHECKPOINT:
        log_type_str = "TRANS_CHECKPOINT";
        break;
      case OB_LOG_OFFLINE_PARTITION:
        log_type_str = "OFFLINE_PARTITION";
        break;
      case OB_LOG_OFFLINE_PARTITION_V2:
        log_type_str = "OFFLINE_PARTITION_V2";
        break;
      case OB_LOG_TEST:
        log_type_str = "TEST";
        break;
      case OB_LOG_TRANSFER_ADD_SSTORE:
        log_type_str = "TRANSFER_ADD_SSTORE";
        break;
      case OB_LOG_TRANSFER_PREPARE:
        log_type_str = "TRANSFER_PREPARE";
        break;
      case OB_LOG_TRANSFER_COMMIT:
        log_type_str = "TRANSFER_COMMIT";
        break;
      case OB_LOG_TRANSFER_ABORT:
        log_type_str = "TRANSFER_ABORT";
        break;
      case OB_LOG_TRANSFER_CLEAR:
        log_type_str = "TRANSFER_CLEAR";
        break;
      case OB_LOG_TRANS_LITE:
        log_type_str = "TRANS_LITE";
        break;
      case OB_LOG_ADD_PARTITION_TO_PG:
        log_type_str = "ADD_PARTITION_TO_PG";
        break;
      case OB_LOG_REMOVE_PARTITION_FROM_PG:
        log_type_str = "REMOVE_PARTITION_FROM_PG";
        break;
      case OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG:
        log_type_str = "OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG";
        break;
      case OB_LOG_FLASHBACK_PARTITION:
        log_type_str = "FLASHBACK_PARTITION";
        break;
      case OB_LOG_DDL_REDO_LOG:
        log_type_str = "OB_LOG_DDL_REDO_LOG";
        break;
      case OB_LOG_DDL_COMMIT_LOG:
        log_type_str = "OB_LOG_DDL_COMMIT_LOG";
        break;
      case OB_LOG_TRANS_RECORD:
        log_type_str = "TRANS_RECORD";
        break;
      default:
        log_type_str = "INVALID_LOG_TYPE";
        break;
    }
    return log_type_str;
  }
};
class ObStorageLogTypeChecker
{
public:
  static bool is_trans_log(const int64_t log_type)
  {
  return (OB_LOG_TRANS_REDO == log_type ||
          OB_LOG_TRANS_PREPARE == log_type ||
          OB_LOG_TRANS_REDO_WITH_PREPARE == log_type ||
          OB_LOG_TRANS_COMMIT == log_type ||
          OB_LOG_TRANS_PREPARE_WITH_COMMIT == log_type ||
          OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT == log_type ||
          OB_LOG_TRANS_PREPARE_WITH_COMMIT_WITH_CLEAR == log_type ||
          OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR == log_type ||
          OB_LOG_TRANS_ABORT == log_type ||
          OB_LOG_TRANS_CLEAR == log_type ||
          OB_LOG_SP_TRANS_REDO == log_type ||
          OB_LOG_SP_TRANS_COMMIT == log_type ||
          OB_LOG_SP_ELR_TRANS_COMMIT == log_type ||
          OB_LOG_SP_TRANS_ABORT == log_type ||
          OB_LOG_TRANS_STATE == log_type ||
          OB_LOG_MUTATOR == log_type ||
          OB_LOG_MUTATOR_WITH_STATE == log_type ||
          OB_LOG_MUTATOR_ABORT == log_type ||
          OB_LOG_TRANS_AGGRE == log_type ||
          OB_LOG_TRANS_RECORD == log_type);
  }
  static bool is_trans_abort_log(const int64_t log_type)
  {
    return OB_LOG_TRANS_ABORT == log_type
      || OB_LOG_SP_TRANS_ABORT == log_type
      || OB_LOG_MUTATOR_ABORT == log_type;
  }
  static bool is_trans_commit_log(const int64_t log_type)
  {
    return OB_LOG_TRANS_COMMIT == log_type
      || OB_LOG_SP_TRANS_COMMIT == log_type;
  }
  static bool is_trans_redo_log(const int64_t log_type)
  {
    return OB_LOG_TRANS_REDO == log_type
      || OB_LOG_SP_TRANS_REDO == log_type;
  }
  static bool has_trans_mutator(const int64_t log_type)
  {
    return OB_LOG_TRANS_REDO == log_type ||
      OB_LOG_TRANS_REDO_WITH_PREPARE == log_type ||
      OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT == log_type ||
      OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR == log_type ||
      OB_LOG_SP_TRANS_REDO == log_type ||
      OB_LOG_SP_TRANS_COMMIT == log_type ||
      OB_LOG_SP_ELR_TRANS_COMMIT == log_type ||
      OB_LOG_MUTATOR == log_type ||
      OB_LOG_MUTATOR_WITH_STATE == log_type;
  }
  static bool is_freeze_log(const int64_t log_type)
  {
    return (OB_LOG_MAJOR_FREEZE == log_type
        || OB_LOG_FREEZE_PREPARE == log_type
        || OB_LOG_FREEZE_COMMIT == log_type
        || OB_LOG_FREEZE_ABORT == log_type
        || OB_LOG_MINOR_FREEZE == log_type);
  }
  static bool is_test_log(const int64_t log_type)
  {
    return OB_LOG_TEST == log_type;
  }
  static bool is_offline_partition_log(const int64_t log_type)
  {
    return (OB_LOG_OFFLINE_PARTITION == log_type || OB_LOG_OFFLINE_PARTITION_V2 == log_type);
  }
  static bool is_offline_partition_log_old(const int64_t log_type)
  {
    return OB_LOG_OFFLINE_PARTITION == log_type;
  }
  static bool is_offline_partition_log_new(const int64_t log_type)
  {
    return OB_LOG_OFFLINE_PARTITION_V2 == log_type;
  }
  static bool is_transfer_log(const int64_t log_type)
  {
    return (OB_LOG_TRANSFER_ADD_SSTORE == log_type
        || OB_LOG_TRANSFER_PREPARE == log_type
        || OB_LOG_TRANSFER_COMMIT == log_type
        || OB_LOG_TRANSFER_ABORT == log_type
        || OB_LOG_TRANSFER_CLEAR == log_type
        || OB_LOG_TRANS_LITE == log_type);
  }
  static bool is_split_log(const int64_t log_type)
  {
    return (OB_LOG_SPLIT_SOURCE_PARTITION == log_type ||
            OB_LOG_SPLIT_DEST_PARTITION == log_type);
  }
  static bool is_checkpoint_log(const int64_t log_type)
  {
    return (OB_LOG_TRANS_CHECKPOINT == log_type);
  }

  static bool is_flashback_log(const int64_t log_type)
  {
    return (OB_LOG_FLASHBACK_PARTITION == log_type);
  }

  static bool is_start_membership_log(const int64_t log_type)
  {
    return (OB_LOG_START_MEMBERSHIP_STORAGE == log_type);
  }

  static bool is_add_partition_to_pg_log(const int64_t log_type)
  {
    return (OB_LOG_ADD_PARTITION_TO_PG == log_type);
  }

  static bool is_ddl_log(const int64_t log_type)
  {
    return (OB_LOG_DDL_REDO_LOG == log_type || OB_LOG_DDL_COMMIT_LOG == log_type);
  }

  static bool is_remove_partition_from_pg_log(const int64_t log_type)
  {
    return (OB_LOG_REMOVE_PARTITION_FROM_PG == log_type);
  }

  static bool is_schema_version_change_log(const int64_t log_type)
  {
    return (OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG == log_type);
  }

  static bool is_partition_meta_log(const int64_t log_type)
  {
    return (OB_LOG_STORAGE_SCHEMA == log_type);
    // TODO: split log
  }

  static bool is_log_replica_need_replay_log(const int64_t log_type) 
  {
    // L replica only replay offline partition log
    return is_offline_partition_log(log_type);
  }
  static bool is_pre_barrier_required_log(const int64_t log_type)
  {

    return (OB_LOG_START_MEMBERSHIP_STORAGE == log_type
            || is_offline_partition_log(log_type)
            || OB_LOG_SPLIT_SOURCE_PARTITION == log_type
            || is_partition_meta_log(log_type)
            || is_remove_partition_from_pg_log(log_type)
            || is_flashback_log(log_type));
  }

  static bool is_post_barrier_required_log(const int64_t log_type)
  {
    return (is_start_membership_log(log_type)
            || is_partition_meta_log(log_type)
            || is_add_partition_to_pg_log(log_type)
            || is_flashback_log(log_type));
  }

  static bool is_valid_log_type(const int64_t log_type)
  {
    return is_trans_log(log_type)
           || is_test_log(log_type)
           || is_freeze_log(log_type)
           || is_offline_partition_log(log_type)
           || is_transfer_log(log_type)
           || is_split_log(log_type)
           || is_start_membership_log(log_type)
           || is_checkpoint_log(log_type)
           || is_partition_meta_log(log_type)
           || is_flashback_log(log_type)
           || is_add_partition_to_pg_log(log_type)
           || is_remove_partition_from_pg_log(log_type)
           || is_schema_version_change_log(log_type)
           || is_ddl_log(log_type);
  }
};

class ObTransLogType
{
public:
  static bool is_valid(const int64_t log_type)
  {
    return ObStorageLogTypeChecker::is_trans_log(log_type);
  }
};

inline bool need_update_trans_version(const int64_t log_type)
{
  return OB_LOG_TRANS_REDO == log_type ||
         OB_LOG_TRANS_PREPARE == log_type ||
         OB_LOG_TRANS_REDO_WITH_PREPARE == log_type ||
         OB_LOG_TRANS_PREPARE_WITH_COMMIT == log_type ||
         OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT == log_type ||
         OB_LOG_TRANS_PREPARE_WITH_COMMIT_WITH_CLEAR == log_type ||
         OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR == log_type ||
         OB_LOG_SP_TRANS_REDO == log_type ||
         OB_LOG_SP_TRANS_COMMIT == log_type ||
         OB_LOG_SP_ELR_TRANS_COMMIT == log_type;
}

inline bool need_carry_base_ts(const int64_t log_type)
{
  return OB_LOG_TRANS_COMMIT == log_type ||
         OB_LOG_TRANS_CLEAR == log_type;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_STORAGE_LOG_TYPE_
