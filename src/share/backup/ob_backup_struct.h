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
#include "share/ob_dml_sql_splicer.h"
#include "share/scn.h"

namespace oceanbase
{

namespace storage
{
class ObBackupLSMetaInfosDesc;
}
namespace share
{

// for log archive and data backup, exclude backup lease service inner table
enum ObBackupInnerTableVersion {
  OB_BACKUP_INNER_TABLE_V1 = 1, // since 2.2.60
  OB_BACKUP_INNER_TABLE_V2 = 2, // upgrade to 2.2.77 with upgrade exist tenants backup inner table
  OB_BACKUP_INNER_TABLE_V3 = 3, // new bootstrap cluster for 2.2.77; or upgrade to 2.2.77 with upgrade all tenants backup inner table
  OB_BACKUP_INNER_TABLE_VMAX,
};
bool is_valid_backup_inner_table_version(const ObBackupInnerTableVersion &version);

const int64_t OB_START_ROUND_ID = 1;
const int64_t OB_START_DEST_ID = 1001;
const int64_t OB_ALL_BACKUP_SET_ID = 0;
const int64_t OB_BACKUP_DEFAULT_PG_NUM = 10000;
const int64_t OB_MAX_BACKUP_DEST_LENGTH = 2048;
const int64_t OB_MAX_RESTORE_DEST_LENGTH = OB_MAX_BACKUP_DEST_LENGTH * 10;
const int64_t OB_MAX_BACKUP_PATH_LENGTH = 1024;
const int64_t OB_MAX_BACKUP_AUTHORIZATION_LENGTH = 1024;
const int64_t OB_MAX_BACKUP_CHECK_FILE_LENGTH = OB_MAX_BACKUP_PATH_LENGTH;
const int64_t OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH = 256;
const int64_t OB_MAX_KEPT_LOG_ARCHIVE_BACKUP_ROUND = 10 * 10000; // 10w
const int64_t OB_MAX_VALIDATE_LOG_INFO_LENGTH = 256;
const int64_t OB_START_BACKUP_SET_ID = 0;
const int64_t OB_DEFAULT_BACKUP_LOG_INTERVAL = 60 * 1000 * 1000; // 60s
const int64_t OB_MAX_LOG_ARCHIVE_THREAD_NUM = 20;
const int64_t OB_GROUP_BACKUP_CONCURRENCY = 1;
const int64_t OB_GROUP_VALIDATE_CONCURRENCY = 1;
const int64_t OB_MAX_INCREMENTAL_BACKUP_NUM = 64;
const int64_t OB_MAX_LOG_ARCHIVE_CONCURRENCY = 128;
const int64_t OB_BACKUP_PIECE_DIR_NAME_LENGTH = 128;
const int64_t OB_BACKUP_NO_SWITCH_PIECE_ID = 0;
const int64_t OB_BACKUP_INVALID_PIECE_ID = -1;
const int64_t OB_BACKUP_SWITCH_BASE_PIECE_ID = 1;
const int64_t OB_MIN_LAG_TARGET = 1 * 1000LL * 1000LL;// 1s
const int64_t OB_MAX_LAG_TARGET = 60 * 60 * 1000LL * 1000LL; // 1h
const int64_t OB_MIN_LOG_ARCHIVE_PIECE_SWITH_INTERVAL = 60 * 1000LL * 1000LL;// 1min
const int64_t OB_DEFAULT_PIECE_SWITCH_INTERVAL = 24 * 3600 * 1000LL * 1000LL;// 1d, unit:us
const int64_t OB_DEFAULT_LAG_TARGET = 120 * 1000LL * 1000LL;// 2min, unit:us
const int64_t OB_BACKUP_MAX_RETRY_TIMES = 64;
const int64_t OB_BACKUP_RETRY_TIME_INTERVAL = 5 * 1000LL * 1000LL;
const int64_t OB_BACKUP_DIR_PREFIX_LENGTH = 64;
const int64_t OB_BACKUP_DELETE_POLICY_NAME_LENGTH = 64;
const int64_t OB_BACKUP_RECOVERY_WINDOW_LENGTH = 32;
const int64_t OB_BACKUP_DEFAULT_FIXED_STR_LEN = 2048; // default fixed string length
const int64_t OB_BACKUP_MAX_TIME_STR_LEN = 50; // time string max length
const int64_t OB_BACKUP_INVALID_INCARNATION_ID = 0;
const int64_t OB_BACKUP_INVALID_BACKUP_SET_ID = 0;
const int64_t OB_BACKUP_INVALID_TURN_ID = 0;
const int64_t OB_BACKUP_INVALID_RETRY_ID = -1;
const int64_t OB_BACKUP_INVALID_JOB_ID = 0;
const int64_t OB_ARCHIVE_INVALID_ROUND_ID = 0;
const int64_t OB_INVALID_DEST_ID = -1;
const int64_t OB_MAX_BACKUP_QUERY_TIMEOUT = 60 * 1000 * 1000; // 60s
const int64_t OB_DEFAULT_RESTORE_CONCURRENCY = 8;
const int64_t OB_MAX_RESTORE_SOURCE_SERVICE_CONFIG_LEN = 7;
const int64_t OB_MAX_RESTORE_SOURCE_IP_LIST_LEN = MAX_IP_PORT_LENGTH * 6;
const int64_t OB_MAX_COMPAT_MODE_STR_LEN = 6; //ORACLE/MYSQL
const int64_t OB_MAX_RESTORE_USER_AND_TENANT_LEN = OB_MAX_ORIGINAL_NANE_LENGTH + OB_MAX_USER_NAME_LENGTH + 1;
const int64_t OB_MAX_RESTORE_TYPE_LEN = 8; // LOCATION/SERVICE/RAWPATH
const int64_t OB_MAX_BACKUP_SET_NUM = 1000000;

const int64_t OB_MAX_BACKUP_PIECE_NUM = 1000000;
const int64_t MIN_LAG_TARGET_FOR_S3 = 60 * 1000 * 1000UL/*60s*/;

static constexpr const int64_t MAX_FAKE_PROVIDE_ITEM_COUNT = 128;
static constexpr const int64_t DEFAULT_FAKE_BATCH_COUNT = 32;
static constexpr const int64_t FAKE_MAX_FILE_ID = MAX_FAKE_PROVIDE_ITEM_COUNT / DEFAULT_FAKE_BATCH_COUNT - 1;
static constexpr const int64_t OB_COMMENT_LENGTH = 1024;

static constexpr const int64_t DEFAULT_ARCHIVE_FILE_SIZE = 64 << 20; // 64MB
static constexpr const int64_t DEFAULT_BACKUP_DATA_FILE_SIZE = 4 * 1024LL * 1024LL * 1024LL; // 4GB

// max ObMigrationTabletParam serialize size during backup.
static constexpr const int64_t MAX_BACKUP_TABLET_META_SERIALIZE_SIZE = 2 * 1024LL * 1024LL; // 2MB

//add by physical backup and restore
const char *const OB_STR_INCARNATION = "incarnation";
const char *const OB_STRING_BACKUP_DATA = "data";
const char *const OB_STRING_BACKUP_CLOG = "clog";
const char *const OB_STRING_BACKUP_INDEX = "index";
const char *const OB_STRING_BACKUP_CLOG_INFO = "clog_info";
const char *const OB_STRING_BACKUP_ARCHIVE_KEY = "archive_key";
const char *const OB_STRING_BACKUP_FULL_BACKUP_SET = "backup_set";
const char *const OB_STRING_BACKUP_INC_BACKUP_SET = "backup";
const char *const OB_STRING_BACKUP_META_INDEX = "meta_index_file";
const char *const OB_STRING_BACKUP_META_FILE = "meta_file";
const char *const OB_STRING_BACKUP_SSTABLE_MACRO_INDEX = "sstable_macro_index";
const char *const OB_STRING_BACKUP_MACRO_BLOCK_INDEX = "macro_block_index";
const char *const OB_STRING_BACKUP_MACRO_BLOCK_FILE = "macro_block";
const char *const OB_STRING_CLUSTER_DATA_BACKUP_INFO = "cluster_data_backup_info";
const char *const OB_STRING_TENANT_DATA_BACKUP_INFO = "tenant_data_backup_info";
const char *const OB_STRING_BACKUP_SET_INFO = "backup_set_info";
const char *const OB_STRING_SYS_PG_LIST = "sys_pg_list";
const char *const OB_STRING_NORMAL_PG_LIST = "normal_pg_list";
const char *const OB_STRING_TENANT_INFO = "tenant_info";
const char *const OB_STRING_TENANT_NAME_INFO = "tenant_name_info";
const char *const OB_STRING_TENANT_LOCALITY_INFO = "tenant_locality_info";
const char *const OB_STRING_TENANT_DIAGNOSE_INFO = "tenant_diagnose_info";
const char *const OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION = "tenant_name_backup_schema_version";
const char *const OB_STR_BACKUP_SCHEDULER_LEADER = "backup_scheduler_leader";
const char *const OB_STR_BACKUP_SCHEDULER_LEADER_EPOCH = "backup_scheduler_leader_epoch";
const char *const OB_STR_CLUSTER_CLOG_BACKUP_INFO = "cluster_clog_backup_info";
const char *const OB_STR_TENANT_CLOG_BACKUP_INFO = "tenant_clog_backup_info";
const char *const OB_STR_CLUSTER_CLOG_BACKUP_PIECE_INFO = "cluster_backup_piece_info";
const char *const OB_STR_CLUSTER_CLOG_BACKUP_BACKUP_PIECE_INFO = "cluster_backup_backup_piece_info";
const char *const OB_STR_TENANT_CLOG_BACKUP_PIECE_INFO = "backup_piece_info";
const char *const OB_STR_TENANT_CLOG_BACKUP_BACKUP_PIECE_INFO = "backup_backup_piece_info";
const char *const OB_STR_TENANT_CLOG_SINGLE_BACKUP_PIECE_INFO = "single_piece_info";
const char *const OB_STR_TENANT_CLOG_CHECKPOINT_INFO = "checkpoint";
const char *const OB_STR_CLUSTER_BACKUP_SET_FILE_INFO = "cluster_backup_set_file_info";
const char *const OB_STR_TENANT_BACKUP_SET_FILE_INFO = "tenant_backup_set_file_info";
const char *const OB_STR_CLUSTER_BACKUP_BACKUP_SET_FILE_INFO = "cluster_backup_backup_set_file_info";
const char *const OB_STR_TENANT_BACKUP_BACKUP_SET_FILE_INFO = "tenant_backup_backup_set_file_info";
const char *const OB_STR_LS_FILE_INFO = "ls_file_info";
const char *const OB_STR_TMP_FILE_MARK = ".tmp.";
const char *const Ob_STR_BACKUP_REGION = "backup_region";
const char *const OB_STR_BACKUP_ZONE = "backup_zone";
const char *const OB_STR_JOB_ID = "job_id";
const char *const OB_STR_BACKUP_SET_ID = "backup_set_id";
const char *const OB_STR_BACKUP_COMPRESSOR_TYPE = "compressor_type";
const char *const OB_STR_INITIATOR_TENANT_ID = "initiator_tenant_id";
const char *const OB_STR_BACKUP_PLUS_ARCHIVELOG = "plus_archivelog";
const char *const OB_STR_BACKUP_TYPE = "backup_type";
const char *const OB_STR_END_TS = "end_ts";
const char *const OB_STR_RESULT = "result";
const char *const OB_STR_PRE_INC_SET_ID = "pre_inc_set_id";
const char *const OB_STR_TASK_ID = "task_id";
const char *const OB_STR_TASK_TYPE = "task_type";
const char *const OB_STR_OPTIONAL_SERVERS = "optional_servers";
const char *const OB_STR_TASK_TRACE_ID = "task_trace_id";
const char *const OB_STR_DATE = "date";
const char *const OB_STR_TURN_ID = "turn_id";
const char *const OB_STR_PREV_FULL_BACKUP_SET_ID = "prev_full_backup_set_id";
const char *const OB_STR_PREV_INC_BACKUP_SET_ID = "prev_inc_backup_set_id";
const char *const OB_STRING_MJAOR_DATA = "major_data";
const char *const OB_STRING_MINOR_DATA = "minor_data";
const char *const OB_STR_COMMENT = "comment";
const char *const OB_STR_RETRY_ID = "retry_id";
const char *const OB_STR_START_TURN_ID = "start_turn_id";
const char *const OB_STR_FILE_COUNT = "file_count";
const char *const OB_STR_DATA_TURN_ID = "data_turn_id";
const char *const OB_STR_META_TURN_ID = "meta_turn_id";
const char *const OB_STR_START_SCN = "start_scn";
const char *const OB_STR_END_SCN = "end_scn";
const char *const OB_BACKUP_STR_JOB_LEVEL = "job_level";
const char *const OB_STR_DESCRIPTION = "description";
const char *const OB_STR_CLEAN_TYPE = "type";
const char *const OB_STR_CLEAN_PARAMETER = "parameter";
const char *const OB_STR_ID = "id";
const char *const OB_STR_TOTAL_BYTES = "total_bytes";
const char *const OB_STR_DELETE_BYTES = "delete_bytes";
const char *const OB_STR_FINISH_BYTES = "finish_bytes";
const char *const OB_STR_TOTAL_FILES_COUNT = "total_files_count";
const char *const OB_STR_DELETE_FILES_COUNT = "delete_files_count";
const char *const OB_STR_TASK_COUNT = "task_count";
const char *const OB_STR_SUCCESS_TASK_COUNT = "success_task_count";
const char *const OB_STR_BACKUP_PATH = "path";
const char *const OB_STR_TOTAL_LS_COUNT = "total_ls_count";
const char *const OB_STR_MIN_RESTORE_SCN = "min_restore_scn";

const char *const OB_STR_LOG_ARCHIVE_STATUS = "log_archive_status";
const char *const OB_STR_DATA_BACKUP_DEST = "data_backup_dest";
const char *const OB_STR_BACKUP_BACKUP_DEST = "backup_backup_dest";
const char *const OB_STR_BACKUP_DEST_OPT = "backup_dest_option";
const char *const OB_STR_BACKUP_BACKUP_DEST_OPT = "backup_backup_dest_option";
const char *const OB_STR_BACKUP_CHECK_FILE = "check_file";
const char *const OB_STR_BACKUP_DEST_ENDPOINT = "endpoint";
const char *const OB_STR_BACKUP_DEST_AUTHORIZATION = "authorization";
const char *const OB_STR_BACKUP_DEST_EXTENSION = "extension";
const char *const OB_STR_BACKUP_CHECK_FILE_NAME = "check_file_name";
const char *const OB_STR_BACKUP_LAST_CHECK_TIME = "last_check_time";
const char *const OB_STR_TENANT_ID = "tenant_id";
const char *const OB_STR_LOG_ARCHIVE_ROUND = "log_archive_round";
const char *const OB_STR_ROUND_ID = "round_id"; // used in piece
const char *const OB_STR_MIN_FIRST_TIME = "min_first_time";
const char *const OB_STR_MAX_NEXT_TIME = "max_next_time";
const char *const OB_STR_STATUS = "status";
const char *const OB_STR_FILE_STATUS = "file_status";
const char *const OB_STR_BACKUP_ENCRYPTION_MODE = "encryption_mode";
const char *const OB_STR_BACKUP_PASSWD = "passwd";
const char *const OB_STR_COPY_ID = "copy_id";
const char *const OB_STR_IS_MARK_DELETED = "is_mark_deleted";
const char *const OB_STR_IS_MOUNT_FILE_CREATED = "is_mount_file_created";
const char *const OB_STR_COMPATIBLE = "compatible";
const char *const OB_STR_BACKUP_COMPATIBLE = "backup_compatible";
const char *const OB_STR_CREATE_DATE = "create_date";
const char *const OB_STR_START_TS = "start_ts";
const char *const OB_STR_LSN = "lsn";
const char *const OB_STR_MIN_LSN = "min_lsn";
const char *const OB_STR_MAX_LSN = "max_lsn";
const char *const OB_STR_ARCHIVE_FILE_ID = "file_id";
const char *const OB_STR_ARCHIVE_FILE_OFFSET = "file_offset";
const char *const OB_STR_CHECKPOINT_TS = "checkpoint_ts";
const char *const OB_STR_MAX_TS = "max_ts";
const char *const OB_STR_BACKUP_PIECE_ID = "backup_piece_id";
const char *const OB_STR_START_PIECE_ID = "start_piece_id";
const char *const OB_STR_MAX_BACKUP_PIECE_ID = "max_backup_piece_id";
const char *const OB_STR_MAX_BACKUP_PIECE_CREATE_DATE= "max_backup_create_date";
const char *const OB_STR_FULL_BACKUP = "full";
const char *const OB_STR_INC_BACKUP = "inc";
const char *const OB_STR_BACKUP_INNER_TABLE_VERSION = "inner_table_version";
const char *const OB_STR_CUR_PIECE_ID = "cur_piece_id";
const char *const OB_STR_PIECE_ID = "piece_id";
const char *const OB_STR_INPUT_BYTES = "input_bytes";
const char *const OB_STR_OUTPUT_BYTES = "output_bytes";
const char *const OB_STR_DELETED_INPUT_BYTES = "deleted_input_bytes";
const char *const OB_STR_DELETED_OUTPUT_BYTES = "deleted_output_bytes";
const char *const OB_STR_PIECE_START_TS = "piece_start_ts";
const char *const OB_STR_PIECE_SWITCH_INTERVAL = "piece_switch_interval";
const char *const OB_STR_LS_ID = "ls_id";
const char *const OB_STR_TABLET_ID = "tablet_id";
const char *const OB_STR_BORN_TS = "born_ts";
const char *const OB_STR_DEST = "dest";
const char *const OB_STR_SEVER_IP = "svr_ip";
const char *const OB_STR_SERVER_PORT = "svr_port";
const char *const OB_STR_BLACK_LIST = "black_list";
const char *const OB_STR_TABLET_COUNT = "tablet_count";
const char *const OB_STR_FINISH_TABLET_COUNT = "finish_tablet_count";
const char *const OB_STR_MACRO_BLOCK_COUNT = "macro_block_count";
const char *const OB_STR_FINISH_MACRO_BLOCK_COUNT = "finish_macro_block_count";
const char *const OB_STR_EXTRA_BYTES = "extra_bytes";
const char *const OB_STR_TENANT_COMPATIBLE = "tenant_compatible";
const char *const OB_STR_START_REPLAY_SCN = "start_replay_scn";
const char *const OB_STR_PATH = "path";
const char *const OB_STR_BACKUP_DATA_TYPE = "data_type";
const char *const OB_STR_MAX_FILE_ID = "max_file_id";
const char *const OB_STR_LOG_RESTORE_SOURCE_ID = "id";
const char *const OB_STR_LOG_RESTORE_SOURCE_TYPE = "type";
const char *const OB_STR_LOG_RESTORE_SOURCE_VALUE = "value";
const char *const OB_STR_LOG_RESTORE_SOURCE_UNTIL_SCN = "recovery_until_scn";
const char *const OB_STR_BACKUP_SKIPPED_TYPE = "skipped_type";

const char *const OB_STR_TENANT = "tenant";
const char *const OB_STR_DATA = "data";
const char *const OB_STR_CLOG = "clog";
const char *const OB_STR_BACKUP_SETS = "backup_sets";
const char *const OB_STR_BACKUP_SET = "backup_set";
const char *const OB_STR_LS = "logstream";
const char *const OB_STR_COMPLEMENT_LOG = "complement_log";
const char *const OB_STR_MAJOR_BACKUP = "major_data";
const char *const OB_STR_MINOR_BACKUP = "minor_data";
const char *const OB_STR_SYS_BACKUP = "sys_data";
const char *const OB_STR_TURN = "turn";
const char *const OB_STR_RETRY = "retry";
const char *const OB_STR_BACKUP_MACRO_BLOCK_DATA = "macro_block_data";
const char *const OB_STR_BACKUP_MACRO_RANGE_INDEX = "macro_range_index";
const char *const OB_STR_BACKUP_META_INDEX = "meta_index";
const char *const OB_STR_BACKUP_SEC_META_INDEX = "sec_meta_index";
const char *const OB_STR_INFOS = "infos";
const char *const OB_STR_DATA_INTO_TURN = "data_info_turn";
const char *const OB_STR_META_INFO_TURN = "meta_info_turn";
const char *const OB_STR_LS_META_INFO = "ls_meta_info";
const char *const OB_STR_TABLET_LOG_STREAM_INFO = "tablet_log_stream_info";
const char *const OB_STR_DELETED_TABLET_INFO = "deleted_tablet_info";
const char *const OB_STR_TENANT_MINOR_MACRO_INDEX = "tenant_minor_data_macro_range_index";
const char *const OB_STR_TENANT_MINOR_META_INDEX = "tenant_minor_data_meta_index";
const char *const OB_STR_TENANT_MINOR_SEC_META_INDEX = "tenant_minor_data_sec_meta_index";
const char *const OB_STR_TENANT_MAJOR_MACRO_INDEX = "tenant_major_data_macro_range_index";
const char *const OB_STR_TENANT_MAJOR_META_INDEX = "tenant_major_data_meta_index";
const char *const OB_STR_TENANT_MAJOR_SEC_META_INDEX = "tenant_major_data_sec_meta_index";
const char *const OB_STR_TABLET_INFO = "tablet_info";
const char *const OB_STR_LS_ATTR_INFO = "ls_attr_info";
const char *const OB_STR_META_INFO = "meta_info";
const char *const OB_STR_SINGLE_BACKUP_SET_INFO = "single_backup_set_info";
const char *const OB_STR_PIECE_INFO = "piece_info";
const char *const OB_STR_CHECKPOINT = "checkpoint";
const char *const OB_STR_PIECES = "pieces";
const char *const OB_STR_RESTORE_SCN = "restore_scn";
const char *const OB_STR_LS_COUNT = "ls_count";
const char *const OB_STR_FINISH_LS_COUNT = "finish_ls_count";
const char *const OB_STR_MACRO_BLOCK_BYTES = "major_block_bytes";
const char *const OB_STR_FINISH_MACRO_BLOCK_BYTES = "finish_major_block_bytes";
const char *const OB_STR_MINOR_BLOCK_BYTES = "minor_block_bytes";
const char *const OB_STR_FINISH_MINOR_BLOCK_BYTES = "finish_minor_block_bytes";
const char *const OB_STR_LOG_FILE_COUNT = "log_file_count";
const char *const OB_STR_FINISH_LOG_FILE_COUNT = "finish_log_file_count";

const char *const OB_STR_START_REPLAY_LSN = "start_replay_lsn";
const char *const OB_STR_LAST_REPLAY_LSN = "last_replay_lsn";
const char *const OB_STR_LAST_REPLAY_SCN = "last_replay_scn";

const char *const OB_STR_TRACE_ID = "trace_id";
const char *const OB_STR_RESTORE_TYPE = "restore_type";
const char *const OB_STR_RESTORE_TENANT_NAME = "restore_tenant_name";
const char *const OB_STR_RESTORE_TENANT_ID = "restore_tenant_id";
const char *const OB_STR_BACKUP_TENANT_ID = "backup_tenant_id";
const char *const OB_STR_START_TIME = "start_time";
const char *const OB_STR_FINISH_TIME = "finish_time";
const char *const OB_STR_ROLE = "role";
const char *const OB_STR_USER_LS_START_SCN = "user_ls_start_scn";
const char *const OB_STR_CHECKPOINT_SCN = "checkpoint_scn";
const char *const OB_STR_MAX_SCN = "max_scn";
const char *const OB_STR_MAX_TABLET_CHECKPOINT_SCN = "max_tablet_checkpoint_scn";
const char *const OB_STR_BASE_PIECE_ID = "base_piece_id";
const char *const OB_STR_USED_PIECE_ID = "used_piece_id";
const char *const OB_STR_BASE_PIECE_SCN = "base_piece_scn";
const char *const OB_STR_BASE_SCN = "base_scn";

const char *const OB_STR_FROZEN_INPUT_BYTES = "frozen_input_bytes";
const char *const OB_STR_FROZEN_OUTPUT_BYTES = "frozen_output_bytes";
const char *const OB_STR_ACTIVE_INPUT_BYTES = "active_input_bytes";
const char *const OB_STR_ACTIVE_OUTPUT_BYTES = "active_output_bytes";

const char *const OB_STR_CREATE_TIMESTAMP = "create_timestamp";
const char *const OB_STR_CP_FILE_ID = "cp_file_id";
const char *const OB_STR_CP_FILE_OFFSET = "cp_file_offset";

const char *const OB_BACKUP_ENCRYPTION_MODE_SESSION_STR = "__ob_backup_encryption_mode__";
const char *const OB_BACKUP_ENCRYPTION_PASSWD_SESSION_STR = "__ob_backup_encryption_passwd__";
const char *const OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR = "__ob_backup_decryption_passwd_array__";

const char *const OB_RESTORE_SOURCE_NAME_SESSION_STR = "__ob_restore_source_name__";
const char *const OB_RESTORE_PREVIEW_TENANT_ID_SESSION_STR = "__ob_restore_preview_tenant_id__";
const char *const OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR = "__ob_restore_preview_backup_dest__";
const char *const OB_RESTORE_PREVIEW_SCN_SESSION_STR = "__ob_restore_preview_scn__";
const char *const OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR = "__ob_restore_preview_timestamp__";
const char *const OB_RESTORE_PREVIEW_BACKUP_CLUSTER_NAME_SESSION_STR = "__ob_restore_preview_backup_cluster_name__";
const char *const OB_RESTORE_PREVIEW_BACKUP_CLUSTER_ID_SESSION_STR = "__ob_restore_preview_backup_cluster_id__";
const char *const MULTI_BACKUP_SET_PATH_PREFIX = "BACKUPSET";
const char *const MULTI_BACKUP_PIECE_PATH_PREFIX = "BACKUPPIECE";

const char *const ENCRYPT_KEY = "encrypt_key=";
const char *const OB_STR_INITIATOR_JOB_ID = "initiator_job_id";
const char *const OB_STR_EXECUTOR_TENANT_ID = "executor_tenant_id";
const char *const OB_STR_DEST_ID = "dest_id";
const char *const OB_STR_DEST_NO = "dest_no";
const char *const OB_STR_SUCCESS = "SUCCESS";
const char *const OB_STR_AVAILABLE = "AVAILABLE";
const char *const OB_STR_OPTIONAL = "optional";
const char *const OB_STR_MANDATORY = "mandatory";
const char *const OB_STR_LOCATION = "location";
const char *const OB_STR_SERVICE = "service";
const char *const OB_STR_USER = "user";
const char *const OB_STR_PASSWORD = "password";
const char *const OB_STR_IP_LIST = "ip_list";
const char *const OB_STR_CLUSTER_ID = "cluster_id";
const char *const OB_COMPATIBILITY_MODE = "compatibility_mode";
const char *const OB_STR_IS_ENCRYPTED = "is_encrypted";
const char *const OB_STR_LAG_TARGET = "lag_target";
const char *const OB_STR_COMPRESSION = "compression";
const char *const OB_STR_BINDING = "binding";
const char *const OB_STR_STATE = "state";
const char *const OB_STR_ENABLE = "enable";
const char *const OB_STR_DISABLE = "disable";

const char *const OB_STR_POLICY_NAME = "policy_name";
const char *const OB_STR_RECOVERY_WINDOW = "recovery_window";
const char *const OB_STR_REDUNDANACY = "redundancy";
const char *const OB_STR_BACKUP_COPIES = "backup_copies";
const char *const OB_STR_TENANT_BACKUP_SET_INFOS = "tenant_backup_set_infos";
const char *const OB_STR_FORMAT_FILE_NAME = "format";
const char *const OB_STR_DIAGNOSE_INFO = "diagnose_info";
const char *const OB_STR_LOCALITY_INFO = "locality_info";
const char *const OB_STR_DEST_TYPE = "dest_type";
const char *const OB_STR_RETRY_COUNT = "retry_count";
const char *const OB_STR_DELETED = "DELETED";

const char *const OB_STR_BACKUP_SET_LIST = "backup_set_list";
const char *const OB_STR_BACKUP_PIECE_LIST = "backup_piece_list";
const char *const OB_STR_LOG_PATH_LIST = "log_path_list";
const char *const OB_STR_LS_META_INFOS = "ls_meta_infos";
const char *const OB_STR_TRANSFER_SEQ = "transfer_seq";
const char *const OB_STR_TRANSFER_SRC_LS_ID = "transfer_src_ls_id";
const char *const OB_STR_TRANSFER_SRC_SEQ = "transfer_src_seq";
const char *const OB_STR_TRANSFER_DEST_LS_ID = "transfer_dest_ls_id";
const char *const OB_STR_TRANSFER_DEST_SEQ = "transfer_dest_seq";
const char *const OB_STR_BACKUP_LS_ID = "backup_ls_id";
const char *const OB_STR_MINOR_TURN_ID = "minor_turn_id";
const char *const OB_STR_MAJOR_TURN_ID = "major_turn_id";
const char *const OB_STR_CONSISTENT_SCN = "consistent_scn";
const char *const OB_STR_ROOT_KEY = "root_key";
const char *const OB_STR_BACKUP_DATA_VERSION = "backup_data_version";
const char *const OB_STR_CLUSTER_VERSION = "cluster_version";
const char *const OB_BACKUP_SUFFIX=".obbak";
const char *const OB_ARCHIVE_SUFFIX=".obarc";
const char *const OB_STR_MIN_RESTORE_SCN_DISPLAY = "min_restore_scn_display";
const char *const OB_STR_CHECKPOINT_FILE_NAME = "checkpoint_info";
const char *const OB_STR_SRC_TENANT_NAME = "src_tenant_name";
const char *const OB_STR_AUX_TENANT_NAME = "aux_tenant_name";
const char *const OB_STR_TARGET_TENANT_NAME = "target_tenant_name";
const char *const OB_STR_TARGET_TENANT_ID = "target_tenant_id";
const char *const OB_STR_TABLE_LIST = "table_list";
const char *const OB_STR_TABLE_LIST_META_INFO = "table_list_meta_info";

enum ObBackupFileType
{
  BACKUP_META = 0,
  BACKUP_MACRO_DATA = 1,
  BACKUP_META_INDEX = 2,
  BACKUP_MACRO_DATA_INDEX = 3,
  BACKUP_SSTABLE_MACRO_INDEX = 4, //not used
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
  BACKUP_ARCHIVE_ROUND_START_INFO = 18, // round start placeholder
  BACKUP_ARCHIVE_ROUND_END_INFO = 19, // round end placeholder
  BACKUP_ARCHIVE_SINGLE_PIECE_INFO = 20, // single piece info
  BACKUP_ARCHIVE_PIECE_START_INFO = 21, // piece start placeholder
  BACKUP_LS_INFO = 22,
  BACKUP_TABLET_TO_LS_INFO = 23,
  BACKUP_DATA_PLACEHOLDER = 24,
  BACKUP_LS_STATISTICS_INFO = 25,
  BACKUP_ARCHIVE_CHECKPOINT = 26, // piece checkpoint file
  BACKUP_TENANT_SET_INFOS = 27,
  BACKUP_ARCHIVE_PIECE_END_INFO = 28, // piece end placeholder
  BACKUP_FORMAT_FILE = 29, // format file
  BACKUP_PIECE_INNER_PLACEHOLDER_INFO = 30, // piece inner piece placeholder
  BACKUP_PIECE_FILE_LIST_INFO = 31, // complete ls file list
  BACKUP_PIECE_SINGLE_LS_FILE_LIST_INFO = 32, // single ls file list
  BACKUP_CHECK_FILE = 33,
  BACKUP_LS_META_INFOS_FILE = 34,
  BACKUP_TENANT_ARCHIVE_PIECE_INFOS = 35,
  BACKUP_DELETED_TABLET_INFO = 36,
  BACKUP_TABLET_METAS_INFO = 37,
  BACKUP_TABLE_LIST_FILE = 38,
  BACKUP_TABLE_LIST_META_FILE = 39,
  // type <=255 is write header struct to disk directly
  // type > 255 is use serialization to disk
  BACKUP_MAX_DIRECT_WRITE_TYPE = 255,
  BACKUP_ARCHIVE_BLOCK_META = 0x4142, // 16706
  BACKUP_ARCHIVE_INDEX_FILE = 0x4149, // 16713 AI means ARCHIVE INDEX
  BACKUP_ARCHIVE_KEY_FILE = 0x414B, // 16713 AK means ARCHIVE  KEY
  BACKUP_TYPE_MAX
};

struct ObBackupSkippedType final
{
public:
  enum TYPE : uint8_t
  {
    DELETED = 0,
    TRANSFER = 1,
    MAX_TYPE
  };
public:
  ObBackupSkippedType() : type_(MAX_TYPE) {}
  ~ObBackupSkippedType() = default;
  explicit ObBackupSkippedType(const TYPE &type) : type_(type) {}

  bool is_valid() const { return DELETED <= type_ && type_ < MAX_TYPE; }
  void reset() { type_ = MAX_TYPE; }
  const char *str() const;
  int parse_from_str(const ObString &str);

  TO_STRING_KV(K_(type), "type", str());
private:
  TYPE type_;
};

enum ObBackupMetaType
{
  PARTITION_GROUP_META = 0,
  PARTITION_META = 1,
  SSTABLE_METAS = 2,
  TABLE_KEYS = 3,
  PARTITION_GROUP_META_INFO = 4,
  META_TYPE_MAX
};

typedef common::ObFixedLengthString<OB_BACKUP_DEFAULT_FIXED_STR_LEN> ObBackupDefaultFixedLenString;
typedef common::ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> ObBackupPathString;

typedef ObBackupPathString ObBackupSetPath;
typedef ObBackupPathString ObBackupPiecePath;
typedef ObBackupPathString ObBackupDescription;

struct ObBackupFileStatus final
{
  enum STATUS
  {
    BACKUP_FILE_AVAILABLE = 0,
    BACKUP_FILE_COPYING = 1,
    BACKUP_FILE_INCOMPLETE = 2,
    BACKUP_FILE_DELETING = 3,
    BACKUP_FILE_EXPIRED = 4,
    BACKUP_FILE_BROKEN = 5,
    BACKUP_FILE_DELETED = 6,
    BACKUP_FILE_MAX
  };

  static const char *get_str(const STATUS &status);
  static STATUS get_status(const char *status_str);
  static OB_INLINE bool is_valid(const STATUS &status) { return status >= 0 && status < BACKUP_FILE_MAX; }
  static int check_can_change_status(
      const ObBackupFileStatus::STATUS &src_file_status,
      const ObBackupFileStatus::STATUS &dest_file_status);
  static bool can_show_in_preview(const ObBackupFileStatus::STATUS &status);
};

struct ObBackupType final
{
  OB_UNIS_VERSION(1);
public:
  enum BackupType
  {
    EMPTY = 0,
    FULL_BACKUP = 1,
    INCREMENTAL_BACKUP = 2,
    MAX,
  };

  ObBackupType(): type_(ObBackupType::EMPTY) {}
  virtual ~ObBackupType() = default;
  void reset() { type_ = EMPTY; }
  bool is_valid() const { return type_ >= FULL_BACKUP && type_ < MAX; }
  const char* get_backup_type_str() const;
  int set_backup_type(const char *buf);
  static OB_INLINE bool is_full_backup(const BackupType &type) { return FULL_BACKUP == type; }
  bool is_full_backup() const { return ObBackupType::is_full_backup(type_); }
  bool is_inc_backup() const { return type_ == BackupType::INCREMENTAL_BACKUP; }
  TO_STRING_KV(K_(type));
  BackupType type_;
};

struct ObBackupSetDesc {
  ObBackupSetDesc();
  int assign(const ObBackupSetDesc &that);
  bool is_valid() const;
  bool operator==(const ObBackupSetDesc &other) const;
  void reset();

  TO_STRING_KV(K_(backup_set_id), K_(backup_type), K_(min_restore_scn), K_(total_bytes));
  int64_t backup_set_id_;
  ObBackupType backup_type_;  // FULL OR INC
  share::SCN min_restore_scn_;
  int64_t total_bytes_;
};

struct ObRestoreBackupSetBriefInfo final
{
public:
  ObRestoreBackupSetBriefInfo(): backup_set_path_(), backup_set_desc_(){}
  ~ObRestoreBackupSetBriefInfo() {}
  void reset() { backup_set_path_.reset(); }
  bool is_valid() const { return !backup_set_path_.is_empty(); }
  int assign(const ObRestoreBackupSetBriefInfo &that);
  int get_restore_backup_set_brief_info_str(common::ObIAllocator &allocator, common::ObString &str) const;
  TO_STRING_KV(K_(backup_set_path), K_(backup_set_desc));
  share::ObBackupSetPath backup_set_path_;
  share::ObBackupSetDesc backup_set_desc_;
};

struct ObRestoreLogPieceBriefInfo final
{
public:
  ObRestoreLogPieceBriefInfo(): piece_path_(), piece_id_(0), start_scn_(), checkpoint_scn_() {}
  ~ObRestoreLogPieceBriefInfo() {}
  void reset() { piece_path_.reset(); }
  bool is_valid() const { return !piece_path_.is_empty(); }
  int get_restore_log_piece_brief_info_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int assign(const ObRestoreLogPieceBriefInfo &that);
  TO_STRING_KV(K_(piece_path), K_(piece_id), K_(start_scn), K_(checkpoint_scn));
  share::ObBackupPiecePath piece_path_;
  int64_t piece_id_;
  share::SCN start_scn_;
  share::SCN checkpoint_scn_;
};

class ObBackupStorageInfo;
class ObPhysicalRestoreBackupDestList
{
  OB_UNIS_VERSION(1);
public:
  ObPhysicalRestoreBackupDestList();
  virtual ~ObPhysicalRestoreBackupDestList();

  int assign(const ObPhysicalRestoreBackupDestList &list);
  int set(const common::ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list,
      const common::ObIArray<share::ObBackupPiecePath> &backup_piece_list,
      const common::ObIArray<share::ObBackupPathString> &log_path_list);
  void reset();

  int get_backup_set_list_format_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  int get_backup_set_desc_list_format_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  int get_backup_piece_list_format_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;
  int get_log_path_list_format_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  int backup_set_list_assign_with_format_str(const common::ObString &str);
  int backup_set_desc_list_assign_with_format_str(const common::ObString &str);
  int backup_piece_list_assign_with_format_str(const common::ObString &str);
  int log_path_list_assign_with_format_str(const common::ObString &str);

  int backup_set_list_assign_with_hex_str(const common::ObString &str);
  int backup_piece_list_assign_with_hex_str(const common::ObString &str);
  int log_path_list_assign_with_hex_str(const common::ObString &str);

  int get_backup_set_list_hex_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;
  int get_backup_piece_list_hex_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;
  int get_log_path_list_hex_str(
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  int get_backup_set_brief_info_list(common::ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list);
  common::ObSArray<share::ObBackupSetPath> &get_backup_set_path_list() { return backup_set_path_list_; }
  common::ObSArray<share::ObBackupPiecePath> &get_backup_piece_path_list() { return backup_piece_path_list_; }
  common::ObSArray<share::ObBackupPathString> &get_log_path_list() { return log_path_list_; }
  const common::ObSArray<share::ObBackupSetPath> &get_backup_set_path_list() const { return backup_set_path_list_; }
  const common::ObSArray<share::ObBackupPiecePath> &get_backup_piece_path_list() const { return backup_piece_path_list_; }
  const common::ObSArray<share::ObBackupPathString> &get_log_path_list() const { return log_path_list_; }
  bool is_compat_backup_path() const { return backup_set_path_list_.empty(); }

  TO_STRING_KV(K_(backup_set_path_list), K_(backup_piece_path_list), K_(log_path_list));

private:
  int64_t get_backup_set_list_format_str_len() const;
  int64_t get_backup_piece_list_format_str_len() const;
  int64_t get_log_path_list_format_str_len() const;

private:
  common::ObArenaAllocator allocator_;
  common::ObSArray<share::ObBackupSetPath> backup_set_path_list_;
  common::ObSArray<share::ObBackupSetDesc> backup_set_desc_list_;
  common::ObSArray<share::ObBackupPiecePath> backup_piece_path_list_;
  common::ObSArray<share::ObBackupPathString> log_path_list_; // for add log source, may be empty if restore is lauched by add restore source.
};

struct ObBackupCommonHeader
{
  static const uint8_t COMMON_HEADER_VERSION = 1;
  static const uint8_t MACRO_DATA_HEADER_VERSION = 1;
  ObBackupCommonHeader();
  void reset();
  void set_header_checksum();
  int set_checksum(const char *buf, const int64_t len);
  int check_header_checksum() const;
  int16_t calc_header_checksum() const;
  int check_valid() const;
  bool is_compresssed_data() const;
  int check_data_checksum(const char *buf, const int64_t len) const;
  int assign(const ObBackupCommonHeader &header);
  TO_STRING_KV(
      K_(header_version),
      K_(compressor_type),
      K_(data_type),
      K_(data_version),
      K_(header_length),
      K_(header_checksum),
      K_(data_length),
      K_(data_zlength),
      K_(data_checksum),
      K_(align_length));
  uint16_t data_type_;
  uint16_t header_version_;        // header version
  uint16_t data_version_;
  uint16_t compressor_type_;       // data compression type
  int16_t header_length_;         // = sizeof(ObBackupCommonHeader)
  int16_t header_checksum_;
  int64_t data_length_;           // length of data before compression
  int64_t data_zlength_;          // length of data after compression
  int64_t data_checksum_;         // checksum of data part
  int64_t align_length_;
};

struct ObLogArchiveStatus final
{
  enum STATUS
  {
    INVALID = 0,
    STOP = 1,
    BEGINNING = 2,
    DOING = 3,
    STOPPING = 4,
    INTERRUPTED = 5,
    MIXED = 6,
    PAUSED = 7, // only used for backup backup log archive
    MAX
  };

  static const char *get_str(const STATUS &status);
  static STATUS get_status(const char *status_str);
  static OB_INLINE bool is_valid(const STATUS &status) { return status >= 0 && status < MAX; }
};

struct ObBackupPieceInfoKey;
struct ObTenantLogArchiveStatus final
{
  enum COMPATIBLE : int64_t
  {
    NONE = 0,
    COMPATIBLE_VERSION_1 = 1,
    COMPATIBLE_VERSION_2 = 2,
    MAX
  };

  OB_UNIS_VERSION(1);
public:
  ObTenantLogArchiveStatus();
  void reset();
  bool is_valid() const;
  static bool is_compatible_valid(COMPATIBLE compatible);
  int update(const ObTenantLogArchiveStatus &new_status);
  int get_piece_key(ObBackupPieceInfoKey &key) const;
  bool need_switch_piece() const;

  uint64_t tenant_id_;
  int64_t copy_id_;
  int64_t start_ts_;
  int64_t checkpoint_ts_;
  int64_t incarnation_;
  int64_t round_;
  ObLogArchiveStatus::STATUS status_;
  bool is_mark_deleted_;
  bool is_mount_file_created_;// used to check if backup dest is mount properly
  COMPATIBLE compatible_;
  int64_t backup_piece_id_;
  int64_t start_piece_id_;
  TO_STRING_KV(K_(tenant_id), K_(copy_id), K_(start_ts), K_(checkpoint_ts), K_(status),
      K_(incarnation), K_(round), "status_str", ObLogArchiveStatus::get_str(status_),
      K_(is_mark_deleted), K_(is_mount_file_created), K_(compatible), K_(backup_piece_id),
      K_(start_piece_id));
private:
  int update_stop_(const ObTenantLogArchiveStatus &new_status);
  int update_beginning_(const ObTenantLogArchiveStatus &new_status);
  int update_doing_(const ObTenantLogArchiveStatus &new_status);
  int update_stopping_(const ObTenantLogArchiveStatus &new_status);
  int update_interrupted_(const ObTenantLogArchiveStatus &new_status);
  int update_paused_(const ObTenantLogArchiveStatus &new_status);
};

struct ObBackupDest;

struct ObLogArchiveBackupInfo final
{
  ObLogArchiveBackupInfo();

  ObTenantLogArchiveStatus status_;
  char backup_dest_[OB_MAX_BACKUP_DEST_LENGTH];

  void reset();
  bool is_valid() const;
  bool is_same(const ObLogArchiveBackupInfo &other) const;
  int get_piece_key(ObBackupPieceInfoKey &key) const;
  bool is_oss() const;
  int get_backup_dest(ObBackupDest &backup_dest) const;
  TO_STRING_KV(K_(status), K_(backup_dest));
};

struct ObBackupPieceInfoKey
{
  int64_t incarnation_;
  uint64_t tenant_id_;
  int64_t round_id_;
  int64_t backup_piece_id_; // 0 means piece not swtich in one round
  int64_t copy_id_;

  ObBackupPieceInfoKey();
  // ObBackupPieceInfoKey is serialized in ObBackupPieceInfo
  bool is_valid() const;
  void reset();
  bool operator==(const ObBackupPieceInfoKey &o) const;
  bool operator < (const  ObBackupPieceInfoKey &o) const;
  TO_STRING_KV(K_(incarnation), K_(tenant_id), K_(round_id), K_(backup_piece_id), K_(copy_id));
};

// the first piece of a log archive round, init state is ACTIVE
// freeze piece step:
// 1. Change cur as previous and its state as FREEZING; Create new piece with ACTIVE state
// 2. RS check if all observers are using new piece. If true, change the previous piece state as FROZEN.
struct ObBackupPieceStatus final
{
  enum STATUS
  {
    BACKUP_PIECE_ACTIVE = 0,
    BACKUP_PIECE_FREEZING = 1,
    BACKUP_PIECE_FROZEN = 2,
    BACKUP_PIECE_INACTIVE = 3,
    BACKUP_PIECE_MAX
  };

  static const char *get_str(const STATUS &status);
  static STATUS get_status(const char *status_str);
  static OB_INLINE bool is_valid(const STATUS &status) { return status >= 0 && status < BACKUP_PIECE_MAX; }
};

struct ObBackupPieceInfo
{
  OB_UNIS_VERSION(1);
public:
  ObBackupPieceInfoKey key_;
  int64_t create_date_;
  int64_t start_ts_; // filled by backup round start or previous piece frozen.
  int64_t checkpoint_ts_; // filled by trigger freeze piece
  int64_t max_ts_; // filled by frozen
  ObBackupPieceStatus::STATUS status_;
  ObBackupFileStatus::STATUS file_status_;
  ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> backup_dest_;
  ObTenantLogArchiveStatus::COMPATIBLE compatible_;
  int64_t start_piece_id_;

  ObBackupPieceInfo();
  bool is_valid() const;
  void reset();
  int get_backup_piece_path(char *buf, const int64_t buf_len) const;
  int init_piece_info(const ObBackupPieceInfo &sys_piece, const uint64_t tenant_id);
  const char *get_status_str() const { return ObBackupPieceStatus::get_str(status_); }
  const char *get_file_status_str() const { return ObBackupFileStatus::get_str(file_status_); }
  int get_backup_dest(ObBackupDest &backup_dest) const;
  bool operator==(const ObBackupPieceInfo &o) const;
  bool operator!=(const ObBackupPieceInfo &o) const { return !(operator ==(o)); }
  TO_STRING_KV(K_(key), K_(create_date), K_(status), K_(file_status), K_(start_ts),
      K_(checkpoint_ts), K_(max_ts), K_(backup_dest), "status_str", get_status_str(),
      "file_status_str", get_file_status_str(), K_(compatible), K_(start_piece_id));
};

struct ObNonFrozenBackupPieceInfo final
{
  bool has_prev_piece_info_;
  ObBackupPieceInfo prev_piece_info_;// is valid only when is_freezing_ is true
  ObBackupPieceInfo cur_piece_info_;

  ObNonFrozenBackupPieceInfo();
  void reset();
  bool is_valid() const;
  int get_backup_piece_id(int64_t &active_piece_id) const;
  int get_backup_piece_info(int64_t &active_piece_id, int64_t &active_piece_create_date) const;
  DECLARE_TO_STRING;
};

class ObBackupStorageInfo : public common::ObObjectStorageInfo
{
public:
  using common::ObObjectStorageInfo::set;

public:
  ObBackupStorageInfo() {}
  virtual ~ObBackupStorageInfo();

  int set(
      const common::ObStorageType device_type,
      const char *endpoint,
      const char *authorization,
      const char *extension);
  int get_authorization_info(char *authorization, const int64_t length) const;

private:
#ifdef OB_BUILD_TDE_SECURITY
  virtual int get_access_key_(char *key_buf, const int64_t key_buf_len) const override;
  virtual int parse_storage_info_(const char *storage_info, bool &has_needed_extension) override;
  int encrypt_access_key_(char *encrypt_key, const int64_t length) const;
  int decrypt_access_key_(const char *buf);
#endif
};

class ObBackupDest final
{
public:
  ObBackupDest();
  ~ObBackupDest();
  int set(const char *backup_dest);
  int set(const common::ObString &backup_dest);
  int set(const ObBackupPathString &backup_dest);
  int set(
      const char *path,
      const char *endpoint,
      const char *authorization,
      const char *extension);
  int set(const char *root_path, const char *storage_info);
  int set(const char *root_path, const ObBackupStorageInfo *storage_info);
  int set_without_decryption(const common::ObString &backup_dest);
  void reset();
  bool is_valid() const;
  bool is_root_path_equal(const ObBackupDest &backup_dest) const;
  int is_backup_path_equal(const ObBackupDest &backup_dest, bool &is_equal) const;
  bool is_storage_type_s3(){ return OB_ISNULL(storage_info_) ? false : ObStorageType::OB_STORAGE_S3 == storage_info_->get_type(); }
  int get_backup_dest_str(char *buf, const int64_t buf_size) const;
  int get_backup_dest_str_with_primary_attr(char *buf, const int64_t buf_size) const;
  int get_backup_path_str(char *buf, const int64_t buf_size) const;
  common::ObString get_root_path() const { return root_path_;}
  share::ObBackupStorageInfo *get_storage_info() const { return storage_info_;}
  bool operator ==(const ObBackupDest &backup_dest) const;
  bool operator !=(const ObBackupDest &backup_dest) const;
  int deep_copy(const ObBackupDest &backup_dest);
  int64_t hash() const;
  DECLARE_TO_STRING;

private:
  int alloc_and_init();
  int parse_backup_dest_str_(const char *backup_dest);
  void root_path_trim_();

  char *root_path_;
  share::ObBackupStorageInfo *storage_info_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDest);
};

struct ObBackupInfoStatus final
{
  enum BackupStatus
  {
    STOP = 0,
    PREPARE = 1,
    SCHEDULE = 2,
    DOING = 3,
    CANCEL = 4,
    CLEANUP = 5,
    MAX,
  };
  ObBackupInfoStatus() : status_(MAX) {}
  virtual ~ObBackupInfoStatus() = default;
  void reset() { status_ = MAX; }
  bool is_valid() const { return status_ >= 0 && status_ < MAX; }
  static const char *get_status_str(const BackupStatus &status);
  const char* get_info_backup_status_str() const;
  int set_info_backup_status(const char *buf);
  bool is_stop_status() const { return STOP == status_; }
  bool is_prepare_status() const { return PREPARE == status_; }
  bool is_scheduler_status() const { return SCHEDULE == status_; }
  bool is_doing_status() const { return DOING == status_; }
  bool is_cleanup_status() const { return CLEANUP == status_; }
  bool is_cancel_status() const { return CANCEL == status_; }
  void set_backup_status_scheduler() { status_ = SCHEDULE; }
  void set_backup_status_doing() { status_ = DOING; }
  void set_backup_status_cleanup() { status_ = CLEANUP; }
  void set_backup_status_stop() { status_ = STOP; }
  void set_backup_status_cancel() { status_ = CANCEL; }
  TO_STRING_KV(K_(status), "status_str", get_status_str(status_));
  BackupStatus status_;
};

struct ObBaseBackupInfoStruct
{
public:
  typedef common::ObFixedLengthString<common::OB_MAX_URI_LENGTH> BackupDest;
  ObBaseBackupInfoStruct();
  virtual ~ObBaseBackupInfoStruct() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObBaseBackupInfoStruct &backup_info_struct);
  bool has_cleaned() const;
  ObBaseBackupInfoStruct &operator =(const ObBaseBackupInfoStruct &info);
  int check_backup_info_match(const ObBaseBackupInfoStruct &info) const;

  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(incarnation), K_(backup_dest),
      K_(backup_backup_dest), K_(backup_snapshot_version), K_(backup_schema_version),
      K_(backup_data_version), K_(detected_backup_region), K_(backup_type),
      K_(backup_status), K_(backup_task_id), K_(encryption_mode));

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
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
};

class ObBackupPath;
class ObBackupSetTaskAttr;
class ObTenantArchiveRoundAttr;
class ObBackupUtils
{
public:
  ObBackupUtils() {}
  virtual ~ObBackupUtils() {}
  static int get_backup_info_default_timeout_ctx(common::ObTimeoutCtx &ctx);
  static bool is_need_retry_error(const int err);
  //format input string split with ',' or ';'
  template<class T>
  static int parse_backup_format_input(
      const ObString &format_input,
      const int64_t max_length,
      common::ObIArray<T> &array);
  static int convert_timestamp_to_date(
      const int64_t snapshot_version,
      int64_t &date);
  static int convert_timestamp_to_timestr(const int64_t ts, char *buf, int64_t len);
  static bool can_backup_pieces_be_deleted(const ObBackupPieceStatus::STATUS &status);
  static int check_passwd(const char *passwd_array, const char *passwd);
  static int check_is_tmp_file(const common::ObString &file_name, bool &is_tmp_file);
  static int get_backup_scn(const uint64_t &tenant_id, share::SCN &scn);
  static int check_tenant_data_version_match(const uint64_t tenant_id, const uint64_t data_version);
  static int get_full_replica_num(const uint64_t tenant_id, int64_t &replica_num);
  static int backup_scn_to_str(const uint64_t tenant_id, const share::SCN &scn, char *buf, int64_t buf_len);
  static int get_tenant_sys_time_zone_wrap(const uint64_t tenant_id,
                                           ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH> &time_zone,
                                           ObTimeZoneInfoWrap &time_zone_info_wrap);
private:
  static const int64_t  RETRY_INTERVAL = 10 * 1000 * 1000;
  static const int64_t  MAX_RETRY_TIMES = 3;
};

struct ObPhysicalRestoreInfo final
{
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
  int64_t cluster_version_;          //restore tenant cluster version
  ObPhysicalRestoreBackupDestList multi_restore_path_list_;
  int64_t backup_date_;  //备份的位点，RS只允许从这个位点之后的位点恢复

  common::ObSArray<share::ObBackupSetPath> &get_backup_set_path_list() { return multi_restore_path_list_.get_backup_set_path_list(); }
  common::ObSArray<share::ObBackupPiecePath> &get_backup_piece_path_list() { return multi_restore_path_list_.get_backup_piece_path_list(); }
  const common::ObSArray<share::ObBackupSetPath> &get_backup_set_path_list() const { return multi_restore_path_list_.get_backup_set_path_list(); }
  const common::ObSArray<share::ObBackupPiecePath> &get_backup_piece_path_list() const { return multi_restore_path_list_.get_backup_piece_path_list(); }
  bool is_compat_backup_path() const { return multi_restore_path_list_.is_compat_backup_path(); }

  ObPhysicalRestoreInfo();
  ~ObPhysicalRestoreInfo() {destroy();}
  void destroy();
  bool is_valid() const;
  void set_array_label(const char *lable);
  int assign(const ObPhysicalRestoreInfo &other);
  TO_STRING_KV(K_(backup_dest), K_(cluster_name), K_(cluster_id), K_(incarnation),
               K_(tenant_id), K_(full_backup_set_id), K_(inc_backup_set_id),
               K_(log_archive_round), K_(restore_snapshot_version), K_(restore_start_ts),
               K_(compatible), K_(cluster_version), K_(backup_date), K_(multi_restore_path_list));
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreInfo);
};

struct ObBackupDataType final
{
  OB_UNIS_VERSION(1);
public:
  // TODO(yanfeng): change this comment when quick_restore branch merge
  // backup sys: ls inner tablet, the granularity of success is log stream level
  // backup minor: mini/minor/ddl/mds sstable, the granularity of success is tablet level
  // backup major: major sstable, the granularity of success is macro block level
  enum BackupDataType
  {
    BACKUP_SYS = 0,
    BACKUP_MINOR = 1,
    BACKUP_MAJOR = 2,
    MAX,
  };

  ObBackupDataType(): type_(MAX) {}
  virtual ~ObBackupDataType() = default;
  void reset() { type_ = MAX; }
  bool is_valid() const { return type_ >= BACKUP_SYS && type_ < MAX; }
  bool operator==(const ObBackupDataType &other) const { return other.type_ == type_; }
  static OB_INLINE bool is_major_backup(const BackupDataType &type) { return BACKUP_MAJOR == type; }
  static OB_INLINE bool is_minor_backup(const BackupDataType &type) { return BACKUP_MINOR == type; }
  static OB_INLINE bool is_sys_backup(const BackupDataType &type) { return BACKUP_SYS == type; }
  bool is_major_backup() const { return ObBackupDataType::is_major_backup(type_); }
  bool is_minor_backup() const { return ObBackupDataType::is_minor_backup(type_); }
  bool is_sys_backup() const { return ObBackupDataType::is_sys_backup(type_); }
  void set_major_data_backup() { type_ = BACKUP_MAJOR; }
  void set_minor_data_backup() { type_ = BACKUP_MINOR; }
  void set_sys_data_backup() { type_ = BACKUP_SYS; }

  TO_STRING_KV(K_(type));
  BackupDataType type_;
};

struct ObBackupRegion
{
  ObBackupRegion();
  virtual ~ObBackupRegion();
  void reset();
  int set(const ObString &region, const int64_t priority);

  bool is_valid() const { return !region_.is_empty() && priority_ >= 0; }
  TO_STRING_KV(K_(region), K_(priority));
  ObRegion region_;
  int64_t priority_;
};

struct ObBackupZone
{
  ObBackupZone();
  virtual ~ObBackupZone();
  void reset();
  int set(const ObString &zone, const int64_t priority);

  bool is_valid() const { return !zone_.is_empty() && priority_ >= 0; }
  TO_STRING_KV(K_(zone), K_(priority));
  ObZone zone_;
  int64_t priority_;
};

//-----------------------------ObBackupUtils---------------------------
template<class T>
int ObBackupUtils::parse_backup_format_input(
    const ObString &format_input,
    const int64_t max_length,
    ObIArray<T> &array)
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
    //do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < format_input.length(); ++i) {

      if (format_input.ptr()[i] == split_commma || format_input.ptr()[i] == split_semicolon) {
        length = i - pos;
        if (length <= 0 || length > max_length || length > INT32_MAX) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "format input value is unexpcted", K(ret), K(format_input), K(length), K(max_length));
        } else {
          ObString tmp_string;
          object.reset();
          tmp_string.assign_ptr(format_input.ptr() + pos, static_cast<int32_t>(length));
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
        tmp_string.assign_ptr(format_input.ptr() + pos, static_cast<int32_t>(length));
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

// 4.0 backup
struct ObBackupStatus final
{
  OB_UNIS_VERSION(1);
public:
  enum Status
  {
    INIT = 0,
    DOING = 1,
    COMPLETED = 2,
    FAILED = 3,
    CANCELING= 4,
    CANCELED = 5,
    BACKUP_SYS_META = 6,
    BACKUP_USER_META = 7,
    BACKUP_META_FINISH = 8,
    BACKUP_DATA_SYS = 9,
    BACKUP_DATA_MINOR = 10,
    BACKUP_DATA_MAJOR = 11,
    BEFORE_BACKUP_LOG = 12,
    BACKUP_LOG = 13,
    MAX_STATUS
  };
  ObBackupStatus(): status_(MAX_STATUS) {}
  ObBackupStatus(const Status &status): status_(status) {}
  virtual ~ObBackupStatus() = default;
  ObBackupStatus &operator=(const Status &status);
  operator Status() const { return status_; }
  bool is_valid() const;

  bool is_backup_meta() const { return BACKUP_SYS_META == status_ || BACKUP_USER_META == status_; }
  bool is_backup_major() const { return BACKUP_DATA_MAJOR == status_; }
  bool is_backup_minor() const { return BACKUP_DATA_MINOR == status_; }
  bool is_backup_log() const { return BEFORE_BACKUP_LOG == status_ || BACKUP_LOG == status_; }
  bool is_backup_sys() const { return BACKUP_DATA_SYS == status_; }
  bool is_backup_finish() const { return COMPLETED == status_ || FAILED == status_ || CANCELED == status_; }
  const char* get_str() const;
  int set_status(const char *str);
  int get_backup_data_type(share::ObBackupDataType &backup_data_type) const;
  TO_STRING_KV(K_(status));
  Status status_;
};

struct ObBackupTaskStatus final
{
  enum Status
  {
    INIT = 0,
    PENDING = 1,
    DOING = 2,
    FINISH = 3,
    MAX_STATUS
  };
  ObBackupTaskStatus(): status_(Status::MAX_STATUS) {}
  ObBackupTaskStatus(const Status &status): status_(status) {}
  virtual ~ObBackupTaskStatus() = default;
  bool is_valid() const;
  const char* get_str() const;
  int set_status(const char *str);
  bool is_init() const { return Status::INIT == status_; }
  bool is_pending() const { return Status::PENDING == status_; }
  bool is_doing() const { return Status::DOING == status_; }
  bool is_finish() const { return Status::FINISH == status_; }
  TO_STRING_KV(K_(status));
  Status status_;
};

struct ObBackupServer final
{
  ObBackupServer();
  virtual ~ObBackupServer();
  void reset();
  int assign(const ObBackupServer &that);
  int set(const ObAddr &server, const int64_t priority);
  bool is_valid() const { return server_.is_valid() && priority_ >= 0; }
  TO_STRING_KV(K_(server), K_(priority));
  ObAddr server_;
  int64_t priority_; // only two kinds of priority, HIGH, LOW
};

class ObBackupStats final
{
  OB_UNIS_VERSION(1);
public:
  ObBackupStats();
  ~ObBackupStats() {}
  bool is_valid() const;
  int assign(const ObBackupStats &other);
  void cum_with(const ObBackupStats &other);
  void reset();
  TO_STRING_KV(K_(input_bytes), K_(output_bytes), K_(tablet_count), K_(finish_tablet_count),
      K_(macro_block_count), K_(finish_macro_block_count), K_(extra_bytes), K_(finish_file_count),
      K_(log_file_count), K_(finish_log_file_count));
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t extra_bytes_;
  int64_t finish_file_count_;
  int64_t log_file_count_;
  int64_t finish_log_file_count_;
};

struct ObBackupLevel final
{
public:
  enum Level
  {
    CLUSTER = 0,
    SYS_TENANT = 1,
    USER_TENANT = 2,
    MAX_LEVEL = 3
  };
  ObBackupLevel() : level_(Level::MAX_LEVEL) {}
  virtual ~ObBackupLevel() = default;
  bool is_valid() const;
  const char *get_str() const;
  int set_level(const char *str);
  TO_STRING_KV(K_(level));
  Level level_;
};

struct ObHAResultInfo
{
public:
  using Comment = common::ObFixedLengthString<OB_COMMENT_LENGTH>;
  enum FailedType {
    ROOT_SERVICE = 0,
    RESTORE_DATA,
    RESTORE_CLOG,
    BACKUP_DATA,
    BACKUP_CLEAN,
    MAX_FAILED_TYPE
  };
  ObHAResultInfo(const FailedType &type, const ObLSID &ls_id, const ObAddr &addr, const ObTaskId &trace_id,
      const int result);
  ObHAResultInfo(const FailedType &type, const ObAddr &addr, const ObTaskId &trace_id, const int result);
  const char *get_failed_type_str() const;
  int get_comment_str(Comment &comment) const;
  int assign(const ObHAResultInfo &that);
  bool is_valid() const;
  TO_STRING_KV(K_(type), K_(ls_id), K_(trace_id), K_(addr), K_(result));
  FailedType type_;
  ObLSID ls_id_;
  ObTaskId trace_id_;
  ObAddr addr_;
  int result_;

private:
  const char *get_error_str_() const;
};

struct ObBackupJobAttr final
{
public:
  ObBackupJobAttr();
  ~ObBackupJobAttr() {}
  bool is_tmplate_valid() const;
  bool is_valid() const;
  int assign(const ObBackupJobAttr &other);
  int set_executor_tenant_id(const ObString &str);
  int get_executor_tenant_id_str(share::ObDMLSqlSplicer &dml) const;
  int set_plus_archivelog(const char *str);
  const char *get_plus_archivelog_str() const;
  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(incarnation_id), K_(backup_set_id), K_(initiator_tenant_id), K_(initiator_job_id),
      K_(executor_tenant_id), K_(plus_archivelog), K_(backup_level), K_(backup_type), K_(encryption_mode),
      K_(passwd), K_(backup_path), K_(description), K_(start_ts), K_(end_ts), K_(status), K_(result), K_(can_retry), K_(retry_count),
      K_(comment));
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_id_;
  int64_t backup_set_id_;
  uint64_t initiator_tenant_id_;
  int64_t initiator_job_id_;
  common::ObSArray<uint64_t> executor_tenant_id_;
  bool plus_archivelog_;
  ObBackupLevel backup_level_;
  ObBackupType backup_type_;
  ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
  ObBackupPathString backup_path_;
  ObBackupDescription description_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObBackupStatus status_;
  int result_;
  bool can_retry_;
  int64_t retry_count_;
  ObHAResultInfo::Comment comment_;
};

struct ObBackupSetTaskAttr final
{
public:
  ObBackupSetTaskAttr();
  ~ObBackupSetTaskAttr() {}
  bool is_valid() const;

  int assign(const ObBackupSetTaskAttr &other);
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(incarnation_id), K_(job_id), K_(backup_set_id), K_(start_ts), K_(end_ts),
      K_(start_scn), K_(end_scn), K_(user_ls_start_scn), K_(data_turn_id), K_(meta_turn_id), K_(minor_turn_id),
      K_(major_turn_id), K_(status), K_(encryption_mode), K_(passwd), K_(stats), K_(backup_path), K_(retry_cnt), K_(result),
      K_(comment));
  int64_t task_id_;
  uint64_t tenant_id_;
  int64_t incarnation_id_;
  int64_t job_id_;
  int64_t backup_set_id_;
  int64_t start_ts_;
  int64_t end_ts_;
  SCN start_scn_;
  SCN end_scn_;
  SCN user_ls_start_scn_;
  int64_t data_turn_id_;
  int64_t meta_turn_id_;
  int64_t minor_turn_id_;
  int64_t major_turn_id_;
  ObBackupStatus status_;
  ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
  ObBackupStats stats_;
  ObBackupPathString backup_path_;
  int64_t retry_cnt_;
  int result_;
  ObHAResultInfo::Comment comment_;
};

struct ObBackupDataTaskType final
{
  enum Type
  {
    BACKUP_META = 0, // backup ls, tablet meta and inner tablet sstable
    BACKUP_META_FINISH = 1,
    BACKUP_DATA_MINOR = 2,
    BACKUP_DATA_MAJOR = 3,
    BEFORE_PLUS_ARCHIVE_LOG = 4,
    BACKUP_PLUS_ARCHIVE_LOG = 5,
    BACKUP_BUILD_INDEX = 6,
    BACKUP_MAX
  };
  ObBackupDataTaskType() : type_(Type::BACKUP_MAX) {}
  ObBackupDataTaskType(const Type &type) : type_(type) {}
  virtual ~ObBackupDataTaskType() = default;
  bool is_valid() const;
  bool is_backup_meta() const { return Type::BACKUP_META == type_; }
  bool is_backup_data() const {
    return BACKUP_META == type_ || BACKUP_DATA_MINOR == type_ || BACKUP_DATA_MAJOR == type_;
  }
  bool is_backup_minor() const { return Type::BACKUP_DATA_MINOR == type_; }
  bool is_backup_major() const { return Type::BACKUP_DATA_MAJOR == type_; }
  bool is_backup_index() const { return Type::BACKUP_BUILD_INDEX == type_; }
  void set_backup_major() { type_ = Type::BACKUP_DATA_MAJOR; }
  void set_backup_minor() { type_ = Type::BACKUP_DATA_MINOR; }
  int get_backup_data_type(share::ObBackupDataType &backup_data_type) const;
  const char* get_str() const;
  int set_type(const char *buf);
  TO_STRING_KV(K_(type));
  Type type_;
};
struct ObBackupLSTaskAttr final
{
  ObBackupLSTaskAttr();
  ~ObBackupLSTaskAttr() {}
  bool is_valid() const;
  int assign(const ObBackupLSTaskAttr &other);
  int get_black_server_str(const ObIArray<ObAddr> &black_servers, ObSqlString &sql_string) const;
  int set_black_servers(const ObString &str);
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(ls_id), K_(job_id), K_(backup_set_id), K_(backup_type), K_(task_type),
      K_(status), K_(start_ts), K_(end_ts), K_(backup_date), K_(black_servers), K_(dst), K_(task_trace_id),
      K_(stats), K_(start_turn_id), K_(turn_id), K_(retry_id), K_(result), K_(comment), K_(max_tablet_checkpoint_scn));
  int64_t task_id_;
  uint64_t tenant_id_;
  ObLSID ls_id_;
  int64_t job_id_;
  int64_t backup_set_id_;
  ObBackupType backup_type_;
  ObBackupDataTaskType task_type_;
  ObBackupTaskStatus status_;
  int64_t start_ts_;
  int64_t end_ts_;
  int64_t backup_date_;
  ObSEArray<ObAddr, OB_MAX_MEMBER_NUMBER> black_servers_;
  ObAddr dst_;
  ObTaskId task_trace_id_;
  ObBackupStats stats_;
  int64_t start_turn_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int result_;
  ObHAResultInfo::Comment comment_;

  SCN max_tablet_checkpoint_scn_; // the checkpoint scn of all the tablets belong to the same ls while backing up tablets meta.
};

struct ObBackupSetFileDesc final
{
  OB_UNIS_VERSION(1);
public:
  enum BackupSetStatus
  {
    DOING = 0,
    SUCCESS = 1,
    FAILED,
    MAX,
  };

  enum Compatible : int64_t
  {
    COMPATIBLE_VERSION_1 = 1, // 4.0
    COMPATIBLE_VERSION_2 = 2,     // 4.1
    COMPATIBLE_VERSION_3 = 3,     // 4.2
    MAX_COMPATIBLE_VERSION,
  };

public:
  ObBackupSetFileDesc();
  virtual ~ObBackupSetFileDesc() = default;
  static bool is_backup_compatible_valid(const Compatible &compatible)
  {
    return compatible >= COMPATIBLE_VERSION_1 && compatible < MAX_COMPATIBLE_VERSION;
  }
  void reset();
  bool is_key_valid() const;
  bool is_valid() const;
  bool is_same_task(const ObBackupSetFileDesc &other) const;
  const char* get_backup_set_status_str() const;
  int set_backup_set_status(const char *buf);
  int set_plus_archivelog(const char *str);
  const char *get_plus_archivelog_str() const;
  bool is_backup_finish() const { return SUCCESS == status_ || FAILED == status_; }
  int check_passwd(const char *passwd_array) const;
  int assign(const ObBackupSetFileDesc &other);

  TO_STRING_KV(K_(backup_set_id), K_(incarnation), K_(tenant_id), K_(dest_id), K_(backup_type), K_(plus_archivelog),
      K_(date), K_(prev_full_backup_set_id), K_(prev_inc_backup_set_id), K_(stats), K_(start_time), K_(end_time),
      K_(status), K_(result), K_(encryption_mode), K_(passwd), K_(file_status), K_(backup_path), K_(start_replay_scn),
      K_(min_restore_scn), K_(tenant_compatible), K_(backup_compatible), K_(data_turn_id), K_(meta_turn_id),
      K_(cluster_version), K_(consistent_scn));
  int64_t to_string(char *min_restore_scn_display, char *buf, int64_t buf_len) const;

  int64_t backup_set_id_;
  int64_t incarnation_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  ObBackupType backup_type_;
  bool plus_archivelog_;
  int64_t date_;
  int64_t prev_full_backup_set_id_;
  int64_t prev_inc_backup_set_id_;
  ObBackupStats stats_;
  int64_t start_time_;
  int64_t end_time_;
  BackupSetStatus status_;
  int32_t result_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<OB_MAX_PASSWORD_LENGTH> passwd_;
  ObBackupFileStatus::STATUS file_status_;
  common::ObFixedLengthString<OB_MAX_BACKUP_DEST_LENGTH> backup_path_;
  SCN start_replay_scn_;
  SCN min_restore_scn_;
  uint64_t tenant_compatible_;
  Compatible backup_compatible_;
  int64_t data_turn_id_;
  int64_t meta_turn_id_;
  uint64_t cluster_version_;
  int64_t minor_turn_id_;
  int64_t major_turn_id_;
  SCN consistent_scn_;
};

struct ObBackupSkippedType;

struct ObBackupSkipTabletAttr final
{
public:
  static const int64_t BASE_MAJOR_TURN_ID = 1000000;
  ObBackupSkipTabletAttr();
  ~ObBackupSkipTabletAttr() = default;
  bool is_valid() const;
  OB_INLINE int hash(uint64_t &hash_val) const { hash_val = tablet_id_.hash(); return OB_SUCCESS; }
  int assign(const ObBackupSkipTabletAttr &that);
  bool operator==(const ObBackupSkipTabletAttr &that) const { return tablet_id_ == that.tablet_id_; }
  TO_STRING_KV(K_(tablet_id), K_(skipped_type));
public:
  ObTabletID tablet_id_;
  share::ObBackupSkippedType skipped_type_;
};

struct ObBackupLSTaskInfoAttr final
{
  ObBackupLSTaskInfoAttr();
  ~ObBackupLSTaskInfoAttr() = default;
  bool is_valid() const;

  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(ls_id), K_(turn_id), K_(retry_id), K_(backup_data_type),
      K_(backup_set_id), K_(input_bytes), K_(output_bytes), K_(tablet_count), K_(finish_tablet_count),
      K_(macro_block_count), K_(finish_macro_block_count), K_(extra_bytes), K_(file_count),
      K_(max_file_id), K_(is_final));

  int64_t task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t backup_data_type_;

  int64_t backup_set_id_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t extra_bytes_;
  int64_t file_count_;
  int64_t max_file_id_;
  bool is_final_;
};

struct ObLogArchiveDestState final
{
  OB_UNIS_VERSION(1);
public:
  enum State {
    ENABLE = 0,
    DEFER,
    MAX
  };
  ObLogArchiveDestState(): state_(State::MAX) {}
  ~ObLogArchiveDestState() {}
  ObLogArchiveDestState(const ObLogArchiveDestState &other) : state_(other.state_) {}
  bool operator==(const ObLogArchiveDestState &other) const
  {
    return state_ == other.state_;
  }

  bool operator!=(const ObLogArchiveDestState &other) const
  {
    return !(*this == other);
  }

  void operator=(const ObLogArchiveDestState &other)
  {
    state_ = other.state_;
  }

  bool is_valid() const;
  const char *get_str() const;
  int set_state(const char *buf);
  int assign(const ObLogArchiveDestState& that);

#define PROPERTY_DECLARE_STATUS(status_alias, status) \
  bool is_##status_alias() const \
  { \
    return state_ == status; \
  } \
  \
  void set_##status_alias() \
  { state_ = status; } \
  \
  static ObLogArchiveDestState status_alias() \
  { \
    ObLogArchiveDestState s; \
    s.state_ = status; \
    return s; \
  }

  PROPERTY_DECLARE_STATUS(enable, State::ENABLE);
  PROPERTY_DECLARE_STATUS(defer, State::DEFER);

#undef PROPERTY_DECLARE_STATUS

  TO_STRING_KV(K_(state));
  State state_;
};

class BackupConfigItemPair;
struct ObLogArchiveDestAtrr final
{
  enum Binding {
    OPTIONAL = 0,
    MANDATORY = 1
  };

  static constexpr const char *STR_BINDING_TYPES[] = {"OPTIONAL", "MANDATORY"};

  ObLogArchiveDestAtrr();
  ~ObLogArchiveDestAtrr() {}

  int set_piece_switch_interval(const char *buf);
  int set_log_archive_dest(const common::ObString &str);
  int set_binding(const char *buf);

  bool is_valid() const;
  bool is_dest_valid() const;
  bool is_piece_switch_interval_valid() const;

  int get_binding(char *buf, int64_t len) const;
  int get_piece_switch_interval(char *buf, int64_t len) const;

  int gen_config_items(common::ObIArray<BackupConfigItemPair> &items) const;
  int gen_path_config_items(common::ObIArray<BackupConfigItemPair> &items) const;

  int assign(const ObLogArchiveDestAtrr& that);

  TO_STRING_KV(K_(dest), K_(binding), K_(dest_id), K_(piece_switch_interval), K_(state));
  share::ObBackupDest dest_;
  Binding binding_;
  int64_t dest_id_;
  int64_t piece_switch_interval_;
  ObLogArchiveDestState state_;
};

// trim '/' from right until encouter a non backslash charactor.
int trim_right_backslash(ObBackupPathString &path);

// Convert  time string, return like '2022-05-31 12:00:00' if concat is ' '.
int backup_time_to_strftime(const int64_t &ts_s, char *buf, const int64_t buf_len, int64_t &pos, const char concat);

// Convert a scn to time tag, return like '20220531T120000'
int backup_scn_to_time_tag(const SCN &scn, char *buf, const int64_t buf_len, int64_t &pos);

inline uint64_t trans_scn_to_second(const SCN &scn) { return scn.convert_to_ts() / 1000 / 1000; }

struct ObBackupTableListItem final
{
  OB_UNIS_VERSION(1);
public:
  ObBackupTableListItem();
  ~ObBackupTableListItem() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObBackupTableListItem &o);
  bool operator==(const ObBackupTableListItem &o) const;
  bool operator!=(const ObBackupTableListItem &o) const { return !(operator ==(o)); }
  bool operator>(const ObBackupTableListItem &o) const;
  bool operator>=(const ObBackupTableListItem &o) const { return operator > (o) || operator == (o); }
  bool operator<(const ObBackupTableListItem &o) const { return !(operator >= (o)); }
  bool operator<=(const ObBackupTableListItem &o) const { return !(operator > (o)); }

  TO_STRING_KV(K_(database_name), K_(table_name));
  common::ObFixedLengthString<OB_MAX_DATABASE_NAME_LENGTH + 1> database_name_;
  common::ObFixedLengthString<OB_MAX_TABLE_NAME_LENGTH + 1> table_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupTableListItem);
};

struct ObBackupPartialTableListMeta final
{
  OB_UNIS_VERSION(1);
public:
  ObBackupPartialTableListMeta();
  ~ObBackupPartialTableListMeta() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObBackupPartialTableListMeta &other);

  TO_STRING_KV(K_(start_key), K_(end_key));
  ObBackupTableListItem start_key_;
  ObBackupTableListItem end_key_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupPartialTableListMeta);
};
}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_STRUCT_H_ */