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

#ifndef OCEANBASE_SHARE_LOG_OB_LOG_ENTRY_
#define OCEANBASE_SHARE_LOG_OB_LOG_ENTRY_

#include "common/ob_record_header.h"

namespace oceanbase
{
namespace common
{
extern const char *DEFAULT_CKPT_EXTENSION;

enum LogCommand
{
  //// Base command ... ////
  OB_LOG_SWITCH_LOG = 101,  //Log switch command
  OB_LOG_CHECKPOINT = 102,  //checkpoint operation
  OB_LOG_NOP = 103,

  //// UpdateServer ... 200 - 399 ////
  OB_LOG_UPS_MUTATOR = 200,
  OB_UPS_SWITCH_SCHEMA = 201,
  OB_LOG_ROLLBACKED_TRANS = 202,
  OB_SWITCH_MASTER = 203,
  OB_LOG_START_WORKING = 204,
  OB_LOG_DUMMY_LOG = 205,

  //// RootServer ...  400 - 599 ////
  OB_RT_SCHEMA_SYNC = 400,
  OB_RT_CS_REGIST = 401,
  OB_RT_MS_REGIST = 402,
  OB_RT_SERVER_DOWN = 403,
  OB_RT_CS_LOAD_REPORT = 404,
  OB_RT_CS_MIGRATE_DONE = 405,
  OB_RT_CS_START_SWITCH_ROOT_TABLE = 406,
  OB_RT_START_REPORT = 407,
  OB_RT_REPORT_TABLETS = 408,
  OB_RT_ADD_NEW_TABLET = 409,
  OB_RT_CREATE_TABLE_DONE = 410,
  OB_RT_BEGIN_BALANCE = 411,
  OB_RT_BALANCE_DONE = 412,
  OB_RT_US_MEM_FRZEEING = 413,
  OB_RT_US_MEM_FROZEN = 414,
  OB_RT_CS_START_MERGEING = 415,
  OB_RT_CS_MERGE_OVER = 416,
  OB_RT_CS_UNLOAD_DONE = 417,
  OB_RT_US_UNLOAD_DONE = 418,
  OB_RT_DROP_CURRENT_BUILD = 419,
  OB_RT_DROP_LAST_CS_DURING_MERGE = 420,
  OB_RT_SYNC_FROZEN_VERSION = 421,
  OB_RT_SET_UPS_LIST = 422,
  OB_RT_SET_CLIENT_CONFIG = 423,
  OB_RT_REMOVE_REPLICA = 424,
  OB_RT_SYNC_FROZEN_VERSION_AND_TIME = 425,
  OB_RT_REMOVE_TABLE = 426,
  OB_RT_SYNC_FIRST_META_ROW = 427,
  OB_RT_BATCH_ADD_NEW_TABLET = 428,
  OB_RT_GOT_CONFIG_VERSION = 429,
  OB_RT_CS_DELETE_REPLICAS = 430,
  OB_RT_SET_BROADCAST_FROZEN_VERSION = 431,
  OB_RT_LMS_REGIST = 432,
  OB_RT_SET_LAST_MERGED_VERSION = 433,


  //// ChunkServer ... 600 - 799 ////
  OB_LOG_CS_DAILY_MERGE = 600,                //Daily consolidation
  OB_LOG_CS_MIGRATE_PARTITION = 601,        //Migrate tablet
  OB_LOG_CS_BYPASS_LOAD_PARTITION = 603,        //Bypass import
  OB_LOG_CS_MERGE_PARTITION = 604,              //Merge tablet
  OB_LOG_CS_DEL_PARTITION = 605,                  //Delete tablet
  OB_LOG_CS_DEL_TABLE = 606,                   //Delete table
  OB_LOG_CS_CREATE_PARTITION = 607,            //Create tablet
  OB_LOG_CS_DISK_MAINTAIN = 608,            //Disk maintenance
  OB_LOG_CS_NORMAL_MACRO_BLOCK_META = 609,  //Write a single macro block meta
  OB_LOG_CS_DISK_BALANCE = 610,

  OB_LOG_CS_MERGE_MACRO_BLOCK_META = 611,   //merge macro block meta
  OB_LOG_CS_BALANCE_MACRO_BLOCK_META = 612, //balance macro block meta
  OB_LOG_CS_MIGRATE_MACRO_BLOCK_META = 613, //migrate macro block meta

  OB_LOG_CS_SCHEMA_MACRO_BLOCK_META = 614,  //schema macro block meta
  OB_LOG_CS_COMPRESSOR_MACRO_BLOCK_META = 615, //compressor macro block meta
  OB_LOG_CS_PARTITION_MACRO_BLOCK_META = 616,   //tablet meta image macro block meta
  OB_LOG_CS_MACRO_MACRO_BLOCK_META = 617, //macro meta image macro block meta
  OB_LOG_BUILD_LOCAL_INDEX = 618,
  OB_LOG_CS_DUMP = 619,
  OB_LOG_CS_REVERT_PARTITION = 620, //revert a partition to a specified version
  OB_LOG_CS_MINOR_MERGE = 621, //Dump minor version merge

  OB_LOG_CS_MIGRATE_MACRO_BLOCK = 622,        //Write a single macro block in the migration tablet
  OB_LOG_CS_CREATE_PARTITION_STORAGE = 623,
  OB_LOG_CS_DEL_PARTITION_STORAGE = 624,

  OB_LOG_ADD_SSTABLE = 625,
  OB_LOG_TABLE_MGR = 626,
  OB_LOG_UPDATE_PARTITION_META = 627,
  OB_LOG_PARTITION_DROP_INDEX = 628,
  OB_LOG_SET_REPLICA_TYPE = 629,

  OB_LOG_SET_PARTITION_SPLIT_INFO = 630,
  OB_LOG_UPDATE_SPLIT_TABLE_STORE = 631,
  OB_LOG_SET_REFERENCE_TABLES = 632,
  OB_LOG_REMOVE_OLD_TABLE = 633,
  OB_LOG_SET_CLOG_INFO = 634,
  OB_LOG_SET_PUBLISH_VERSION_AFTER_CREATE = 635,
  OB_LOG_SET_STORAGE_INFO = 636,
  OB_LOG_UPDATE_MULTI_VERSION_START = 637,
  OB_LOG_WRITE_REPORT_STATUS = 638,
  OB_LOG_UPDATE_STORAGE_INFO_AFTER_RELEASE_MEMTABLE = 639,
  OB_LOG_SET_RESTORE_FLAG = 640,
  OB_LOG_SET_MIGRATE_STATUS = 641,
  OB_LOG_CLEAR_SSTABLES_AFTER_REPLAY_POINT = 642,
  OB_LOG_WRITE_TENANT_CONFIG = 643,
  OB_LOG_CS_CREATE_PARTITION_GROUP = 644,
  OB_LOG_SET_MEMBER_LIST = 645,
  OB_LOG_SET_PARTITION_RECOVER_POINT = 646,
  OB_LOG_UPDATE_PG_SUPER_BLOCK = 647,
  OB_LOG_UPDATE_TENANT_FILE_INFO = 648,
  OB_LOG_BATCH_REPLACE_MINOR_SSTABLE = 649,
  OB_LOG_SET_PG_LAST_RESTORE_LOG_ID = 650,
  OB_LOG_BATCH_REPLACE_STORE_MAP = 651,
  OB_LOG_ADD_BACKUP_META_DATA = 652,
  OB_LOG_REMOVE_BACKUP_META_DATA = 653,
  OB_LOG_ADD_RECOVERY_POINT_DATA = 654,
  OB_LOG_REMOVE_RECOVERY_POINT_DATA = 655,
  OB_LOG_UPDATE_BLOCK_MGR = 656,
  OB_LOG_UPDATE_DDL_BARRIER_LOG_STATE = 657,
  OB_PG_LOG_ARCHIVE_PERSIST = 658,
  OB_UPDATE_STORAGE_SCHEMA = 659,

  // ls 680 - 700
  OB_LOG_CREATE_LS = 680,
  OB_LOG_REMOVE_LS = 681,

  // tablet 701 - 720
  OB_LOG_UPDATE_TABLET = 701,
  OB_LOG_DELETE_TABLET = 702,

  //// Base command ... ////
  OB_LOG_UNKNOWN = 199
};

/**
 * @brief 日志项
 * 一条日志项由四部分组成:
 *     ObRecordHeader + 日志序号 + LogCommand + 日志内容
 * ObLogEntry中保存ObRecordHeader, 日志序号, LogCommand 三部分
 * ObLogEntry中的data_checksum_项是对"日志序号", "LogCommand", "日志内容" 部分的校验
 */
struct ObLogEntry
{
  ObRecordHeader header_;
  uint64_t seq_;
  int32_t cmd_;

  static const int16_t MAGIC_NUMER = static_cast<int16_t>(0xAAAAL);
  static const int16_t LOG_VERSION = 1;

  ObLogEntry()
  {
    memset(this, 0x00, sizeof(ObLogEntry));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "[LogEntry] ");
    pos += header_.to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, " seq=%lu cmd=%d", seq_, cmd_);
    return pos;
  }

  /**
   * @brief 设置日志序号
   */
  int set_log_seq(const uint64_t seq);

  /**
   * @brief 设置LogCommand
   */
  int set_log_command(const int32_t cmd);

  /**
   * @brief fill all fields of ObRecordHeader
   * 调用该函数需要保证set_log_seq函数已经被调用
   * @param [in] log_data 日志内容缓冲区地址
   * @param [in] data_len 缓冲区长度
   */
  int fill_header(const char *log_data, const int64_t data_len, const int64_t timestamp = 0);

  /**
   * @brief 计算日志序号+LogCommand+日志内容的校验和
   * @param [in] buf 日志内容缓冲区地址
   * @param [in] len 缓冲区长度
   */
  int64_t calc_data_checksum(const char *log_data, const int64_t data_len) const;

  /**
   * After deserialization, this function get the length of log
   */
  int32_t get_log_data_len() const
  {
    return static_cast<int32_t>(header_.data_length_ - sizeof(uint64_t) - sizeof(LogCommand));
  }

  int check_header_integrity(const bool dump_content = true) const;

  /**
   * 调用deserialization之后, 调用该函数检查数据正确性
   * @param [in] log 日志内容缓冲区地址
   */
  int check_data_integrity(const char *log_data, const bool dump_content = true) const;

  static int get_header_size() {return sizeof(ObRecordHeader) + sizeof(uint64_t) + sizeof(LogCommand);}

  NEED_SERIALIZE_AND_DESERIALIZE;
};
} // end namespace common
} // end namespace oceanbase

#endif  //OCEANBASE_SHARE_LOG_OB_LOG_ENTRY_
