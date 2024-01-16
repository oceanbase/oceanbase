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

#ifndef OCENABASE_LOG_MINER_ARGS_H_
#define OCENABASE_LOG_MINER_ARGS_H_

#include <cstdint>
#include "lib/allocator/page_arena.h"     // ObArenaAllocator
#include "lib/string/ob_string_buffer.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_log_miner_mode.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerCmdArgs {
public:
  static void print_usage(const char *prog_name);
public:
  ObLogMinerCmdArgs()
  { reset(); }

  int init(int argc, char *argv[]);

  void reset();

  TO_STRING_KV(
    K(mode_),
    KCSTRING(cluster_addr_),
    KCSTRING(user_name_),
    KCSTRING(table_list_),
    KCSTRING(column_cond_),
    KCSTRING(operations_),
    KCSTRING(target_table_),
    KCSTRING(source_table_),
    KCSTRING(start_time_),
    KCSTRING(end_time_),
    KCSTRING(log_level_),
    KCSTRING(timezone_),
    K(verbose_),
    K(print_usage_)
  );

public:
  LogMinerMode  mode_;
  const char    *cluster_addr_;
  const char    *user_name_;
  const char    *password_;
  const char    *archive_dest_;
  const char    *table_list_;
  const char    *column_cond_;
  const char    *operations_;
  const char    *output_;
  const char    *recovery_path_;
  const char    *target_table_;
  const char    *source_table_;
  const char    *start_time_;
  const char    *end_time_;
  const char    *log_level_;
  const char    *timezone_;
  bool          verbose_;
  bool          print_usage_;
private:
  int validate_() const;
};

class AnalyzerArgs {
  static const char *CLUSTER_ADDR_KEY;
  static const char *USER_NAME_KEY;
  static const char *PASSWORD_KEY;
  static const char *ARCHIVE_DEST_KEY;
  static const char *TABLE_LIST_KEY;
  static const char *COLUMN_COND_KEY;
  static const char *OPERATIONS_KEY;
  static const char *OUTPUT_DST_KEY;
  static const char *LOG_LEVEL_KEY;
  static const char *START_TIME_US_KEY;
  static const char *END_TIME_US_KEY;
  static const char *TIMEZONE_KEY;
public:
  AnalyzerArgs() { reset(); }
  int init(const ObLogMinerCmdArgs &args);
  int validate() const;
  void reset();

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    KCSTRING(cluster_addr_),
    KCSTRING(user_name_),
    KCSTRING(table_list_),
    KCSTRING(column_cond_),
    KCSTRING(operations_),
    KCSTRING(log_level_),
    K(start_time_us_),
    K(end_time_us_),
    KCSTRING(timezone_)
  );

public:
  char *cluster_addr_;
  char *user_name_;
  char *password_;
  char *archive_dest_;
  char *table_list_;
  char *column_cond_;
  char *operations_;
  char *output_dst_;
  char *log_level_;
  int64_t start_time_us_;
  int64_t end_time_us_;
  char *timezone_;
private:
  ObArenaAllocator alloc_;
};

class FlashbackerArgs {
public:
  FlashbackerArgs() { reset(); }
  int init(const ObLogMinerCmdArgs &args);
  int validate() const;
  void reset();

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    KCSTRING(cluster_addr_),
    KCSTRING(user_name_),
    KCSTRING(tgt_table_),
    KCSTRING(src_table_),
    KCSTRING(progress_dst_),
    KCSTRING(log_level_),
    K(start_time_us_),
    K(end_time_us_)
  );
public:
  const char *cluster_addr_;
  const char *user_name_;
  const char *password_;
  const char *recovery_path_;
  const char *tgt_table_;
  const char *src_table_;
  const char *progress_dst_;
  const char *log_level_;
  int64_t start_time_us_;
  int64_t end_time_us_;
};

class ObLogMinerArgs {
public:
  static const char *DEFAULT_LOG_LEVEL;
  static const char *LOGMINER_LOG_FILE;
  static const char *DEFAULT_LOGMNR_TIMEZONE;
  static const char *DEFAULT_LOGMNR_OPERATIONS;
  static const char *DEFAULT_LOGMNR_TABLE_LIST;
public:
  ObLogMinerArgs() { reset(); }
  ~ObLogMinerArgs() { reset(); }
  int init(int argc, char *argv[]);
  int validate() const;
  void reset();

  NEED_SERIALIZE_AND_DESERIALIZE;

  DECLARE_TO_STRING;

public:
  LogMinerMode mode_;
  bool verbose_;
  bool print_usage_;
  const char *timezone_;
  AnalyzerArgs analyzer_args_;
  FlashbackerArgs flashbacker_args_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMinerArgs);
};

}
}

#endif