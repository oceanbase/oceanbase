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

#ifndef OCEANBASE_LIB_OBLOG_OB_LOG_
#define OCEANBASE_LIB_OBLOG_OB_LOG_

#include <stdarg.h>
#include <time.h>
#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <deque>
#include <string>
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>
#include <cstring>
#include <sys/uio.h>

#include "lib/ob_errno.h"
#include "ob_log_print_kv.h"
#include "lib/coro/co_var.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log_level.h"
#include "lib/oblog/ob_async_log_struct.h"
#include "lib/utility/ob_sample_rate_limiter.h"
#include "common/ob_local_store.h"

#define OB_LOG_MAX_PAR_MOD_SIZE 32
#define OB_LOG_MAX_SUB_MOD_SIZE 32

namespace oceanbase {
namespace lib {
class ObRateLimiter;
}  // namespace lib
}  // namespace oceanbase

namespace oceanbase {
namespace common {
class ObFIFOAllocator;
class ObPLogItem;

#define OB_LOGGER ::oceanbase::common::ObLogger::get_logger()
#define OB_LOG_NEED_TO_PRINT(level) OB_LOGGER.need_to_print(OB_LOG_LEVEL_##level)

#define OB_T_TO_STRING_BODY_1                                                                \
  char* buf1 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key1, false, obj1); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_2                                                                \
  OB_T_TO_STRING_BODY_1                                                                      \
  char* buf2 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key2, false, obj2); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_3                                                                \
  OB_T_TO_STRING_BODY_2                                                                      \
  char* buf3 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key3, false, obj3); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_4                                                                \
  OB_T_TO_STRING_BODY_3                                                                      \
  char* buf4 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key4, false, obj4); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_5                                                                \
  OB_T_TO_STRING_BODY_4                                                                      \
  char* buf5 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key5, false, obj5); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_6                                                                \
  OB_T_TO_STRING_BODY_5                                                                      \
  char* buf6 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key6, false, obj6); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_7                                                                \
  OB_T_TO_STRING_BODY_6                                                                      \
  char* buf7 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key7, false, obj7); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_8                                                                \
  OB_T_TO_STRING_BODY_7                                                                      \
  char* buf8 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key8, false, obj8); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_9                                                                \
  OB_T_TO_STRING_BODY_8                                                                      \
  char* buf9 = buf + MAX(__pos, 0);                                                          \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key9, false, obj9); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_10                                                                 \
  OB_T_TO_STRING_BODY_9                                                                        \
  char* buf10 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key10, false, obj10); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_11                                                                 \
  OB_T_TO_STRING_BODY_10                                                                       \
  char* buf11 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key11, false, obj11); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_12                                                                 \
  OB_T_TO_STRING_BODY_11                                                                       \
  char* buf12 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key12, false, obj12); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_13                                                                 \
  OB_T_TO_STRING_BODY_12                                                                       \
  char* buf13 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key13, false, obj13); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_14                                                                 \
  OB_T_TO_STRING_BODY_13                                                                       \
  char* buf14 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key14, false, obj14); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_15                                                                 \
  OB_T_TO_STRING_BODY_14                                                                       \
  char* buf15 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key15, false, obj15); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_16                                                                 \
  OB_T_TO_STRING_BODY_15                                                                       \
  char* buf16 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key16, false, obj16); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_17                                                                 \
  OB_T_TO_STRING_BODY_16                                                                       \
  char* buf17 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key17, false, obj17); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_18                                                                 \
  OB_T_TO_STRING_BODY_17                                                                       \
  char* buf18 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key18, false, obj18); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_19                                                                 \
  OB_T_TO_STRING_BODY_18                                                                       \
  char* buf19 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key19, false, obj19); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_20                                                                 \
  OB_T_TO_STRING_BODY_19                                                                       \
  char* buf20 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key20, false, obj20); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_21                                                                 \
  OB_T_TO_STRING_BODY_20                                                                       \
  char* buf21 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key21, false, obj21); \
  buf[__pos++] = '\0';

#define OB_T_TO_STRING_BODY_22                                                                 \
  OB_T_TO_STRING_BODY_21                                                                       \
  char* buf22 = buf + MAX(__pos, 0);                                                           \
  ret = ::oceanbase::common::logdata_print_key_obj(buf, buf_size, __pos, key22, false, obj22); \
  buf[__pos++] = '\0';

#define DEFINE_OB_PRINT_KV(n)                                                                               \
  template <LOG_TYPENAME_TN##n>                                                                             \
  void OB_PRINT(const char* mod_name,                                                                       \
      const int32_t level,                                                                                  \
      const char* file,                                                                                     \
      const int32_t line,                                                                                   \
      const char* function,                                                                                 \
      const uint64_t location_hash_val,                                                                     \
      const char* info_string,                                                                              \
      LOG_PARAMETER_KV##n)                                                                                  \
  {                                                                                                         \
    int64_t __pos = 0;                                                                                      \
    int ret = OB_SUCCESS;                                                                                   \
    if (OB_LIKELY(!OB_LOGGER.get_guard())) {                                                                \
      OB_LOGGER.get_guard() = true;                                                                         \
      char* buf = ::oceanbase::common::ObLogger::get_small_buffer();                                        \
      const int64_t buf_size = ::oceanbase::common::ObLogger::OB_SMALL_LOG_BUFFER_SIZE - 1;                 \
      OB_T_TO_STRING_BODY_##n OB_LOGGER.log_message_kv(                                                     \
          mod_name, level, file, line, function, location_hash_val, info_string, EXPAND_ARGUMENT_##n(buf)); \
      OB_LOGGER.get_guard() = false;                                                                        \
      UNUSED(ret);                                                                                          \
    }                                                                                                       \
  }

//@class ObLogIdLevelMap
//@brief store the level of each par-module and sub-module. The key is module ID.
//       To be used for SQL hint, this class should be POD-type.
struct ObLogIdLevelMap {
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  //@brief Set to default value OB_LOG_LEVEL_INFO.
  inline void reset_level()
  {
    set_level(OB_LOG_LEVEL_INFO);
  }
  //@brief Set all modules' levels.
  void set_level(const int8_t level);
  //@brief Set the par-module's level.
  int set_level(const uint64_t par_mod_id, const int8_t level);
  //@brief Set the sub-module's level.
  int set_level(const uint64_t par_mod_id, const uint64_t sub_mod_id, const int8_t level);

  //@brief Get the non-module's level.
  inline int8_t get_level() const
  {
    return non_mod_level_;
  }
  //@brief Get par-module's level.
  inline int8_t get_level(uint64_t par_mod_id) const;
  //@brief Get sub-module's level
  inline int8_t get_level(uint64_t par_mod_id, uint64_t sub_mod_id) const;

  /// convert log level map to a bitset.
  // @todo later by
  // int to_bitset(ObBitSet<768> &bitset) const;
  // int from_bitset(const ObBitSet<768> &bitset);

  // log_level_[i][0] representing level of the par-module.
  int8_t log_level_[MAX_PAR_MOD_SIZE][MAX_SUB_MOD_SIZE + 1];
  int8_t non_mod_level_;
};

//@class ObLogIdLevelMap
//@brief stroe the ID of each par-module and sub-module. The key is module's name.
class ObLogNameIdMap {
public:
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  ObLogNameIdMap();

  //@brief Regist parent module.
  int register_mod(const uint64_t mod_id, const char* mod_name);
  //@brief Regist sub-module.
  int register_mod(const uint64_t mod_id, const uint64_t sub_mod_id, const char* sub_mod_name);
  //@brief Get par-module's ID and sub-module's ID by module's name.
  int get_mod_id(const char* mod_name, const char* sub_mod_name, uint64_t& par_mod_id, uint64_t& sub_mod_id) const;
  //@brief Get par-module's ID by module's name.
  int get_mod_id(const char* mod_name, uint64_t& mod_id) const;

  //@brief Get par-module's name by module's ID.
  int get_par_mod_name(const uint64_t par_mod_id, const char*& mod_name) const;

  //@brief Get sub-module's name by module's ID.
  int get_sub_mod_name(const uint64_t par_mod_id, const uint64_t sub_mod_id, const char*& mod_name) const;

private:
  // name_id_map_[i][0] par-module's name, name_id_map_[i][i](i>1) sub-module's name
  const char* name_id_map_[MAX_PAR_MOD_SIZE][MAX_SUB_MOD_SIZE + 1];
};

ObLogIdLevelMap& get_thread_log_id_level_map();

// set thread_log_id_level_map with str setting.
// must use with ObThreadLogLevelUtils::clear
int process_thread_log_id_level_map(const char* str, const int32_t str_length);

//@class ObThreadLogLevelUtils
class ObThreadLogLevelUtils {
public:
  //@brief Set pointer to default value NULL.
  static void init();
  //@brief Set the pointer to the session's id_level_map.
  // Must used with clear() in pair
  static inline void init(const ObLogIdLevelMap* id_level_map);

  // Set thread log level.
  // Must used with clear() in pair
  static inline void init(const int8_t level);

  //@brief Set pointer to NULL and level to none.
  static inline void clear();

  //@brief Get the pointer of the session's id_level_map.
  static inline const ObLogIdLevelMap* get();

  static inline int8_t get_level();
  //@brief Get the thread-only ObThreadLogLevel.
  static inline ObThreadLogLevel* get_thread_log_level();
};

class ObThreadFlags {
public:
  enum {
    RS_FLAG = 0,
  };

  static void set_bit(uint64_t& flags, const uint64_t idx)
  {
    flags |= (0x1 << idx);
  }

  static void del_bit(uint64_t& flags, const uint64_t idx)
  {
    flags &= ~(0x1 << idx);
  }

  static bool has_bit(const uint64_t& flags, const uint64_t idx)
  {
    return (flags & (0x1 << idx));
  }

  static uint64_t& get_thread_flags()
  {
    // NOTE: used to check if current thread is owned by root service
    // that all logging would print into specified file.
    static __thread uint64_t thread_flags = 0;
    return thread_flags;
  }

  static void clear_flags()
  {
    get_thread_flags() = 0;
  }

  static void set_rs_flag()
  {
    set_bit(get_thread_flags(), RS_FLAG);
  }

  static void cancel_rs_flag()
  {
    del_bit(get_thread_flags(), RS_FLAG);
  }

  static bool is_rs_thread()
  {
    return has_bit(get_thread_flags(), RS_FLAG);
  }
};

class ObRSThreadFlag {
public:
  ObRSThreadFlag()
  {
    ObThreadFlags::set_rs_flag();
  }
  ~ObRSThreadFlag()
  {
    ObThreadFlags::cancel_rs_flag();
  }
};

// Type retention for future expansion
struct ObPLogWriterCfg : public ObBaseLogWriterCfg {
  ObPLogWriterCfg();
  virtual ~ObPLogWriterCfg()
  {}
  inline bool is_valid() const
  {
    return ObBaseLogWriterCfg::is_valid();
  }
};

class ObBasebLogPrint {
public:
  ObBasebLogPrint()
  {}
  virtual ~ObBasebLogPrint()
  {}
  virtual int64_t get_data_length() const = 0;
  virtual int64_t get_timestamp() const = 0;
  virtual int print_data(char* buf, int64_t buf_len, int64_t& pos) const = 0;
};

//@class ObLogger
//@brief main class of logging facilities. Provide base function, for example log_message(),
// parse_set().
//
// This class is changed from the class 'CLogger' in tblog.h which was written by the God
// named DuoLong.
class ObLogger : public ObBaseLogWriter {
private:
  static constexpr int LOG_ITEM_SIZE = sizeof(ObPLogItem);

public:
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;  // default max log file size
  // check whether disk storing log-file has no space every 2s
  // Only max_file_size_ > 0, this is effective.
  static const int64_t DISK_SAMPLE_TIME = 2 * 1000 * 1000;
  // check whether log file exist every 5s.
  static const int64_t FILE_SAMPLE_TIME = 5 * 1000 * 1000;
  // stat async flush speed every 1s.
  static const int64_t FLUSH_SAMPLE_TIME = 1 * 1000 * 1000;
  static const uint64_t MAX_THREAD_LOG_NUM = 1;
  static const uint64_t MAX_TASK_LOG_TYPE = 5;                   // ObTaskType::MAX
  static const uint64_t CAN_PRINT_DISK_SIZE = 32 * 1024 * 1024;  // 32MB
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;
  static const int32_t MAX_LOG_FILE_COUNT = 10 * 1024;

  static const int64_t MAX_LOG_HEAD_SIZE = 256;
  static const int64_t MAX_LOG_SIZE = 64 * 1024;          // 64kb
  static const int64_t LOCAL_BUF_SIZE = 64 * 1024;        // 64kb
  static const int64_t REMAIN_TRACE_LOG_SIZE = 4 * 1024;  // 4kb

  static const int64_t GROUP_COMMIT_MAX_WAIT_US = 500 * 1000;  // 0.5s
  static const int64_t GROUP_COMMIT_MIN_ITEM_COUNT = 1;
  static const int64_t GROUP_COMMIT_MAX_ITEM_COUNT = 4;
  static const char* PERF_LEVEL;

  // mainly for ob_localtime
  static __thread time_t last_unix_sec_;
  static __thread struct tm last_localtime_;

  enum UserMsgLevel { USER_NONE = 0, USER_WARN, USER_ERROR, USER_NOTE };

  enum LogLevel {
    LOG_ERROR = 0,
    LOG_USER_ERROR,
    LOG_WARN,
    LOG_INFO,
    LOG_TRACE,
    LOG_DEBUG,
    LOG_MAX_LEVEL,
  };

  struct FileName {
    // can not add other member and virtual function in this class.
    FileName()
    {
      memset(file_name_, 0, sizeof(file_name_));
    }
    int64_t to_string(char* buf, const int64_t buf_len) const;
    char file_name_[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
  };

private:
  struct LogBuffer {
    LogBuffer() : pos_(0), trace_mode_(false)
    {
      buffer_[0] = '\0';
    }
    ~LogBuffer()
    {}
    void reset();
    void set_trace_mode(bool trace_mode);
    int64_t get_len() const
    {
      return pos_;
    }
    bool is_oversize() const
    {
      return (pos_ < 0 || pos_ >= MAX_LOG_SIZE);
    }
    int64_t pos_;
    bool trace_mode_;
    char buffer_[MAX_LOG_SIZE];
  };

  struct LogBufferMgr {
    LogBufferMgr()
    {
      reset();
    }
    void reset();
    void set_trace_mode(bool trace_mode);
    LogBuffer& get_buffer();
    void print_trace_buffer(
        int32_t level, const char* file, int32_t line, const char* function, const uint64_t location_hash_val);

    uint64_t idx_;
    bool trace_mode_;
    LogBuffer buffers_[MAX_THREAD_LOG_NUM];
  };

  struct LogLocation {
    LogLocation(const char* file, const int32_t line, const char* function)
        : file_(file), line_(line), function_(function)
    {}
    const char* file_;
    const int32_t line_;
    const char* function_;
  };

  struct ModSetting {
    static const uint64_t NON_SUBMOD_ID = 1024;
    ModSetting() : par_mod_id_(NON_SUBMOD_ID), sub_mod_id_(NON_SUBMOD_ID), level_(OB_LOG_LEVEL_INFO)
    {}
    ModSetting(uint64_t par_mod_id, uint64_t sub_mod_id, int8_t level)
        : par_mod_id_(par_mod_id), sub_mod_id_(sub_mod_id), level_(level)
    {}
    uint64_t par_mod_id_;  // ALL:NON_SUBMOD_ID
    uint64_t sub_mod_id_;  // sub_mod_id_:NON_SUBMOD_ID present only setting par_mod.
    int8_t level_;
  };

public:
  ObLogger();
  virtual ~ObLogger();
  virtual int init(const ObBaseLogWriterCfg& log_cfg);
  virtual void destroy();

protected:
  virtual void process_log_items(ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt);

public:
  bool is_async_log_used() const
  {
    return (enable_async_log() && OB_LIKELY(is_inited()) && !has_stoped());
  }
  bool is_in_async_logging() const
  {
    return is_async_log_used() && !is_enable_logging();
  }

  int64_t get_generic_log_write_count() const
  {
    return written_count_[0];
  }
  int64_t get_user_request_log_write_count() const
  {
    return written_count_[1];
  }
  int64_t get_data_maintain_log_write_count() const
  {
    return written_count_[2];
  }
  int64_t get_root_service_log_write_count() const
  {
    return written_count_[3];
  }
  int64_t get_schema_log_write_count() const
  {
    return written_count_[4];
  }
  int64_t get_force_allow_log_write_count() const
  {
    return written_count_[5];
  }

  int64_t get_generic_log_dropped_count() const
  {
    return dropped_count_[0];
  }
  int64_t get_user_request_log_dropped_count() const
  {
    return dropped_count_[1];
  }
  int64_t get_data_maintain_log_dropped_count() const
  {
    return dropped_count_[2];
  }
  int64_t get_root_service_log_dropped_count() const
  {
    return dropped_count_[3];
  }
  int64_t get_schema_log_dropped_count() const
  {
    return dropped_count_[4];
  }
  int64_t get_force_allow_log_dropped_count() const
  {
    return dropped_count_[5];
  }

  int64_t get_dropped_error_log_count() const
  {
    return dropped_log_count_[LOG_ERROR];
  }
  int64_t get_dropped_warn_log_count() const
  {
    return dropped_log_count_[LOG_WARN];
  }
  int64_t get_dropped_info_log_count() const
  {
    return dropped_log_count_[LOG_INFO];
  }
  int64_t get_dropped_trace_log_count() const
  {
    return dropped_log_count_[LOG_TRACE];
  }
  int64_t get_dropped_debug_log_count() const
  {
    return dropped_log_count_[LOG_DEBUG];
  }

  int64_t get_async_flush_log_speed() const
  {
    return last_async_flush_count_per_sec_;
  }
  bool enable_async_log() const
  {
    return enable_async_log_;
  }
  void set_enable_async_log(const bool flag)
  {
    enable_async_log_ = flag;
  }
  void set_stop_append_log()
  {
    stop_append_log_ = true;
  }
  void disable()
  {
    stop_append_log_ = true;
  }
  void set_disable_logging(const bool flag)
  {
    disable_logging_ = flag;
  }
  bool is_enable_logging() const
  {
    return !disable_logging_;
  }
  void set_use_multi_flush(const bool flag)
  {
    use_multi_flush_ = flag;
  }
  bool is_use_multi_flush() const
  {
    return use_multi_flush_;
  }
  bool is_force_allows() const;
  void check_reset_force_allows();

  int64_t get_reimbursation_time();
  int64_t get_wait_us(const int32_t level);

  //@brief thread buffer for printing log
  LogBuffer* get_thread_buffer();

  //@brief set thread trace mode
  void set_trace_mode(bool trace_mode);
  bool is_trace_mode();

  // print thread trace buffer
  void print_trace_buffer(
      int32_t level, const char* file, int32_t line, const char* function, const uint64_t location_hash_val);

  int64_t get_write_size() const
  {
    return log_file_[FD_SVR_FILE].write_size_;
  }
  int64_t get_rs_write_size() const
  {
    return log_file_[FD_RS_FILE].write_size_;
  }
  int64_t get_elec_write_size() const
  {
    return log_file_[FD_ELEC_FILE].write_size_;
  }
  int64_t get_total_write_count() const
  {
    return log_file_[FD_SVR_FILE].write_count_;
  }
  int64_t get_rs_total_write_count() const
  {
    return log_file_[FD_RS_FILE].write_count_;
  }
  int64_t get_elec_total_write_count() const
  {
    return log_file_[FD_ELEC_FILE].write_count_;
  }
  ObPLogFileStruct& get_elec_log()
  {
    return *(log_file_ + FD_ELEC_FILE);
  }

  //@brief Log the message without level checking.
  void log_message(const char* mod_name, const int32_t level, const char* file, const int32_t line,
      const char* function, const uint64_t location_hash_val, const char* fmt, ...)
      __attribute__((format(printf, 8, 9)));
  void log_message_va(const char* mod_name, int32_t level, const char* file, int32_t line, const char* function,
      const uint64_t location_hash_val, const char* fmt, va_list args);

  void log_user_message(const UserMsgLevel user_msg_level, const int errcode, const char* fmt, ...)
      __attribute__((format(printf, 4, 5)));

  void log_message_kv(const char* mod_name, const int32_t level, const char* file, const int32_t line,
      const char* function, const uint64_t location_hash_val, const char* info_string);

  void log_user_message_info(const char* mod_name, UserMsgLevel user_msg_level, const int32_t level, const char* file,
      const int32_t line, const char* function, const uint64_t location_hash_val, const int errcode,
      const char* info_string);

  int async_log_message_kv(const char* mod_name, const int32_t level, const LogLocation& location,
      const uint64_t location_hash_val, const char* info_string, const int64_t string_len);

  template <typename Function>
  void do_async_log_message(const char* mod_name, int32_t level, const char* file, int32_t line, const char* function,
      const uint64_t location_hash_val, Function& log_data_func);

  //@brief Log the message without level checking.
  void async_log_message(const char* mod_name, const int32_t level, const char* file, const int32_t line,
      const char* function, const uint64_t location_hash_val, const char* fmt, ...)
      __attribute__((format(printf, 8, 9)));

  void log_user_error_line_column(const UserMsgLevel user_msg_level, const int line, const int column);

  DEFINE_LOG_PRINT_KV(1);
  DEFINE_LOG_PRINT_KV(2);
  DEFINE_LOG_PRINT_KV(3);
  DEFINE_LOG_PRINT_KV(4);
  DEFINE_LOG_PRINT_KV(5);
  DEFINE_LOG_PRINT_KV(6);
  DEFINE_LOG_PRINT_KV(7);
  DEFINE_LOG_PRINT_KV(8);
  DEFINE_LOG_PRINT_KV(9);
  DEFINE_LOG_PRINT_KV(10);
  DEFINE_LOG_PRINT_KV(11);
  DEFINE_LOG_PRINT_KV(12);
  DEFINE_LOG_PRINT_KV(13);
  DEFINE_LOG_PRINT_KV(14);
  DEFINE_LOG_PRINT_KV(15);
  DEFINE_LOG_PRINT_KV(16);
  DEFINE_LOG_PRINT_KV(17);
  DEFINE_LOG_PRINT_KV(18);
  DEFINE_LOG_PRINT_KV(19);
  DEFINE_LOG_PRINT_KV(20);
  DEFINE_LOG_PRINT_KV(21);
  DEFINE_LOG_PRINT_KV(22);

  //@brief Check whether the level to print.
  bool __attribute__((weak, noinline, cold)) need_to_print(const int32_t level)
  {
    return (level <= get_log_level());
  }
  //@brief Check whether the level of the par-module to print.
  bool __attribute__((weak, noinline, cold)) need_to_print(const uint64_t par_mod_id, const int32_t level);
  //@brief Check whether the level of the sub-module to print.
  bool __attribute__((weak, noinline, cold))
  need_to_print(const uint64_t par_mod_id, const uint64_t sub_mod_id, const int32_t level);

  //@brief Set the log-file's name.
  //@param[in] filename The log-file's name.
  //@param[in] flag Whether redirect the stdout and stderr to the descriptor of the log-file.
  // FALSE:redirect TRUE:no redirect.
  //@param[in] open_wf whether create warning log-file to store warning buffer.
  //@param[in] filename of rootservice log-file's name.
  //@param[in] filename of election log-file's name.
  void set_file_name(const char* filename, const bool no_redirect_flag = false, const bool open_wf = false,
      const char* rs_filename = NULL, const char* elec_filename = NULL);

  // whether log warn/error log to log.wf
  void set_log_warn(bool log_warn)
  {
    enable_wf_flag_ = log_warn;
  }

  //@brief Check the log-file's status.
  void check_file();
  //@brief Check the log-file's status.
  void check_file(ObPLogFileStruct& log_struct, const bool redirect_flag, const bool open_wf_flag);

  //@brief Set whether checking the log-file at each message logging.
  //@param[in] v 1:with check, 0:without check
  void set_check(const bool force_check)
  {
    force_check_ = force_check;
  }

  //@brief Set the max log-file size. The unit is byte. Default value and max value are 1GB.
  void set_max_file_size(int64_t max_file_size);
  //@brief Set the max number of log-files. If max_file_index = 0, no limit.
  int set_max_file_index(int64_t max_file_index = 0x0F);
  //@brief Set whether record old log file. If this flag and max_file_index set,
  // will record log files in the directory for log file
  int set_record_old_log_file(bool rec_old_file_flag = false);

  //@brief Get current time.
  static struct timeval get_cur_tv();

  //@brief Get the process-only ObLogger.
  static ObLogger& get_logger();

  //@brief Regist par-module name with par_mod_id.
  int register_mod(const uint64_t par_mod_id, const char* par_mod_name);
  //@brief Regist sub-module name with par_mod_id and sub_mod_id.
  int register_mod(const uint64_t par_mod_id, const uint64_t sub_mod_id, const char* sub_mod_name);

  //@brief Set global log-file's level and warning log-file's level.
  //@param[in] level The log-file's level.
  //@param[in] wf_level The warning log-file's level.
  //@param[in] version The time(us) change the log level.
  void set_log_level(const char* level, const char* wf_level = NULL, int64_t version = 0);
  //@brief Set global level.
  void set_log_level(const int32_t level, int64_t version = 0)
  {
    set_log_level(static_cast<int8_t>(level), version);
  }
  void set_log_level(const int8_t level, int64_t version = 0);
  int set_mod_log_levels(const char* level_str, int64_t version = 0);

  void down_log_level(int64_t version = 0)
  {
    set_log_level(id_level_map_.get_level() + 1, version);
  }
  void up_log_level(int64_t version = 0)
  {
    set_log_level(id_level_map_.get_level() - 1, version);
  }

  int32_t get_level() const
  {
    return id_level_map_.get_level();
  }
  //@brief current log level, with session log level considered.
  int32_t get_log_level() const;
  int32_t get_log_level(const uint64_t par_mod_id) const;
  int32_t get_log_level(const uint64_t par_mod_id, const uint64_t sub_mod_id) const;
  inline const char* get_level_str() const
  {
    return errstr_[id_level_map_.get_level()];
  }
  int get_level_str(const int8_t level_id, const char*& level_str) const;

  void disable_thread_log_level()
  {
    disable_thread_log_level_ = true;
  }

  //@brief Get the int8_t type representing the level in string.
  int level_str2int(const char* level_name, int8_t& level_int);

  int parse_check(const char* str, const int32_t str_length);
  int parse_check(const char* str, const int32_t str_length, int32_t& valid_length);
  //@brief Parse the string like "ALL.*:INFO, SQL.ENG:DEBUG, COMMON.*:ERROR",
  // and set the global level map.
  int parse_set(const char* str, const int32_t str_length, int64_t version = 0);
  // for safe, this function name different with parse_set.
  int parse_set_with_valid_ret(const char* str, const int32_t str_length, int32_t& valid_length, int64_t version = 0);
  //@brief Parse the string like "ALL.*:INFO, SQL.ENG:DEBUG, COMMON.*:ERROR",
  // and set the level map.
  int parse_set(const char* str, const int32_t str_length, int32_t& valid_length, ObLogIdLevelMap& id_level_map);

  //@brief get name_id_map_
  const ObLogNameIdMap& get_name_id_map() const
  {
    return name_id_map_;
  }
  //@brief get id_level_map_
  const ObLogIdLevelMap& get_id_level_map() const
  {
    return id_level_map_;
  }

  static void set_tl_limiter(::oceanbase::lib::ObRateLimiter& limiter)
  {
    tl_log_limiter_ = &limiter;
  }

  static void set_tl_type(const int32_t type)
  {
    tl_type_ = type;
  }

  static void set_default_limiter(::oceanbase::lib::ObRateLimiter& limiter)
  {
    default_log_limiter_ = &limiter;
  }

  enum { OB_SMALL_LOG_BUFFER_SIZE = (1 << 16) };

  static char* get_small_buffer(const int32_t index = 0)
  {
    static __thread char buffer[OB_SMALL_LOG_BUFFER_SIZE];
    UNUSED(index);
    return buffer;
  }

  bool& get_guard()
  {
    static __thread bool guard = false;
    return guard;
  }

private:
  //@brief If version <= 0, return true.
  // If version > 0, return version > level_version_ and if true, update level_version_.
  bool check_and_set_level_version(int64_t version);

  //@brief get thread buffer mgr
  ObLogger::LogBufferMgr* get_buffer_mgr();

  void log_tail(int32_t level, LogBuffer& log_buffer);

  void log_head_info(const char* mod_name, int32_t level, LogLocation location, LogBuffer& log_buffer);

  void log_data(const char* mod_name, const int32_t level, LogLocation location, LogBuffer& log_buffer);

  void insert_warning_buffer(
      const UserMsgLevel user_msg_level, const int errcode, const char* data, const int64_t data_len);

  void insert_warning_buffer_line_column_info(const UserMsgLevel user_msg_level, const int line, const int column);

  int get_mod_set(const char* par_mod, const char* sub_mod, const char* level, ModSetting& mod_set);

  //@brief When setting mod_setting_list = NULL, do not return mod_setting_list.
  int parse_check(
      const char* str, const int32_t str_length, int32_t& valid_length, void* mod_setting_list /*ObList<ModSetting>*/);

  int setting_list_processing(ObLogIdLevelMap& id_level_map, void* mod_setting_list /*ObList<ModSetting>* */);
  //@brief record log files in the log directory. It's used when max_file_index and rec_old_file_flag_ set.
  int record_old_log_file();

  int get_log_files_in_dir(
      const char* filename, void* files /*ObIArray<FileName> * */, void* wf_files /*ObIArray<FileName> * */);

  int add_files_to_list(void* files /*ObIArray<FileName> * */, void* wf_files /*ObIArray<FileName> * */,
      std::deque<std::string>& file_list, std::deque<std::string>& wf_file_list);

  void rotate_log(
      const int64_t size, const bool redirect_flag, ObPLogFileStruct& log_struct, const ObPLogFDType fd_type);
  //@brief Rename the log to a filename with fmt. And open a new file with the old, then add old file to file_list.
  //@param[in] filename the old filename to rotate.
  //@param[in] whether redirect, FALSE:redirect TRUE:no redirect
  //@param[out] after rotated log, open new file_fd
  //@param[out] after rotated wf log, open new wf_file_fd
  //@param[out] add rotated log file name to file list
  //@param[out] add rotated wf log file name to file list
  void rotate_log(const char* filename, const ObPLogFDType fd_type, const bool redirect_flag, int32_t& fd,
      int32_t& wf_fd, std::deque<std::string>& file_list, std::deque<std::string>& wf_file_list);

  static void* async_flush_log_handler(void* arg);
  void flush_logs_to_file(ObPLogItem** log_item, const int64_t count);

  int async_log_data_header(ObPLogItem& log_item, const timeval& tv, const char* mod_name, const int32_t level,
      const char* file, const int32_t line, const char* function);

  int try_upgrade_log_item(ObPLogItem*& log_item, bool& upgrade_result);

  int async_log_data_body(ObPLogItem& log_item, const char* info_string, const int64_t string_len);
  int async_log_data_body_v(ObPLogItem& log_item, const char* fmt, va_list args);
  void check_log_end(ObPLogItem& log_item, int64_t pos);

  int check_error_log(ObPLogItem& log_item);
  int precheck_tl_log_limiter(const int32_t level, bool& allow);
  int check_tl_log_limiter(ObPLogItem& log_item, const uint64_t location_hash_val);

  int alloc_log_item(const int32_t level, const int32_t size, ObPLogItem*& log_item);
  void free_log_item(ObPLogItem* log_item);
  void inc_dropped_log_count(const int32_t level);

private:
  static const char* const errstr_[];
  // default log rate limiter if there's no tl_log_limiter
  static ::oceanbase::lib::ObRateLimiter* default_log_limiter_;
  static RLOCAL(lib::ObRateLimiter*, tl_log_limiter_);
  static constexpr int N_LIMITER = 4096;
  static lib::ObSampleRateLimiter per_log_limiters_[N_LIMITER];
  static RLOCAL(int32_t, tl_type_);
  // used for stat logging time and log dropped
  static RLOCAL(uint64_t, curr_logging_seq_);
  static RLOCAL(uint64_t, last_logging_seq_);
  static RLOCAL(int64_t, last_logging_cost_time_us_);

  // NOTE: whether to stop logging
  static RLOCAL(bool, disable_logging_);

  ObPLogFileStruct log_file_[MAX_FD_FILE];

  int64_t max_file_size_;
  int64_t max_file_index_;

  pthread_mutex_t file_size_mutex_;
  pthread_mutex_t file_index_mutex_;

  // log level
  ObLogNameIdMap name_id_map_;
  ObLogIdLevelMap id_level_map_;  // level of log-file
  int8_t wf_level_;               // level of warning log-file
  int64_t level_version_;         // version of log level

  // Whether to disable the Thread Log Level function, the default is false, that is, the Thread Log Level is not
  // disabled
  //
  // The purpose is to do not require Thread Log Level scenarios,
  // Optimize performance to avoid accessing ObThreadLogLevel structure every time get_log_level()
  bool disable_thread_log_level_;

  bool force_check_;         // whether check log-file at each message logging.
  bool redirect_flag_;       // whether redirect, TRUE: redirect FALSE: no redirect.
  bool open_wf_flag_;        // whether open warning log-file.
  bool enable_wf_flag_;      // whether write warning log to wf log-file.
  bool rec_old_file_flag_;   // whether record old file.
  volatile bool can_print_;  // when disk has no space, logger control

  bool enable_async_log_;  // if false, use sync way logging
  bool use_multi_flush_;   // whether use multi flush, default false
  bool stop_append_log_;   // whether stop product log
  bool enable_perf_mode_;
  // used for statistics
  int64_t dropped_log_count_[LOG_MAX_LEVEL];
  int64_t last_async_flush_count_per_sec_;

  int64_t dropped_count_[MAX_TASK_LOG_TYPE + 1];  // last one is force allow count
  int64_t written_count_[MAX_TASK_LOG_TYPE + 1];
  int64_t current_written_count_[MAX_TASK_LOG_TYPE + 1];
  ObFIFOAllocator* allocator_;
  ObFIFOAllocator* error_allocator_;
  static __thread char local_buf_[LOCAL_BUF_SIZE];
};

inline ObLogger& ObLogger::get_logger()
{
  static ObLogger logger;
  return logger;
}

class ObLoggerTraceMode {
public:
  ObLoggerTraceMode()
  {
    OB_LOGGER.set_trace_mode(true);
  }
  ~ObLoggerTraceMode()
  {
    OB_LOGGER.set_trace_mode(false);
  }

  void set_trace_mode(bool trace_mode)
  {
    OB_LOGGER.set_trace_mode(trace_mode);
  }
  // used print_trace_buffer(OB_LOG_LEVEL(INFO))
  void print_trace_buffer(
      int32_t level, const char* file, int32_t line, const char* function, const uint64_t location_hash_val)
  {
    OB_LOGGER.need_to_print(level) ? OB_LOGGER.print_trace_buffer(level, file, line, function, location_hash_val)
                                   : (void)0;
  }
};

inline int8_t ObLogIdLevelMap::get_level(uint64_t par_mod_id) const
{
  return (par_mod_id < MAX_PAR_MOD_SIZE) ? log_level_[par_mod_id][0] : static_cast<int8_t>(OB_LOG_LEVEL_NP);
}

inline int8_t ObLogIdLevelMap::get_level(uint64_t par_mod_id, uint64_t sub_mod_id) const
{
  return (par_mod_id < MAX_PAR_MOD_SIZE && sub_mod_id < MAX_SUB_MOD_SIZE) ? log_level_[par_mod_id][sub_mod_id + 1]
                                                                          : static_cast<int8_t>(OB_LOG_LEVEL_NP);
}

inline void ObThreadLogLevelUtils::init()
{
  ObThreadLogLevel* trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->id_level_map_ = NULL;
  }
}

inline void ObThreadLogLevelUtils::init(const ObLogIdLevelMap* id_level_map)
{
  ObThreadLogLevel* trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->id_level_map_ = id_level_map;
    trace_log_level->level_ = (id_level_map == NULL ? (int8_t)OB_LOG_LEVEL_NONE : id_level_map->non_mod_level_);
  }
}

inline void ObThreadLogLevelUtils::init(const int8_t level)
{
  ObThreadLogLevel* trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->level_ = level;
  }
}

inline void ObThreadLogLevelUtils::clear()
{
  ObThreadLogLevel* trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->id_level_map_ = NULL;
    trace_log_level->level_ = OB_LOG_LEVEL_NONE;
  }
}

inline const ObLogIdLevelMap* ObThreadLogLevelUtils::get()
{
  const ObLogIdLevelMap* ret = NULL;
  ObThreadLogLevel* trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    ret = trace_log_level->id_level_map_;
  }
  return ret;
}

inline int8_t ObThreadLogLevelUtils::get_level()
{
  int8_t level = OB_LOG_LEVEL_NONE;
  ObThreadLogLevel* trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    level = trace_log_level->level_;
  }
  return level;
}

inline ObThreadLogLevel* ObThreadLogLevelUtils::get_thread_log_level()
{
  ObThreadLogLevel* ret = nullptr;
  lib::CoRoutine* co = lib::CoSched::get_active_routine();
  if (co != nullptr) {
    ret = &reinterpret_cast<common::ObLocalStore*>(co->get_context().get_local_store())->log_level_;
  }
  return ret;
}

inline void ObLogger::check_log_end(ObPLogItem& log_item, int64_t pos)
{
  const int64_t buf_size = log_item.get_buf_size();
  if (buf_size > 0) {
    if (pos < 0) {
      pos = 0;
    } else if (pos > buf_size - 2) {
      pos = buf_size - 2;
    }
    char* data = log_item.get_buf();
    data[pos++] = '\n';
    data[pos] = '\0';
    log_item.set_data_len(pos);
  }
}

inline void ObLogger::log_message_kv(const char* mod_name, const int32_t level, const char* file, const int32_t line,
    const char* function, const uint64_t location_hash_val, const char* info_string)
{
  int ret = OB_SUCCESS;
  LogBuffer* log_buffer = NULL;
  if (OB_NOT_NULL(info_string) && OB_LIKELY(is_enable_logging())) {
    if (is_trace_mode()) {
      if (OB_NOT_NULL(log_buffer = get_thread_buffer()) && OB_LIKELY(!log_buffer->is_oversize())) {
        set_disable_logging(true);
        log_head_info(mod_name, level, LogLocation(file, line, function), *log_buffer);
        int64_t& pos = log_buffer->pos_;
        char* data = log_buffer->buffer_;
        LOG_PRINT_INFO(info_string);
        log_tail(level, *log_buffer);
      }
    } else if (is_async_log_used()) {
      ret = async_log_message_kv(mod_name,
          level,
          LogLocation(file, line, function),
          location_hash_val,
          info_string,
          static_cast<int64_t>(strlen(info_string)));
    } else {  // sync away
      if (OB_NOT_NULL(log_buffer = get_thread_buffer()) && OB_LIKELY(!log_buffer->is_oversize())) {
        set_disable_logging(true);
        int64_t& pos = log_buffer->pos_;
        char* data = log_buffer->buffer_;
        LOG_PRINT_INFO(info_string);
        log_data(mod_name, level, LogLocation(file, line, function), *log_buffer);
      }
    }
    set_disable_logging(false);
  }
}

inline void ObLogger::log_user_message_info(const char* mod_name, UserMsgLevel user_msg_level, const int32_t level,
    const char* file, const int32_t line, const char* function, const uint64_t location_hash_val, const int errcode,
    const char* info_string)
{
  if (OB_NOT_NULL(info_string)) {
    insert_warning_buffer(user_msg_level, errcode, info_string, static_cast<int64_t>(strlen(info_string)));
    if (need_to_print(level)) {
      char buf[OB_SMALL_LOG_BUFFER_SIZE];
      int64_t pos = 0;
      ::oceanbase::common::logdata_print_key_obj(buf, OB_SMALL_LOG_BUFFER_SIZE, pos, "ret", false, errcode);
      log_message_kv(mod_name, level, file, line, function, location_hash_val, info_string, buf);
    }
  }
}

bool __attribute__((weak, noinline, cold)) ObLogger::need_to_print(const uint64_t par_mod_id, const int32_t level)
{
  return (level <= get_log_level(par_mod_id));
}

bool __attribute__((weak, noinline, cold))
ObLogger::need_to_print(const uint64_t par_mod_id, const uint64_t sub_mod_id, const int32_t level)
{
  return (level <= get_log_level(par_mod_id, sub_mod_id));
}

inline int32_t ObLogger::get_log_level() const
{
  int8_t cur_level = OB_LOG_LEVEL_INFO;
  if (enable_perf_mode_) {
    cur_level = OB_LOG_LEVEL_ERROR;
  } else {
    cur_level = id_level_map_.get_level();

    if (!disable_thread_log_level_) {
      const ObLogIdLevelMap* session_id_level_map = ObThreadLogLevelUtils::get();
      int8_t thread_level = ObThreadLogLevelUtils::get_level();
      if (NULL != session_id_level_map) {
        cur_level = session_id_level_map->get_level();
      } else if (OB_LOG_LEVEL_NONE != thread_level) {
        cur_level = thread_level;
      }
    }
  }
  return cur_level;
}

inline int32_t ObLogger::get_log_level(const uint64_t par_mod_id) const
{
  int8_t cur_level = OB_LOG_LEVEL_INFO;
  if (enable_perf_mode_) {
    cur_level = OB_LOG_LEVEL_ERROR;
  } else {
    cur_level = id_level_map_.get_level(par_mod_id);

    if (!disable_thread_log_level_) {
      const ObLogIdLevelMap* session_id_level_map = ObThreadLogLevelUtils::get();
      int8_t thread_level = ObThreadLogLevelUtils::get_level();
      if (NULL != session_id_level_map) {
        cur_level = session_id_level_map->get_level(par_mod_id);
      } else if (OB_LOG_LEVEL_NONE != thread_level) {
        cur_level = thread_level;
      }
    }
  }
  return cur_level;
}

inline int32_t ObLogger::get_log_level(const uint64_t par_mod_id, const uint64_t sub_mod_id) const
{
  int8_t cur_level = OB_LOG_LEVEL_INFO;
  if (enable_perf_mode_) {
    cur_level = OB_LOG_LEVEL_ERROR;
  } else {
    cur_level = id_level_map_.get_level(par_mod_id, sub_mod_id);

    if (!disable_thread_log_level_) {
      const ObLogIdLevelMap* session_id_level_map = ObThreadLogLevelUtils::get();
      int8_t thread_level = ObThreadLogLevelUtils::get_level();
      if (NULL != session_id_level_map) {
        cur_level = session_id_level_map->get_level(par_mod_id, sub_mod_id);
      } else if (OB_LOG_LEVEL_NONE != thread_level) {
        cur_level = thread_level;
      }
    }
  }
  return cur_level;
}

inline struct timeval ObLogger::get_cur_tv()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv;
}

inline int ObLogger::set_mod_log_levels(const char* level_str, int64_t version)
{
  return parse_set(level_str, static_cast<int32_t>(std::strlen(level_str)), version);
}

template <typename Function>
inline void ObLogger::do_async_log_message(const char* mod_name, int32_t level, const char* file, int32_t line,
    const char* function, const uint64_t location_hash_val, Function& log_data_func)
{
  int ret = OB_SUCCESS;
  bool allow = true;
  if (OB_FAIL(precheck_tl_log_limiter(level, allow))) {
    LOG_STDERR("precheck_tl_log_limiter error, ret=%d\n", ret);
  } else if (OB_UNLIKELY(!allow)) {
    inc_dropped_log_count(level);
  } else {
    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    const int64_t logging_time_us_begin =
        static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec);
    ++curr_logging_seq_;

    // format to local buf
    ObPLogItem* log_item = new (local_buf_) ObPLogItem();
    log_item->set_buf_size(sizeof(local_buf_) - LOG_ITEM_SIZE);
    log_item->set_log_level(level);
    if (OB_FAIL(async_log_data_header(*log_item, tv, mod_name, level, file, line, function))) {
      LOG_STDERR("async_log_data_header error ret = %d\n", ret);
    } else if (OB_FAIL(log_data_func(log_item))) {
      LOG_STDERR("log data error ret = %d\n", ret);
    }

    // clone by data_size
    if (OB_SUCC(ret)) {
      ObPLogItem* new_log_item = nullptr;
      if (OB_FAIL(alloc_log_item(level, LOG_ITEM_SIZE + log_item->get_data_len(), new_log_item))) {
        LOG_STDERR("alloc_log_item error, ret=%d\n", ret);
      } else {
        MEMCPY(new_log_item, log_item, LOG_ITEM_SIZE + log_item->get_data_len());
        // update buf_size
        new_log_item->set_buf_size(log_item->get_data_len());
        log_item = new_log_item;
      }
    }

    // check log limiter
    if (OB_SUCC(ret)) {
      const int32_t tl_type = log_item->get_tl_type();
      if (OB_FAIL(check_tl_log_limiter(*log_item, location_hash_val))) {
        LOG_STDERR("check_tl_log_limiter error ret = %d\n", ret);
      } else if (OB_FAIL(append_log(*log_item))) {
        LOG_STDERR("append_log error ret = %d\n", ret);
      } else {
        // can't access log_item after append_log
        if (tl_type >= 0 && tl_type < MAX_TASK_LOG_TYPE) {
          (void)ATOMIC_AAF(current_written_count_ + tl_type, 1);
        }
        last_logging_seq_ = curr_logging_seq_;
        struct timeval tv_end;
        gettimeofday(&tv_end, NULL);
        const int64_t logging_time_us_end =
            static_cast<int64_t>(tv_end.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv_end.tv_usec);
        last_logging_cost_time_us_ = logging_time_us_end - logging_time_us_begin;
      }
    }

    // stat
    if (OB_FAIL(ret)) {
      inc_dropped_log_count(level);
      if ((char*)log_item != local_buf_) {
        free_log_item(log_item);
      }
      log_item = NULL;
    }
    check_reset_force_allows();
  }
}

void __attribute__((weak)) OB_PRINT(const char* mod_name, const int32_t level, const char* file, const int32_t line,
    const char* function, const uint64_t location_hash_val, const char* info_string)
{
  if (OB_LIKELY(!OB_LOGGER.get_guard())) {
    OB_LOGGER.get_guard() = true;
    OB_LOGGER.log_message_kv(mod_name, level, file, line, function, location_hash_val, info_string);
    OB_LOGGER.get_guard() = false;
  }
}

DEFINE_OB_PRINT_KV(1)
DEFINE_OB_PRINT_KV(2)
DEFINE_OB_PRINT_KV(3)
DEFINE_OB_PRINT_KV(4)
DEFINE_OB_PRINT_KV(5)
DEFINE_OB_PRINT_KV(6)
DEFINE_OB_PRINT_KV(7)
DEFINE_OB_PRINT_KV(8)
DEFINE_OB_PRINT_KV(9)
DEFINE_OB_PRINT_KV(10)
DEFINE_OB_PRINT_KV(11)
DEFINE_OB_PRINT_KV(12)
DEFINE_OB_PRINT_KV(13)
DEFINE_OB_PRINT_KV(14)
DEFINE_OB_PRINT_KV(15)
DEFINE_OB_PRINT_KV(16)
DEFINE_OB_PRINT_KV(17)
DEFINE_OB_PRINT_KV(18)
DEFINE_OB_PRINT_KV(19)
DEFINE_OB_PRINT_KV(20)
DEFINE_OB_PRINT_KV(21)
DEFINE_OB_PRINT_KV(22)

}  // namespace common
}  // namespace oceanbase
#endif
