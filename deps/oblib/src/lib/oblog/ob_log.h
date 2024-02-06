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
#include <sys/statfs.h>
#include <signal.h>

#include "lib/ob_errno.h"
#include "ob_log_print_kv.h"
#include "lib/coro/co_var.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log_level.h"
#include "lib/oblog/ob_async_log_struct.h"
#include "lib/utility/ob_defer.h"
#include "lib/oblog/ob_syslog_rate_limiter.h"
#include "lib/signal/ob_signal_handlers.h"

#define OB_LOG_MAX_PAR_MOD_SIZE 64
#define OB_LOG_MAX_SUB_MOD_SIZE 64

namespace oceanbase {
namespace lib {
class ObRateLimiter;
}  // lib
}  // oceanbase

namespace oceanbase
{
namespace common
{

inline int64_t log_cur_ts()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  const int64_t us =
            static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec);
  return us;
}

class ObSyslogTimeGuard
{
public:
  explicit ObSyslogTimeGuard()
  {
    start_ts_ = log_cur_ts();
    last_ts_ = start_ts_;
    click_count_ = 0;
  }
  void click(const char *mod = NULL)
  {
    const int64_t cur_ts = log_cur_ts();
    if (OB_LIKELY(click_count_ < MAX_CLICK_COUNT)) {
      click_str_[click_count_] = mod;
      click_[click_count_++] = (int32_t)(cur_ts - last_ts_);
      last_ts_ = cur_ts;
    }
  }
  int64_t get_start_ts() const
  {
    return start_ts_;
  }
  int64_t get_diff() const
  {
    return log_cur_ts() - start_ts_;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  static const int64_t MAX_CLICK_COUNT = 16;
private:
  int64_t start_ts_;
  int64_t last_ts_;
  int64_t click_count_;
  int32_t click_[MAX_CLICK_COUNT];
  const char *click_str_[MAX_CLICK_COUNT];
};

class ObVSliceAlloc;
class ObBlockAllocMgr;
class ObFIFOAllocator;
class ObPLogItem;

extern void allow_next_syslog(int64_t count = 1);
extern int logdata_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args);
extern ObPLogFDType get_fd_type(const char *mod_name);

#define OB_LOGGER ::oceanbase::common::ObLogger::get_logger()
#define OB_LOG_NEED_TO_PRINT(level) (OB_UNLIKELY(OB_LOGGER.need_to_print(OB_LOG_LEVEL_##level)))

//@class ObLogIdLevelMap
//@brief stroe the level of each par-module and sub-module. The key is module ID.
//       To be used for SQL hint, this class should be POD-type.
struct ObLogIdLevelMap
{
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  //@brief Set to default value OB_LOG_LEVEL_INFO.
  inline void reset_level() {set_level(OB_LOG_LEVEL_INFO);}
  //@brief Set all modules' levels.
  void set_level(const int8_t level);
  //@brief Set the par-module's level.
  int set_level(const uint64_t par_mod_id, const int8_t level);
  //@brief Set the sub-module's level.
  int set_level(const uint64_t par_mod_id, const uint64_t sub_mod_id, const int8_t level);

  //@brief Get the non-module's level.
  inline int8_t get_level() const {return non_mod_level_;}
  //@brief Get par-module's level.
  inline int8_t get_level(uint64_t par_mod_id) const;
  //@brief Get sub-module's level
  inline int8_t get_level(uint64_t par_mod_id, uint64_t sub_mod_id) const;

  /// convert log level map to a bitset.
  // @todo later by zhuweng.yzf
  //int to_bitset(ObBitSet<768> &bitset) const;
  //int from_bitset(const ObBitSet<768> &bitset);

  //log_level_[i][0] representing level of the par-module.
  int8_t log_level_[MAX_PAR_MOD_SIZE][MAX_SUB_MOD_SIZE + 1];
  int8_t non_mod_level_;
};



//@class ObLogIdLevelMap
//@brief stroe the ID of each par-module and sub-module. The key is module's name.
class ObLogNameIdMap
{
public:
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  ObLogNameIdMap();

  //@brief Regist parent module.
  int register_mod(const uint64_t mod_id, const char *mod_name);
  //@brief Regist sub-module.
  int register_mod(const uint64_t mod_id,
                   const uint64_t sub_mod_id,
                   const char *sub_mod_name);
  //@brief Get par-module's ID and sub-module's ID by module's name.
  int get_mod_id(const char *mod_name,
                 const char *sub_mod_name,
                 uint64_t &par_mod_id,
                 uint64_t &sub_mod_id) const;
  //@brief Get par-module's ID by module's name.
  int get_mod_id(const char *mod_name, uint64_t &mod_id) const;

private:
  //name_id_map_[i][0] par-module's name, name_id_map_[i][i](i>1) sub-module's name
  const char *name_id_map_[MAX_PAR_MOD_SIZE][MAX_SUB_MOD_SIZE + 1];
};

ObLogIdLevelMap &get_thread_log_id_level_map();

//set thread_log_id_level_map with str setting.
//must use with ObThreadLogLevelUtils::clear
int process_thread_log_id_level_map(const char *str, const int32_t str_length);

//@class ObThreadLogLevelUtils
class ObThreadLogLevelUtils
{
public:
  //@brief Set pointer to default value NULL.
  static void init();
  //@brief Set the pointer to the session's id_level_map.
  //Must used with clear() in pair
  static inline void init(const ObLogIdLevelMap *id_level_map);

  //Set thread log level.
  //Must used with clear() in pair
  static inline void init(const int8_t level);

  //@brief Set pointer to NULL and level to none.
  static inline void clear();

  //@brief Get the pointer of the session's id_level_map.
  static inline const ObLogIdLevelMap *get();

  static inline int8_t get_level();
private:
  static inline const ObLogIdLevelMap*& get_id_level_map_();
  static inline int8_t& get_level_();
};

class ObThreadFlags
{
public:
  enum {
    RS_FLAG = 0,
  };

  static void set_bit(uint64_t &flags, const uint64_t idx)
  { flags |= (0x1 << idx); }

  static void del_bit(uint64_t &flags, const uint64_t idx)
  { flags &= ~(0x1 << idx); }

  static bool has_bit(const uint64_t &flags, const uint64_t idx)
  { return (flags & (0x1 << idx)); }

  OB_INLINE static uint64_t &get_thread_flags()
  {
    // NOTE: used to check if current thread is owned by root service
    // that all logging would print into specified file.
    RLOCAL_INLINE(uint64_t, thread_flags);
    return thread_flags;
  }

  static void clear_flags()
  { get_thread_flags() = 0; }

  static void set_rs_flag()
  { set_bit(get_thread_flags(), RS_FLAG); }

  static void cancel_rs_flag()
  { del_bit(get_thread_flags(), RS_FLAG); }

  static bool is_rs_thread()
  { return has_bit(get_thread_flags(), RS_FLAG); }
};

class ObRSThreadFlag
{
public:
  ObRSThreadFlag()
  { ObThreadFlags::set_rs_flag(); }
  ~ObRSThreadFlag()
  { ObThreadFlags::cancel_rs_flag(); }
};

// Type retention for future expansion
struct ObPLogWriterCfg : public ObBaseLogWriterCfg
{
  ObPLogWriterCfg();
  virtual ~ObPLogWriterCfg() {}
  inline bool is_valid() const
  {
    return ObBaseLogWriterCfg::is_valid();
  }
};

class ObBasebLogPrint
{
public:
  ObBasebLogPrint() {}
  virtual ~ObBasebLogPrint() {}
  virtual int64_t get_data_length() const = 0;
  virtual int64_t get_timestamp() const = 0;
  virtual int print_data(char *buf, int64_t buf_len, int64_t &pos) const = 0;
};

enum class ProbeAction
{
 PROBE_NONE,
 PROBE_BT,
 PROBE_ABORT,
 PROBE_DISABLE,
 PROBE_STACK,
};

//@class ObLogger
//@brief main class of logging facilities. Provide base function, for example log_message(),
//parse_set().
//
//This class is changed from the class 'CLogger' in tblog.h which was written by the God
//named DuoLong.
class ObLogger : public ObBaseLogWriter
{
private:
  static constexpr int LOG_ITEM_SIZE = sizeof(ObPLogItem);
public:
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024; // default max log file size
  //check whether disk storing log-file has no space every 2s
  //Only max_file_size_ > 0, this is effective.
  static const int64_t DISK_SAMPLE_TIME = 2 * 1000 * 1000;
  //check whether log file exist every 5s.
  static const int64_t FILE_SAMPLE_TIME = 5 * 1000 * 1000;
  //stat async flush speed every 1s.
  static const int64_t FLUSH_SAMPLE_TIME = 1* 1000 * 1000;
  static const uint64_t MAX_THREAD_LOG_NUM = 1;
  static const uint64_t MAX_TASK_LOG_TYPE = 5;//ObTaskType::MAX
  static const uint64_t CAN_PRINT_DISK_SIZE = 32 * 1024 * 1024; //32MB
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;
  static const int32_t MAX_LOG_FILE_COUNT = 10 * 1024;

  static const int64_t MAX_LOG_HEAD_SIZE = 256;
  static const int64_t MAX_LOG_SIZE = 64 * 1024; //64kb
  static const int64_t LOCAL_BUF_SIZE = 65 * 1024; //64kb

  static const int64_t GROUP_COMMIT_MAX_WAIT_US = 500*1000;//0.5s
  static const int64_t GROUP_COMMIT_MIN_ITEM_COUNT = 1;
  static const int64_t GROUP_COMMIT_MAX_ITEM_COUNT = 4;
  static const char *PERF_LEVEL;
  static const int64_t NORMAL_LOG_SIZE = 1 << 10;

  static const char *const SECURITY_AUDIT_FILE_NAME_FORMAT;

  //mainly for ob_localtime
  RLOCAL_STATIC(time_t, last_unix_sec_);
  RLOCAL_STATIC(struct tm, last_localtime_);
  RLOCAL_STATIC(int64_t, limited_left_log_size_);

  enum UserMsgLevel
  {
    USER_NONE = 0,
    USER_WARN,
    USER_ERROR,
    USER_NOTE
  };

  struct FileName
  {
    //can not add other member and virtual function in this class.
    FileName() { memset(file_name_, 0, sizeof(file_name_)); }
    int64_t to_string(char *buf, const int64_t buf_len) const;
    char file_name_[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
  };

private:
  struct TraceBuffer
  {
    TraceBuffer() : pos_(0)
    { buffer_[0] = '\0'; }
    ~TraceBuffer()
    { }
    void reset();
    void set_pos(int64_t pos) { pos_ = pos; }
    int64_t &get_pos() { return pos_; }
    int64_t get_cap() const { return sizeof(buffer_); }
    bool is_oversize() const { return (pos_ < 0 || pos_ >= sizeof buffer_); }
    int64_t pos_;
    char buffer_[MAX_LOG_SIZE];
  };

  struct ModSetting
  {
    static const uint64_t NON_SUBMOD_ID = 1024;
    ModSetting() : par_mod_id_(NON_SUBMOD_ID), sub_mod_id_(NON_SUBMOD_ID), level_(OB_LOG_LEVEL_INFO) { }
    ModSetting(uint64_t par_mod_id, uint64_t sub_mod_id, int8_t level) :
        par_mod_id_(par_mod_id), sub_mod_id_(sub_mod_id), level_(level) { }
    uint64_t par_mod_id_;//ALL:NON_SUBMOD_ID
    uint64_t sub_mod_id_;//sub_mod_id_:NON_SUBMOD_ID present only setting par_mod.
    int8_t level_;
  };


public:
  ObLogger();
  virtual ~ObLogger();
  virtual int init(const ObBaseLogWriterCfg &log_cfg,
                   const bool is_arb_replica);
  virtual void stop();
  virtual void wait();
  virtual void destroy();

protected:
  virtual void process_log_items(ObIBaseLogItem **items, const int64_t item_cnt, int64_t &finish_cnt);

public:
  int set_probe(char *str);
  const char *show_probe() const;
  bool is_async_log_used() const { return (enable_async_log() && OB_LIKELY(is_inited()) && !has_stopped()); }
  bool is_in_async_logging() const { return is_async_log_used() && !is_enable_logging(); }

  int64_t get_generic_log_write_count() const { return written_count_[0]; }
  int64_t get_user_request_log_write_count() const { return written_count_[1]; }
  int64_t get_data_maintain_log_write_count() const { return written_count_[2]; }
  int64_t get_root_service_log_write_count() const { return written_count_[3]; }
  int64_t get_schema_log_write_count() const { return written_count_[4]; }
  int64_t get_force_allow_log_write_count() const { return written_count_[5]; }

  int64_t get_generic_log_dropped_count() const { return dropped_count_[0]; }
  int64_t get_user_request_log_dropped_count() const { return dropped_count_[1]; }
  int64_t get_data_maintain_log_dropped_count() const { return dropped_count_[2]; }
  int64_t get_root_service_log_dropped_count() const { return dropped_count_[3]; }
  int64_t get_schema_log_dropped_count() const { return dropped_count_[4]; }
  int64_t get_force_allow_log_dropped_count() const { return dropped_count_[5]; }

  int64_t get_dropped_error_log_count() const { return dropped_log_count_[OB_LOG_LEVEL_ERROR]; }
  int64_t get_dropped_warn_log_count() const { return dropped_log_count_[OB_LOG_LEVEL_WARN]; }
  int64_t get_dropped_info_log_count() const { return dropped_log_count_[OB_LOG_LEVEL_INFO]; }
  int64_t get_dropped_trace_log_count() const { return dropped_log_count_[OB_LOG_LEVEL_TRACE]; }
  int64_t get_dropped_debug_log_count() const { return dropped_log_count_[OB_LOG_LEVEL_DEBUG]; }

  int64_t get_async_flush_log_speed() const { return last_async_flush_count_per_sec_; }
  bool enable_async_log() const { return enable_async_log_; }
  void set_enable_async_log(const bool flag) { enable_async_log_ = flag; }
  void set_stop_append_log() { stop_append_log_ = true; }
  void disable() { stop_append_log_ = true; }
  bool set_disable_logging(const bool flag) {
    bool old_val = disable_logging_;
    disable_logging_ = flag;
    return old_val;
  }
  bool is_enable_logging() const { return !disable_logging_; }
  void set_use_multi_flush(const bool flag) { use_multi_flush_ = flag; }
  bool is_use_multi_flush() const { return use_multi_flush_; }
  bool is_force_allows() const;
  void check_reset_force_allows();

  int64_t get_reimbursation_time();
  int64_t get_wait_us(const int32_t level);

  //@brief thread buffer for printing log
  TraceBuffer *get_trace_buffer();

  //@brief set thread trace mode
  void set_trace_mode(bool trace_mode);
  bool is_trace_mode() const { return trace_mode_; }

  //print thread trace buffer
  void print_trace_buffer(const char* mod_name,
                          const int32_t level,
                          const char* file,
                          int32_t line,
                          const char* function,
                          const uint64_t location_hash_val);

  int64_t get_write_size() const { return log_file_[FD_SVR_FILE].write_size_; }
  int64_t get_rs_write_size() const { return log_file_[FD_RS_FILE].write_size_; }
  int64_t get_elec_write_size() const { return log_file_[FD_ELEC_FILE].write_size_; }
  int64_t get_trace_write_size() const { return log_file_[FD_TRACE_FILE].write_size_; }
  int64_t get_total_write_count() const { return log_file_[FD_SVR_FILE].write_count_; }
  int64_t get_rs_total_write_count() const { return log_file_[FD_RS_FILE].write_count_; }
  int64_t get_elec_total_write_count() const { return log_file_[FD_ELEC_FILE].write_count_; }
  int64_t get_trace_total_write_count() const { return log_file_[FD_TRACE_FILE].write_count_; }
  ObPLogFileStruct &get_elec_log() { return *(log_file_ + FD_ELEC_FILE); }

  void insert_warning_buffer(const UserMsgLevel user_msg_level,
                             const int errcode,
                             const char *data,
                             const int64_t data_len);
  void log_user_message(const UserMsgLevel user_msg_level,
                        const int errcode,
                        const char *fmt, ...) __attribute__((format(printf, 4, 5)));
  //@brief Log the message without level checking.
  void log_message_fmt(const char *mod_name,
                   const int32_t level,
                   const char *file,
                   const int32_t line,
                   const char *function,
                   const uint64_t location_hash_val,
                   const int errcode,
                   const char *fmt, ...) __attribute__((format(printf, 9, 10)));
  void log_message_va(const char *mod_name,
                      int32_t level,
                      const char *file,
                      int32_t line,
                      const char *function,
                      const uint64_t location_hash_val,
                      const int errcode,
                      const char *fmt,
                      va_list args);
  void log_user_error_line_column(const UserMsgLevel user_msg_level,
                             const int line,
                             const int column);

  __attribute__((noinline, cold)) int fill_kv(char *buf, const int64_t buf_len, int64_t &pos,
                                              const bool with_comma)
  {
    return OB_SUCCESS;
  }

  __attribute__((noinline, cold)) int fill_kv(char *buf, const int64_t buf_len, int64_t &pos,
                                              const bool with_comma,
                                              const ObILogKV &kv)
  {
    return kv.print(buf, buf_len, pos, with_comma);
  }

  template <typename ... Args>
  __attribute__((noinline, cold)) int fill_kv(char *buf, const int64_t buf_len, int64_t &pos,
                                              const bool with_comma,
                                              const ObILogKV &kv,
                                              Args const & ... args)
  {
    int ret = OB_SUCCESS;
    ret = fill_kv(buf, buf_len, pos, with_comma, kv);
    if (OB_SUCC(ret)) {
      ret = fill_kv(buf, buf_len, pos, 1, std::forward<const Args&&>(args)...);
    }
    return ret;
  }

  __attribute__((noinline, cold)) int fill_log_buffer(char *data, const int64_t buf_len,
      int64_t &pos, const char *info)
  {
    int ret = OB_SUCCESS;
    LOG_PRINT_INFO(info);
    return ret;
  }

  template <typename ... Args>
  __attribute__((noinline, cold)) int fill_log_buffer(char *data, const int64_t buf_len,
                                                      int64_t &pos, const char *info,
                                                      const ObILogKV &kv,
                                                      Args const & ... args)
  {
    int ret = OB_SUCCESS;
    LOG_PRINT_INFO_BEGIN(info);
    if (OB_SUCC(ret)) {
      ret = fill_kv(data, buf_len, pos, 0, kv);
    }
    if (OB_SUCC(ret)) {
      ret = fill_kv(data, buf_len, pos, 1, std::forward<const Args&&>(args)...);
    }
    LOG_KV_END();
    return ret;
  }

  template <typename ... Args>
  __attribute__((noinline, cold)) void log_message_kv(const char *mod_name,
      const int32_t level, const char *file, const int32_t line, const char *function,
      const uint64_t location_hash_val, const int errcode,
      const char *info, Args const && ... args)
  {
    auto &&log_data_func = [&] (char *buf, const int64_t buf_len, int64_t &pos) {
                             int ret = OB_SUCCESS;
                             ret = fill_log_buffer(buf, buf_len, pos, info,
                                                   std::forward<const Args&&>(args)...);
                             return ret;
                           };
    log_it(mod_name, level, file, line, function, location_hash_val, errcode, log_data_func);
  }


#ifdef OB_BUILD_AUDIT_SECURITY
  int async_audit_dump(const common::ObBasebLogPrint &info);
#endif

  //@brief Check whether the level to print.
  bool __attribute__((weak, noinline, cold)) need_to_print(const int32_t level) { return (level <= get_log_level()); }
  //@brief Check whether the level of the par-module to print.
  bool __attribute__((weak, noinline, cold)) need_to_print(const uint64_t par_mod_id, const int32_t level);
  //@brief Check whether the level of the sub-module to print.
  bool __attribute__((weak, noinline, cold)) need_to_print(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                            const int32_t level);

  //@brief Set the log-file's name.
  //@param[in] filename The log-file's name.
  //@param[in] flag Whether redirect the stdout and stderr to the descriptor of the log-file.
  //FALSE:redirect TRUE:no redirect.
  //@param[in] open_wf whether create warning log-file to store warning buffer.
  //@param[in] finename of rootservice log-file's name.
  //@param[in] finename of election log-file's name.
  void set_file_name(const char *filename,
                     const bool no_redirect_flag = false,
                     const bool open_wf = false,
                     const char *rs_filename = NULL,
                     const char *elec_filename = NULL,
                     const char *trace_filename = NULL,
                     const char *audit_filename = NULL);

  //whether log warn/error log to log.wf
  void set_log_warn(bool log_warn) { enable_wf_flag_ = log_warn; }

  //@brief Check the log-file's status.
  void check_file();
  //@brief Check the log-file's status.
  void check_file(ObPLogFileStruct &log_struct, const bool redirect_flag, const bool open_wf_flag);

  //@brief Set whether checking the log-file at each message logging.
  //@param[in] v 1:with check, 0:without check
  void set_check(const bool force_check) { force_check_ = force_check;}

  //@brief Set the max log-file size. The unit is byte. Default value and max value are 1GB.
  void set_max_file_size(int64_t max_file_size);
  //@brief Set the max number of log-files. If max_file_index = 0, no limit.
  int set_max_file_index(int64_t max_file_index = 0x0F);
  //@brief Set whether record old log file. If this flag and max_file_index set,
  //will record log files in the directory for log file
  int set_record_old_log_file(bool rec_old_file_flag = false);

  //@brief Get the process-only ObLogger.
  static ObLogger &get_logger();

  //@brief Regist par-module name with par_mod_id.
  int register_mod(const uint64_t par_mod_id, const char *par_mod_name);
  //@brief Regist sub-module name with par_mod_id and sub_mod_id.
  int register_mod(const uint64_t par_mod_id, const uint64_t sub_mod_id, const char *sub_mod_name);

  //@brief Set global log-file's level and warning log-file's level.
  //@param[in] level The log-file's level.
  //@param[in] wf_level The warning log-file's level.
  //@param[in] version The time(us) change the log level.
  void set_log_level(const char *level, const char *wf_level = NULL, int64_t version = 0);
  //@brief Set global level.
  void set_log_level(const int32_t level, int64_t version = 0)
  {set_log_level(static_cast<int8_t>(level), version);}
  void set_log_level(const int8_t level, int64_t version = 0);
  int set_mod_log_levels(const char* level_str, int64_t version = 0);

  void down_log_level(int64_t version = 0) {set_log_level(id_level_map_.get_level() + 1, version);}
  void up_log_level(int64_t version = 0) {set_log_level(id_level_map_.get_level() - 1, version);}

  int32_t get_level() const {return id_level_map_.get_level();}
  //@brief current log level, with session log level considered.
  int32_t get_log_level() const;
  int32_t get_log_level(const uint64_t par_mod_id) const;
  int32_t get_log_level(const uint64_t par_mod_id, const uint64_t sub_mod_id) const;
  inline const char *get_level_str() const { return errstr_[id_level_map_.get_level()]; }

  void disable_thread_log_level() { disable_thread_log_level_ = true; }

  //@brief Get the int8_t type representing the level in string.
  int level_str2int(const char *level_name, int8_t &level_int);

  int parse_check(const char *str, const int32_t str_length);
  //@brief Parse the string like "ALL.*:INFO, SQL.ENG:DEBUG, COMMON.*:ERROR",
  //and set the global level map.
  int parse_set(const char *str, const int32_t str_length, int64_t version = 0);
  //@brief Parse the string like "ALL.*:INFO, SQL.ENG:DEBUG, COMMON.*:ERROR",
  //and set the level map.
  int parse_set(const char *str,
                const int32_t str_length,
                int32_t &valid_length,
                ObLogIdLevelMap &id_level_map);

  //@brief get name_id_map_
  const ObLogNameIdMap &get_name_id_map() const { return name_id_map_; }
  //@brief get id_level_map_
  const ObLogIdLevelMap &get_id_level_map() const { return id_level_map_; }

  void set_enable_log_limit(bool enable_log_limit) {
    enable_log_limit_ = enable_log_limit;
  }

  static void set_tl_limiter(::oceanbase::lib::ObRateLimiter &limiter)
  {
    tl_log_limiter_ = &limiter;
  }

  static void set_tl_type(const int32_t type)
  {
    tl_type_ = type;
  }

  static void set_default_limiter(::oceanbase::lib::ObRateLimiter &limiter)
  {
    default_log_limiter_ = &limiter;
  }

  OB_INLINE bool& get_guard()
  {
    RLOCAL_INLINE(bool, guard);
    return guard;
  }

  void set_new_file_info(const char *info) { new_file_info_ = info; }
  void set_info_as_wdiag(const bool v) { info_as_wdiag_ = v; }
  bool is_info_as_wdiag() const { return info_as_wdiag_; }

  // issue a ERROR message for each EDIAG log right now
  void issue_dba_error(const int errcode, const char *file, const int line, const char *info_str);

private:
  //@brief If version <= 0, return true.
  //If version > 0, return version > level_version_ and if true, update level_version_.
  bool check_and_set_level_version(int64_t version);

  int log_tail(int32_t level, char *buf, const int64_t buf_len, int64_t &pos);

  int log_head(const int64_t ts,
               const char *mod_name,
               const int32_t level,
               const char *file,
               const int32_t line,
               const char *function,
               const int errcode,
               char *buf, const int64_t buf_len, int64_t &pos);

  void insert_warning_buffer_line_column_info(const UserMsgLevel user_msg_level,
                                                      const int line,
                                                      const int column);

  int get_mod_set(const char *par_mod,
                  const char *sub_mod,
                  const char *level,
                  ModSetting &mod_set);

  //@brief When setting mod_setting_list = NULL, do not return mod_setting_list.
  int parse_check(const char *str,
                  const int32_t str_length,
                  int32_t &valid_length,
                  void *mod_setting_list/*ObList<ModSetting>*/);

  int setting_list_processing(ObLogIdLevelMap &id_level_map,
                              void *mod_setting_list/*ObList<ModSetting>* */);
  //@brief record log files in the log directory. It's used when max_file_index and rec_old_file_flag_ set.
  int record_old_log_file();

  int get_log_files_in_dir(const char *filename,
                           void *files/*ObIArray<FileName> * */,
                           void *wf_files/*ObIArray<FileName> * */);

  int add_files_to_list(void *files/*ObIArray<FileName> * */,
                        void *wf_files/*ObIArray<FileName> * */,
                        std::deque<std::string> &file_list,
                        std::deque<std::string> &wf_file_list);

  void rotate_log(const int64_t size, const bool redirect_flag,
                  ObPLogFileStruct &log_struct, const ObPLogFDType fd_type);
  //@brief Rename the log to a filename with fmt. And open a new file with the old, then add old file to file_list.
  //@param[in] filename the old filename to rotate.
  //@param[in] fd_type.
  //@param[in] whether redirect, FALSE:redirect TRUE:no redirect
  //@param[out] after retated log, open new file_fd
  //@param[out] after retated wf log, open new wf_file_fd
  //@param[out] add retated log file name to file list
  //@param[out] add retated wf log file name to file list
  void rotate_log(const char *filename,
                  const ObPLogFDType fd_type,
                  const bool redirect_flag,
                  int32_t &fd,
                  int32_t &wf_fd,
                  std::deque<std::string> &file_list,
                  std::deque<std::string> &wf_file_list);

  static void *async_flush_log_handler(void *arg);
  void flush_logs_to_file(ObPLogItem **log_item, const int64_t count);

  int try_upgrade_log_item(ObPLogItem *&log_item, bool &upgrade_result);

  void check_log_end(ObPLogItem &log_item, int64_t pos);

  int backtrace_if_needed(ObPLogItem &log_item, const bool force);
  int check_tl_log_limiter(const uint64_t location_hash_val, const int32_t level, const int errcode,
                           const int64_t log_size, bool &allow, const char *&limiter_info);
  bool need_print_log_limit_msg();

  int alloc_log_item(const int32_t level, const int64_t size, ObPLogItem *&log_item);
  void free_log_item(ObPLogItem *log_item);
  void inc_dropped_log_count(const int32_t level);
  template<typename Function>
  void do_log_message(const bool is_async,
                      const char *mod_name,
                      int32_t level,
                      const char *file,
                      int32_t line,
                      const char *function,
                      const bool with_head,
                      const uint64_t location_hash_val,
                      const int errcode,
                      Function &log_data_func);

  template<typename Function>
  void log_it(const char *mod_name,
              const int32_t level,
              const char *file,
              const int32_t line,
              const char *function,
              const uint64_t location_hash_val,
              const int errcode,
              Function &&log_data_func);
  void check_probe(
      const char* file,
      int32_t line,
      const uint64_t location_hash_val,
      bool& force_bt,
      bool& disable);

  int log_new_file_info(const ObPLogFileStruct &log_file);
private:
  static const char *const errstr_[];
  // default log rate limiter if there's no tl_log_limiger
  static ::oceanbase::lib::ObRateLimiter *default_log_limiter_;
  RLOCAL_STATIC(lib::ObRateLimiter*, tl_log_limiter_);
  static constexpr int N_LIMITER = 4096;
  static ObSyslogSampleRateLimiter per_log_limiters_[N_LIMITER];
  RLOCAL_STATIC(int32_t, tl_type_);
  //used for stat logging time and log dropped
  RLOCAL_STATIC(uint64_t, curr_logging_seq_);
  RLOCAL_STATIC(uint64_t, last_logging_seq_);
  RLOCAL_STATIC(uint64_t, last_logging_cost_time_us_);
  // NOTE: whether to stop logging
  RLOCAL_STATIC(bool, disable_logging_);
  RLOCAL_STATIC(bool, trace_mode_);

  ObPLogFileStruct log_file_[MAX_FD_FILE];

  int64_t max_file_size_;
  int64_t max_file_index_;

  pthread_mutex_t file_size_mutex_;
  pthread_mutex_t file_index_mutex_;

  //log level
  ObLogNameIdMap name_id_map_;
  ObLogIdLevelMap id_level_map_;//level of log-file
  int8_t wf_level_;//level of warning log-file
  int64_t level_version_;//version of log level

  // Whether to disable the Thread Log Level function, the default is false, that is, the Thread Log Level is not disabled
  //
  // The purpose is to do not require Thread Log Level scenarios,
  // Optimize performance to avoid accessing ObThreadLogLevel structure every time get_log_level()
  bool disable_thread_log_level_;

  bool force_check_;//whether check log-file at each message logging.
  bool redirect_flag_;//whether redirect, TRUE: redirect FALSE: no redirect.
  bool open_wf_flag_;//whether open warning log-file.
  bool enable_wf_flag_; //whether write waring log to wf log-file.
  bool rec_old_file_flag_;//whether record old file.
  volatile bool can_print_;//when disk has no space, logger control

  bool enable_async_log_;//if false, use sync way logging
  bool use_multi_flush_;//whether use multi flush, default false
  bool stop_append_log_;//whether stop product log
  bool enable_perf_mode_;
  //used for statistics
  int64_t dropped_log_count_[OB_LOG_LEVEL_MAX];
  int64_t last_async_flush_count_per_sec_;

  int64_t dropped_count_[MAX_TASK_LOG_TYPE + 1];//last one is force allow count
  int64_t written_count_[MAX_TASK_LOG_TYPE + 1];
  int64_t current_written_count_[MAX_TASK_LOG_TYPE + 1];
  ObBlockAllocMgr* log_mem_limiter_;
  ObVSliceAlloc* allocator_;
  ObFIFOAllocator* error_allocator_;
  // juse use it for test promise log print
  bool enable_log_limit_;
  RLOCAL_STATIC(ByteBuf<LOCAL_BUF_SIZE>, local_buf_);
  struct {
    ProbeAction action_;
    char file_[128];
    int line_;
    uint64_t location_hash_val_;
  } probes_[8];
  int probe_cnt_ = 0;
  bool is_arb_replica_;

  // This info will be logged when log file created.
  const char *new_file_info_;
  bool info_as_wdiag_;
};

inline ObLogger& ObLogger::get_logger()
{
  static ObLogger logger;
  return logger;
}

inline int8_t ObLogIdLevelMap::get_level(uint64_t par_mod_id) const
{
  return (par_mod_id < MAX_PAR_MOD_SIZE) ?
         log_level_[par_mod_id][0] : static_cast<int8_t>(OB_LOG_LEVEL_NP);
}

inline int8_t ObLogIdLevelMap::get_level(uint64_t par_mod_id, uint64_t sub_mod_id) const
{
  return (par_mod_id < MAX_PAR_MOD_SIZE && sub_mod_id < MAX_SUB_MOD_SIZE) ?
         log_level_[par_mod_id][sub_mod_id + 1] : static_cast<int8_t>(OB_LOG_LEVEL_NP);
}

inline void ObThreadLogLevelUtils::init()
{
  get_id_level_map_() = NULL;
}

inline void ObThreadLogLevelUtils::init(const ObLogIdLevelMap *id_level_map)
{
  get_id_level_map_() = id_level_map;
  get_level_() = (id_level_map == NULL
                  ? (int8_t)OB_LOG_LEVEL_NONE : id_level_map->non_mod_level_);
}

inline void ObThreadLogLevelUtils::init(const int8_t level)
{
  get_level_() = level;
}

inline void ObThreadLogLevelUtils::clear()
{
  get_id_level_map_() = NULL;
  get_level_() = OB_LOG_LEVEL_NONE;
}

inline const ObLogIdLevelMap *ObThreadLogLevelUtils::get()
{
  return get_id_level_map_();
}

inline int8_t ObThreadLogLevelUtils::get_level()
{
  return get_level_();
}

inline const ObLogIdLevelMap*& ObThreadLogLevelUtils::get_id_level_map_()
{
  thread_local const ObLogIdLevelMap* id_level_map = nullptr;
  return id_level_map;
}

inline int8_t& ObThreadLogLevelUtils::get_level_()
{
  thread_local int8_t level = OB_LOG_LEVEL_NONE;
  return level;
}

inline void ObLogger::check_log_end(ObPLogItem &log_item, int64_t pos)
{
  const int64_t buf_size = log_item.get_buf_size();
  if (buf_size > 0) {
    if (pos < 0) {
      pos = 0;
    } else if (pos > buf_size - 2) {
      pos = buf_size - 2;
    }
    char *data = log_item.get_buf();
    if (pos > 0 && data[pos - 1] != '\n') {
      data[pos++] = '\n';
    }
    data[pos] = '\0';
    log_item.set_data_len(pos);
  }
}

template<typename Function>
void ObLogger::log_it(const char *mod_name,
            const int32_t level,
            const char *file,
            const int32_t line,
            const char *function,
            const uint64_t location_hash_val,
            const int errcode,
            Function &&log_data_func)
{
    int ret = OB_SUCCESS;
    if (OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG)
        && OB_LIKELY(level >= OB_LOG_LEVEL_DBA_ERROR)
        && OB_LIKELY(is_enable_logging())
        && OB_NOT_NULL(mod_name) && OB_NOT_NULL(file) && OB_NOT_NULL(function)
        && OB_NOT_NULL(function)) {
      if (is_trace_mode()) {
        TraceBuffer *tb = nullptr;
        if (OB_NOT_NULL(tb = get_trace_buffer())) {
          /* loop 2 times at most */
          while (true) {
            char *buf = tb->buffer_;
            int64_t buf_len = tb->get_cap();
            int64_t &pos = tb->get_pos();
            int64_t orig_pos = pos;
            ret = log_head(log_cur_ts(), mod_name, level, file, line, function, errcode, buf, buf_len, pos);
            if (OB_SUCC(ret)) {
              ret = log_data_func(buf, buf_len, pos);
            }
            if (OB_SUCC(ret))  {
              ret = log_tail(level, buf, buf_len, pos);
            }
            if (OB_SIZE_OVERFLOW == ret && orig_pos > 0) {
              /* restore pos */
              tb->set_pos(orig_pos);
              print_trace_buffer(mod_name, level, file, line, function, location_hash_val);
              /* retry */
              continue;
            }
            break;
          }
        }
      } else {
        do_log_message(is_async_log_used(), mod_name, level, file, line, function, true,
                       location_hash_val, errcode, log_data_func);
      }
    }
    UNUSED(ret);
}

inline void ObLogger::log_message_fmt(const char *mod_name,
                 const int32_t level,
                 const char *file,
                 const int32_t line,
                 const char *function,
                 const uint64_t location_hash_val,
                 const int errcode,
                 const char *fmt, ...)
{
  if (OB_NOT_NULL(fmt)) {
    va_list args;
    va_start(args, fmt);
    DEFER(va_end(args));
    log_message_va(mod_name, level, file, line, function, location_hash_val, errcode, fmt, args);
  }
}

inline void ObLogger::log_message_va(const char *mod_name,
                 const int32_t level,
                 const char *file,
                 const int32_t line,
                 const char *function,
                 const uint64_t location_hash_val,
                 const int errcode,
                 const char *fmt,
                 va_list args)
{
  if (OB_NOT_NULL(fmt)) {
    auto &&log_data_func = [&](char *buf, const int64_t buf_len, int64_t &pos) {
      va_list args_cpy;
      va_copy(args_cpy, args);
      DEFER(va_end(args_cpy));
      return logdata_vprintf(buf, buf_len, pos, fmt, args_cpy);
    };
    log_it(mod_name, level, file, line, function, location_hash_val, errcode, log_data_func);
  }
}

bool __attribute__((weak, noinline, cold)) ObLogger::need_to_print(const uint64_t par_mod_id, const int32_t level)
{
  return (level <= get_log_level(par_mod_id));
}

bool __attribute__((weak, noinline, cold)) ObLogger::need_to_print(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                                    const int32_t level)
{
  return (level <= get_log_level(par_mod_id, sub_mod_id));
}

inline int32_t ObLogger::get_log_level() const
{
  int8_t cur_level = OB_LOG_LEVEL_WARN;
  if (enable_perf_mode_) {
    cur_level = OB_LOG_LEVEL_DBA_ERROR;
  } else {
    cur_level = id_level_map_.get_level();

    if (! disable_thread_log_level_) {
      const ObLogIdLevelMap *session_id_level_map = ObThreadLogLevelUtils::get();
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
  int8_t cur_level = OB_LOG_LEVEL_WARN;
  if (enable_perf_mode_) {
    cur_level = OB_LOG_LEVEL_DBA_ERROR;
  } else {
    cur_level = id_level_map_.get_level(par_mod_id);

    if (! disable_thread_log_level_) {
      const ObLogIdLevelMap *session_id_level_map = ObThreadLogLevelUtils::get();
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
  int8_t cur_level = OB_LOG_LEVEL_WARN;
  if (enable_perf_mode_) {
    cur_level = OB_LOG_LEVEL_DBA_ERROR;
  } else {
    cur_level = id_level_map_.get_level(par_mod_id, sub_mod_id);

    if (! disable_thread_log_level_) {
      const ObLogIdLevelMap *session_id_level_map = ObThreadLogLevelUtils::get();
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

inline int ObLogger::set_mod_log_levels(const char *level_str, int64_t version)
{
  return parse_set(level_str, static_cast<int32_t>(std::strlen(level_str)), version);
}

extern void __attribute__ ((noinline)) on_probe_abort();

inline void ObLogger::check_probe(
      const char* file,
      int32_t line,
      const uint64_t location_hash_val,
      bool& force_bt,
      bool& disable)
{
  force_bt = false;
  disable = false;
  for (int i = 0; i < probe_cnt_; i++) {
    auto &probe = probes_[i];
    if (location_hash_val == probe.location_hash_val_ &&
        line == probe.line_ &&
        ProbeAction::PROBE_NONE != probe.action_) {
      const char *filename = NULL;
      if (NULL == (filename = strrchr(file, '/'))) {
        filename = file;
      } else {
        filename++;
      }
      if (0 == strncmp(filename, probe.file_, sizeof probe.file_)) {
        switch (probe.action_) {
          case ProbeAction::PROBE_BT: {
            force_bt = true;
            break;
          }
          case ProbeAction::PROBE_ABORT: {
            on_probe_abort();
            break;
          }
          case ProbeAction::PROBE_DISABLE: {
            disable = true;
            break;
          }
          case ProbeAction::PROBE_STACK: {
            IGNORE_RETURN faststack();
            break;
          }
          default: {
            // do nothing
            break;
          }
        }
        break;
      }
    }
  }
}

template<typename Function>
inline void ObLogger::do_log_message(const bool is_async,
                                     const char *mod_name,
                                     int32_t level,
                                     const char *file,
                                     int32_t line,
                                     const char *function,
                                     const bool with_head,
                                     const uint64_t location_hash_val,
                                     const int errcode,
                                     Function &log_data_func)
{
  int ret = OB_SUCCESS;
  if(!is_enable_logging()) return;
  bool old_val = set_disable_logging(true);
  DEFER(set_disable_logging(old_val));
  bool allow = true;
  const char *limiter_info = "";

  bool force_bt = false;
  bool disable = false;
  check_probe(file, line, location_hash_val, force_bt, disable);
  if(OB_UNLIKELY(disable)) return;
  auto fd_type = get_fd_type(mod_name);
  const int64_t log_size = limited_left_log_size_ + NORMAL_LOG_SIZE;
  limited_left_log_size_ = 0;
  ObSyslogTimeGuard tg;
  if (FD_TRACE_FILE != fd_type && OB_FAIL(check_tl_log_limiter(location_hash_val, level, errcode, log_size,
          allow, limiter_info))) {
    LOG_STDERR("precheck_tl_log_limiter error, ret=%d\n", ret);
  } else if (OB_UNLIKELY(!allow) && !need_print_log_limit_msg()) {
    inc_dropped_log_count(level);
  } else {
    ++curr_logging_seq_;
    // format to local buf
    ObPLogItem *log_item = new (local_buf_) ObPLogItem();
    log_item->set_buf_size(MAX_LOG_SIZE);
    log_item->set_log_level(level);
    log_item->set_timestamp(tg.get_start_ts());
    log_item->set_tl_type(tl_type_);
    log_item->set_force_allow(is_force_allows());
    log_item->set_fd_type(fd_type);

    char *buf = log_item->get_buf();
    int64_t buf_len = log_item->get_buf_size();
    int64_t pos = log_item->get_data_len();
    if (with_head) {
      if (OB_FAIL(log_head(tg.get_start_ts(), mod_name, level, file, line, function, errcode,
                           buf, buf_len, pos))) {
        LOG_STDERR("log_header error ret = %d\n", ret);
      }
    }
    if (OB_SUCC(ret)) {
      log_item->set_data_len(pos);
      log_item->set_header_len(pos);
      if (OB_FAIL(log_data_func(buf, buf_len, pos))) {
        LOG_STDERR("log data error ret = %d\n", ret);
      }
      check_log_end(*log_item, pos);
      if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
        log_item->set_size_overflow();
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(backtrace_if_needed(*log_item, force_bt))) {
          LOG_STDERR("backtrace_if_needed error ret = %d\n", ret);
        }
      }
    }

    if (OB_SUCC(ret) && !allow) {
      int64_t pos = log_item->get_header_len();
      if (OB_FAIL(logdata_print_info(log_item->get_buf(), log_item->get_buf_size(), pos,
                                     limiter_info))) {
        // do nothing
      } else {
        check_log_end(*log_item, pos);
      }
    }
    tg.click("FORMAT_END");


    if (OB_SUCC(ret)) {
      limited_left_log_size_ = std::max(0L, log_item->get_data_len() - NORMAL_LOG_SIZE);
      if (is_async) {
        // clone by data_size
        ObPLogItem *new_log_item = nullptr;
        if (OB_FAIL(alloc_log_item(level, LOG_ITEM_SIZE + log_item->get_data_len(), new_log_item))) {
          LOG_STDERR("alloc_log_item error, ret=%d\n", ret);
        } else {
_Pragma("GCC diagnostic push")
#ifdef __clang__
_Pragma("GCC diagnostic ignored \"-Wdynamic-class-memaccess\"")
#endif
          MEMCPY((void *)new_log_item, (void *)log_item, LOG_ITEM_SIZE + log_item->get_data_len());
_Pragma("GCC diagnostic pop")
            // update buf_size
          new_log_item->set_buf_size(log_item->get_data_len());
          log_item = new_log_item;
          tg.click("ALLOC_END");
        }

        if (OB_SUCC(ret)) {
          const int32_t tl_type = log_item->get_tl_type();
          if (OB_FAIL(append_log(*log_item))) {
            LOG_STDERR("append_log error ret = %d\n", ret);
          } else {
            // can't access log_item after append_log
            if (tl_type >= 0 && tl_type < MAX_TASK_LOG_TYPE) {
              (void)ATOMIC_AAF(current_written_count_ + tl_type, 1);
            }
            last_logging_seq_ = curr_logging_seq_;
            tg.click("APPEND_END");
          }
        }
      } else {
        flush_logs_to_file(&log_item, 1);
        tg.click("FLUSH_END");
      }

      // stat
      if (OB_FAIL(ret)) {
        inc_dropped_log_count(level);
        if ((char*)log_item != local_buf_) {
          free_log_item(log_item);
        }
        log_item = NULL;
        tg.click("FREE_END");
      }
      check_reset_force_allows();
    } /* not allow */
  }
#ifndef OB_BUILD_RPM
  const int64_t threshold_us = 500 * 1000;
#else
  const int64_t threshold_us = 1000 * 1000;
#endif
  const int64_t cost_time = tg.get_diff();
  if (OB_UNLIKELY(cost_time > threshold_us)) {
    char buf[512] = {'\0'};
    const int64_t buf_len = sizeof buf;
    int64_t pos = 0;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(log_head(tg.get_start_ts(), mod_name, OB_LOG_LEVEL_ERROR, file, line, function,
                             errcode, buf, buf_len, pos))) {
    } else if (OB_TMP_FAIL(logdata_printf(buf, buf_len, pos,
                                          "LOGGER COST TOO MUCH TIME, cost: %ld, ", cost_time))) {
    } else {
      pos += tg.to_string(buf + pos, buf_len - pos);
      fprintf(stderr, "%.*s, BACKTRACE: %s\n", static_cast<int>(pos), buf, lbt());
    }
  }
  last_logging_cost_time_us_ = cost_time;
}

template <typename ... Args>
void OB_PRINT(const char *mod_name, const int32_t level, const char *file, const int32_t line,
              const char *function, const uint64_t location_hash_val,
              const int errcode, const char *info_string,
              const char *, /* placeholder */
              Args const && ... args)
{
  int ret = OB_SUCCESS;
  if (OB_LOG_LEVEL_ERROR == level) {
    OB_LOGGER.issue_dba_error(errcode, file, line, info_string);
  }
  if (OB_LIKELY(!OB_LOGGER.get_guard())) {
    OB_LOGGER.get_guard() = true;
    OB_LOGGER.log_message_kv(mod_name, level, file, line, function, location_hash_val, errcode,
                             info_string, std::forward<const Args&&>(args)...);
    OB_LOGGER.get_guard() = false;
    UNUSED(ret);
  }
}

} // common
} // oceanbase
#endif
