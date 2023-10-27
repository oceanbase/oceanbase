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

#define USING_LOG_PREFIX LIB
#include "lib/oblog/ob_log.h"

#include <string.h>
#include <sys/uio.h>
#include <dirent.h>
#include <libgen.h>
#include <sys/prctl.h>
#include <linux/prctl.h>
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/ob_errno.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/ob_define.h"
#include "lib/list/ob_list.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/utility/ob_rate_limiter.h"
#include "lib/container/ob_vector.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "common/ob_smart_var.h"

using namespace oceanbase::lib;

namespace oceanbase
{
namespace common
{

int64_t ObSyslogTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (click_count_ > 0) {
    ret = databuff_printf(buf, buf_len, pos, "time dist: %s=%d", click_str_[0], click_[0]);
    for (int i = 1; OB_SUCC(ret) && i < click_count_; i++) {
      ret = databuff_printf(buf, buf_len, pos, ", %s=%d", click_str_[i], click_[i]);
    }
  }
  if (OB_FAIL(ret)) pos = 0;
  return pos;
}

void __attribute__((weak)) allow_next_syslog(int64_t)
{
  // do nothing
}

const char* __attribute__((weak)) ob_strerror(const int oberr)
{
  const char* ret = "ob_strerror";
  return ret;
}
_RLOCAL(ByteBuf<ObLogger::LOCAL_BUF_SIZE>, ObLogger::local_buf_);
extern void update_easy_log_level();
lib::ObRateLimiter *ObLogger::default_log_limiter_ = nullptr;
_RLOCAL(lib::ObRateLimiter*, ObLogger::tl_log_limiter_);
static const int64_t limiter_initial = 1000;
static const int64_t limiter_thereafter = 100;
ObSyslogSampleRateLimiter ObLogger::per_log_limiters_[];
_RLOCAL(int32_t, ObLogger::tl_type_);
_RLOCAL(uint64_t, ObLogger::curr_logging_seq_);
_RLOCAL(uint64_t, ObLogger::last_logging_seq_);
_RLOCAL(uint64_t, ObLogger::last_logging_cost_time_us_);
_RLOCAL(time_t, ObLogger::last_unix_sec_);
_RLOCAL(struct tm, ObLogger::last_localtime_);
_RLOCAL(bool, ObLogger::disable_logging_);
_RLOCAL(bool, ObLogger::trace_mode_);
_RLOCAL(int64_t, ObLogger::limited_left_log_size_);
static int64_t last_check_file_ts = 0; //last file sample timestamps
static int64_t last_check_disk_ts = 0; //last disk sample timestamps

const char *ObLogger::PERF_LEVEL = "PERF";
const char *const ObLogger::SECURITY_AUDIT_FILE_NAME_FORMAT = "audit/observer_%d_%04d%02d%02d%02d%02d%02d%03d.aud";
static const int NORMAL_LOG_INIT_MEM = 14L << 20; // 14M
static const int ERROR_LOG_INIT_MEM = 2L << 20; // 2M
// Calculated based on an average of 512 logs, 512K is enough
static const int64_t MAX_BUFFER_ITEM_CNT = 512 << 10;

static const int64_t POP_COMPENSATED_TIME[5] = {0, 1, 2, 3, 4};//for pop timeout

ObPLogFDType get_fd_type(const char *mod_name)
{
  ObPLogFDType type = FD_SVR_FILE;
  static const size_t RS_MODULE_LEN = strlen("[RS");
  static const size_t ELEC_MODULE_LEN = strlen("[ELECT");
  static const size_t FLT_MODULE_LEN = strlen("[FLT");
  if (0 == STRNCMP(mod_name, "[FLT", FLT_MODULE_LEN)) {
    type = FD_TRACE_FILE;
  } else if (ObThreadFlags::is_rs_thread()
             || 0 == STRNCMP(mod_name, "[RS", RS_MODULE_LEN)) {
    type = FD_RS_FILE;
  } else if (0 == STRNCMP(mod_name, "[ELECT", ELEC_MODULE_LEN)) {
    type = FD_ELEC_FILE;
  }
  return type;
}


int logdata_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  va_list args;
  va_start(args, fmt);
  ret = logdata_vprintf(buf, buf_len, pos, fmt, args);
  va_end(args);
  return ret;
}

int logdata_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != buf) && OB_LIKELY(0 <= pos && pos < buf_len)) {
    int len = vsnprintf(buf + pos, buf_len - pos, fmt, args);
    if (OB_UNLIKELY(len < 0)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_LIKELY(len < buf_len - pos)) {
      pos += len;
    } else {
      pos = buf_len - 1;  //skip '\0' written by vsnprintf
      ret = OB_SIZE_OVERFLOW;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint64_t obj)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != buf)
      && OB_LIKELY(0 <= pos)
      && OB_LIKELY(pos < buf_len)
      && OB_LIKELY(buf_len - pos >= ObFastFormatInt::MAX_DIGITS10_STR_SIZE)) {
    pos += ObFastFormatInt::format_unsigned(obj, buf + pos);
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int64_t obj)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != buf)
      && OB_LIKELY(0 <= pos)
      && OB_LIKELY(pos < buf_len)
      && OB_LIKELY(buf_len - pos >= ObFastFormatInt::MAX_DIGITS10_STR_SIZE)) {
    pos += ObFastFormatInt::format_signed(obj, buf + pos);
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

ObLogIdLevelMap &get_thread_log_id_level_map()
{
  RLOCAL(ObLogIdLevelMap, id_level_map);
  return id_level_map;
}

int process_thread_log_id_level_map(const char *str, const int32_t str_length)
{
  int ret = OB_SUCCESS;
  ObLogIdLevelMap &id_level_map = get_thread_log_id_level_map();
  int32_t valid_length = 0;
  id_level_map.reset_level();
  if (OB_FAIL(OB_LOGGER.parse_set(str, str_length, valid_length, id_level_map))) {
    LOG_WARN("Failed to parse set id_level_map", K(ret));
  } else {
    ObThreadLogLevelUtils::init(&id_level_map);
  }
  return ret;
}

void ObLogIdLevelMap::set_level(const int8_t level)
{
  non_mod_level_ = level;
  for (uint64_t par_mod_index = 0; par_mod_index < MAX_PAR_MOD_SIZE; ++par_mod_index) {
    for (uint64_t sub_mod_index = 0; sub_mod_index < MAX_SUB_MOD_SIZE + 1; ++sub_mod_index) {
      log_level_[par_mod_index][sub_mod_index] = level;
    }
  }
}

int ObLogIdLevelMap::set_level(const uint64_t par_mod_id, const int8_t level)
{
  int ret = OB_SUCCESS;
  if (par_mod_id < MAX_PAR_MOD_SIZE) {
    for (uint64_t sub_mod_index = 0; sub_mod_index < MAX_SUB_MOD_SIZE + 1; ++sub_mod_index) {
      log_level_[par_mod_id][sub_mod_index] = level;
    }
  } else {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WARN("Invalid mod id", K(ret), K(par_mod_id));
  }
  return ret;
}


int ObLogIdLevelMap::set_level(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                               const int8_t level)
{
  int ret = OB_SUCCESS;
  if (par_mod_id < MAX_PAR_MOD_SIZE && sub_mod_id < MAX_SUB_MOD_SIZE) {
    log_level_[par_mod_id][sub_mod_id + 1] = level;
  } else {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WARN("Invalid mod id", K(ret), K(par_mod_id), K(sub_mod_id));
  }
  return ret;
}

ObLogNameIdMap::ObLogNameIdMap()
{
  for (uint64_t par_index = 0; par_index < MAX_PAR_MOD_SIZE; ++par_index) {
    for (uint64_t sub_index = 0; sub_index < MAX_SUB_MOD_SIZE; ++sub_index) {
      name_id_map_[par_index][sub_index] = NULL;
    }
  }
}

int ObLogNameIdMap::register_mod(const uint64_t mod_id, const char *mod_name)
{
  int ret = OB_SUCCESS;
  if (mod_id >= MAX_PAR_MOD_SIZE || NULL == mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid mod id and name", K(ret), K(mod_id), KCSTRING(mod_name));
  } else {
    name_id_map_[mod_id][0] = mod_name;
  }
  return ret;
}

int ObLogNameIdMap::register_mod(const uint64_t mod_id,
                                 const uint64_t sub_mod_id,
                                 const char *sub_mod_name)
{
  int ret = OB_SUCCESS;
  if (mod_id >= MAX_PAR_MOD_SIZE || sub_mod_id >= MAX_SUB_MOD_SIZE || NULL == sub_mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid mod id or name", K(ret), K(mod_id), K(sub_mod_id), KCSTRING(sub_mod_name));
  } else {
    name_id_map_[mod_id][sub_mod_id + 1] = sub_mod_name;
  }
  return ret;
}

int ObLogNameIdMap::get_mod_id(const char *mod_name,
                               const char *sub_mod_name,
                               uint64_t &par_mod_id,
                               uint64_t &sub_mod_id) const
{
  int ret = OB_SUCCESS;
  par_mod_id = 0;
  sub_mod_id = 0;

  if (NULL == mod_name || NULL == sub_mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KCSTRING(mod_name), KCSTRING(sub_mod_name));
  } else if (OB_FAIL(get_mod_id(mod_name, par_mod_id))) {
    LOG_WARN("Failed to get mod id", K(ret), KCSTRING(mod_name));
  } else if (OB_UNLIKELY(par_mod_id >= MAX_PAR_MOD_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get invalid par mod id", K(ret));
  } else {
    //find sub_mod_id
    bool find_mod = false;
    for (uint64_t idx = 0; OB_SUCC(ret) && !find_mod && idx < MAX_SUB_MOD_SIZE; ++idx) {
      if (NULL == name_id_map_[par_mod_id][idx + 1]) {
        //do nothing
      } else if (0 == STRCASECMP(sub_mod_name, name_id_map_[par_mod_id][idx + 1])) {
        sub_mod_id = idx;
        find_mod = true;
      } else {
        //do nothing
      }
    }//end of for

    if (!find_mod) {
      ret = OB_LOG_MODULE_UNKNOWN;
      LOG_WARN("Failed to find sub_mod", K(ret), KCSTRING(mod_name), KCSTRING(sub_mod_name));
    }
  }
  return ret;
}

int ObLogNameIdMap::get_mod_id(const char *mod_name, uint64_t &par_mod_id) const
{
  int ret = OB_SUCCESS;
  par_mod_id = 0;

  if (NULL == mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KCSTRING(mod_name));
  } else {
    bool find_mod = false;
    for (uint64_t idx = 0; OB_SUCC(ret) && !find_mod && idx < MAX_PAR_MOD_SIZE; ++idx) {
      if (NULL == name_id_map_[idx][0]) {
        //do nothing
      } else if (0 == STRCASECMP(mod_name, name_id_map_[idx][0])) {
        par_mod_id = idx;
        find_mod = true;
      } else {
        //do nothing
      }
    }
    if (!find_mod) {
      ret = OB_LOG_MODULE_UNKNOWN;
      LOG_WARN("Failed to find sub_mod", K(ret), KCSTRING(mod_name));
    }
  }
  return ret;
}

ObPLogWriterCfg::ObPLogWriterCfg()
  : ObBaseLogWriterCfg(MAX_BUFFER_ITEM_CNT,
                       ObLogger::GROUP_COMMIT_MAX_WAIT_US,
                       ObLogger::GROUP_COMMIT_MIN_ITEM_COUNT,
                       ObLogger::GROUP_COMMIT_MAX_ITEM_COUNT)
{
}

int64_t ObLogger::FileName::to_string(char * buff, const int64_t len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buff) || OB_UNLIKELY(len <= 0)) {
  } else {
    pos = snprintf(buff, len, "%s", file_name_);
    if (OB_UNLIKELY(pos < 0)) {
      pos = 0;
    } else if (OB_UNLIKELY(pos >= len)) {
      pos = len - 1;
    } else { }//do nothing
  }
  return pos;
}

void __attribute__ ((noinline)) on_probe_abort()
{
  abort();
}

ProbeAction probe_str2action(const char *str)
{
  ProbeAction action = ProbeAction::PROBE_NONE;
  if (0 == strcmp(str, "bt")) {
    action = ProbeAction::PROBE_BT;
  } else if (0 == strcmp(str, "abort")) {
    action = ProbeAction::PROBE_ABORT;
  } else if (0 == strcmp(str, "disable")) {
    action = ProbeAction::PROBE_DISABLE;
  } else if (0 == strcmp(str, "stack")) {
    action = ProbeAction::PROBE_STACK;
  }
  return action;
}

const char *probe_action2str(ProbeAction action)
{
  const char *str = "none";
  switch (action) {
  case ProbeAction::PROBE_BT: {
      str = "bt";
      break;
    }
  case ProbeAction::PROBE_ABORT: {
    str = "abort";
    break;
    }
  case ProbeAction::PROBE_DISABLE: {
    str = "disable";
    break;
    }
  case ProbeAction::PROBE_STACK: {
    str = "stack";
    break;
    }
  default: {
    str = "none";
    break;
    }
  }
  return str;
}

int ObLogger::set_probe(char *str)
{
  probe_cnt_ = 0;
  char *saveptr = NULL;
  char *p = str;
  while ((p = strtok_r(p, "|", &saveptr)) != NULL &&
         probe_cnt_ < ARRAYSIZEOF(probes_)) {
    char *end = saveptr - 1;
    char *option = p;
    p = strstr(p, "@");
    if (p && p + 1 < end) {
      *p = '\0';
      char *file = p + 1;
      p = strstr(p + 1, ":");
      if (p && p + 1 < end) {
        *p = '\0';
        char *line = p + 1;
        ProbeAction action = probe_str2action(option);
        if (action != ProbeAction::PROBE_NONE) {
          auto &probe = probes_[probe_cnt_++];
          probe.action_ = action;
          strncpy(probe.file_, file, sizeof probe.file_);
          probe.file_[sizeof probe.file_ - 1] = '\0';
          probe.line_ = atoi(line);
          char location[256];
          snprintf(location, sizeof location, "%s:%s", file, line);
          probe.location_hash_val_ =
            oceanbase::common::hash::fnv_hash_for_logger(location, strlen(location) - 1);
        }
      }
    }
    p = saveptr;
  }
  return probe_cnt_;
}

const char *ObLogger::show_probe() const
{
  static char buf[256];
  int64_t buf_len = sizeof buf;
  int64_t pos = 0;
  for (int i = 0; i < probe_cnt_; i++) {
    auto &probe = probes_[i];
    int32_t size = snprintf(buf + pos, buf_len - pos, "%s@%s:%d%s",
             probe_action2str(probe.action_), probe.file_, probe.line_,
             (probe_cnt_ - 1) == i ? "" : "\n");
    if (size > buf_len - pos) {
      break;
    } else {
      pos += size;
    }
  }
  buf[pos] = '\0';
  return buf;
}

void ObLogger::TraceBuffer::reset()
{
  pos_ = 0;
}

void ObLogger::print_trace_buffer(const char* mod_name,
                                  const int32_t level,
                                  const char* file,
                                  int32_t line,
                                  const char* function,
                                  const uint64_t location_hash_val)
{
  TraceBuffer *tb = get_trace_buffer();
  if (tb->get_pos() > 0) {
    auto &&log_data_func = [&](char *buf, const int64_t buf_len, int64_t &pos) {
                             int64_t len = min(buf_len - pos, tb->get_pos());
                             // overlap
                             memmove(buf, tb->buffer_, len);
                             pos += len;
                             return OB_SUCCESS;
    };
    // invoke log_it isn't workable，that will recycle infinitely
    do_log_message(is_async_log_used(), mod_name, level, file, line, function,
                   false, location_hash_val, 0, log_data_func);
    tb->reset();//reset, than reuse the TraceBuffer
  }
}



const char *const ObLogger::errstr_[] = {"ERROR", "WARN", "INFO", "EDIAG", "WDIAG", "TRACE", "DEBUG"};

ObLogger::ObLogger()
  : ObBaseLogWriter(), log_file_(), max_file_size_(DEFAULT_MAX_FILE_SIZE), max_file_index_(0),
    name_id_map_(), id_level_map_(), wf_level_(OB_LOG_LEVEL_DBA_WARN), level_version_(0),
    disable_thread_log_level_(false), force_check_(false), redirect_flag_(false), open_wf_flag_(false),
    enable_wf_flag_(false), rec_old_file_flag_(false), can_print_(true),
    enable_async_log_(true), use_multi_flush_(false), stop_append_log_(false), enable_perf_mode_(false),
    last_async_flush_count_per_sec_(0), log_mem_limiter_(nullptr),
    allocator_(nullptr), error_allocator_(nullptr), enable_log_limit_(true), is_arb_replica_(false),
    new_file_info_(nullptr), info_as_wdiag_(true)
{
  id_level_map_.set_level(OB_LOG_LEVEL_DBA_ERROR);

  (void)pthread_mutex_init(&file_size_mutex_, NULL);
  (void)pthread_mutex_init(&file_index_mutex_, NULL);

  memset(dropped_log_count_, 0, sizeof(dropped_log_count_));
  memset(written_count_, 0, sizeof(written_count_));
  memset(dropped_count_, 0, sizeof(dropped_count_));
  memset(current_written_count_, 0, sizeof(current_written_count_));
}

ObLogger::~ObLogger()
{
  destroy();
  (void)pthread_mutex_destroy(&file_size_mutex_);
  (void)pthread_mutex_destroy(&file_index_mutex_);
}
void ObLogger::stop()
{
  OB_LOGGER.set_enable_async_log(false);
  ObBaseLogWriter::stop();
}
void ObLogger::wait()
{
  ObBaseLogWriter::wait();
}
void ObLogger::destroy()
{
  stop_append_log_ = true;
  enable_async_log_ = false;
  enable_log_limit_ = false;
  ObBaseLogWriter::destroy();
  if (error_allocator_) {
    error_allocator_->~ObFIFOAllocator();
    error_allocator_ = nullptr;
  }
  if (allocator_) {
    allocator_->~ObVSliceAlloc();
    allocator_ = nullptr;
  }
  if (log_mem_limiter_) {
    log_mem_limiter_->~ObBlockAllocMgr();
    ob_free(log_mem_limiter_);
    log_mem_limiter_ = nullptr;
  }
}

void ObLogger::set_trace_mode(bool trace_mode)
{
  trace_mode_ = trace_mode;
  get_trace_buffer()->reset();
}

void ObLogger::set_log_level(const char *level, const char *wf_level, int64_t version)
{
  int ret = OB_SUCCESS;
  if (check_and_set_level_version(version)) {
    if (NULL != level) {
      int8_t level_int = OB_LOG_LEVEL_INFO;
      if (OB_SUCC(level_str2int(level, level_int))) {
        set_log_level(level_int);
      }
    }

    if (NULL != wf_level) {
      int8_t level_int = OB_LOG_LEVEL_INFO;
      if (OB_SUCC(level_str2int(wf_level, level_int))) {
        wf_level_ = level_int;
      }
    }
  }
}

void ObLogger::set_log_level(const int8_t level, int64_t version)
{
  if (check_and_set_level_version(version)) {
    if (level >= 0 && level < static_cast<int8_t>(sizeof(errstr_) / sizeof(char *))) {
      id_level_map_.set_level(level);
    }
  }
  update_easy_log_level();
}

void ObLogger::set_file_name(const char *filename,
                             const bool no_redirect_flag,
                             const bool open_wf,
                             const char *rs_filename,
                             const char *elec_filename,
                             const char *trace_filename,
                             const char *audit_filename)
{
  int ret = OB_SUCCESS;
  redirect_flag_ = !no_redirect_flag;
  //open wf file
  open_wf_flag_ = open_wf;
  enable_wf_flag_ = open_wf;
  if (OB_FAIL(log_file_[FD_SVR_FILE].open(filename, open_wf, redirect_flag_))
      && OB_FAIL(log_new_file_info(log_file_[FD_SVR_FILE]))) {
    LOG_STDERR("fail to open log_file = %p, ret=%d\n", filename, ret);
  } else if (NULL != rs_filename && OB_FAIL(log_file_[FD_RS_FILE].open(rs_filename, open_wf, false))
             && OB_FAIL(log_new_file_info(log_file_[FD_RS_FILE]))) {
    LOG_STDERR("fail to open log_file = %p, ret=%d\n", rs_filename, ret);
  } else if (NULL != elec_filename && OB_FAIL(log_file_[FD_ELEC_FILE].open(elec_filename, open_wf, false))
             && OB_FAIL(log_new_file_info(log_file_[FD_ELEC_FILE]))) {
    LOG_STDERR("fail to open log_file = %p, ret=%d\n", elec_filename, ret);
  } else if (NULL != trace_filename && OB_FAIL(log_file_[FD_TRACE_FILE].open(trace_filename, false, false))) {
    LOG_STDERR("fail to open log_file = %p, ret=%d\n", trace_filename, ret);
  } else if (NULL != audit_filename && OB_FAIL(log_file_[FD_AUDIT_FILE].open(audit_filename, false, false))) {
    LOG_STDERR("fail to open log_file = %p, ret=%d\n", audit_filename, ret);
  }
}

ObLogger::TraceBuffer *ObLogger::get_trace_buffer()
{
  RLOCAL_INLINE(TraceBuffer*, tb);
  if (OB_UNLIKELY(NULL == tb)) {
    STATIC_ASSERT(sizeof(TraceBuffer) <= LOCAL_BUF_SIZE - LOG_ITEM_SIZE, "check sizeof TraceBuffer failed");
    tb = new (&local_buf_[0] + LOG_ITEM_SIZE) TraceBuffer(); /* LOG_ITEM_SIZE is reserved for ObPlogitem */
  }
  return tb;
}

char NEWLINE[1] = {'\n'};

void ObLogger::log_user_message(
    const UserMsgLevel user_msg_level,
    const int errcode,
    const char *fmt,
    ...)
{
  char buf[ObWarningBuffer::WarningItem::STR_LEN] = {};
  va_list args;
  va_start(args, fmt);
  int64_t len = vsnprintf(buf, ObWarningBuffer::WarningItem::STR_LEN, fmt, args);
  va_end(args);
  insert_warning_buffer(user_msg_level, errcode, buf, len);
}

bool ObLogger::check_and_set_level_version(int64_t version)
{
  bool refresh_level = true;
  if (version <= 0) {
    //do nothing
  } else if (version > level_version_) {
    level_version_ = version;
  } else {
    refresh_level = false;
  }
  return refresh_level;
}

int ObLogger::log_tail(int32_t level, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(pos >= 0)) {
    if (OB_UNLIKELY(OB_LOG_LEVEL_ERROR == level)) {
      const char *bt = oceanbase::common::lbt();
      ret = logdata_printf(buf, buf_len, pos,
                           " BACKTRACE:%s", bt);
    }
    if (pos >= buf_len) {
      pos = buf_len - 1;
    }
    while (pos > 0 && '\n' == buf[pos - 1]) {
      --pos;
    }
    buf[pos] = '\n';
    ++pos;
  }
  return ret;
}

void ts_to_tv(int64_t ts, timeval &tv)
{
  tv.tv_sec = static_cast<long>(ts / 1000000);
  tv.tv_usec = static_cast<long>(ts % 1000000);
}

int ObLogger::log_head(const int64_t ts,
                       const char *mod_name,
                       const int32_t level,
                       const char *file,
                       const int32_t line,
                       const char *function,
                       const int errcode,
                       char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (level >= 0 && level < static_cast<int>(sizeof(errstr_) / sizeof(char *))
      && NULL != mod_name && NULL != file && NULL != function) {
    //only print base filename.
    const char *base_file_name = strrchr(file, '/');
    base_file_name = (NULL != base_file_name) ? base_file_name + 1 : file;

    struct timeval tv;
    ts_to_tv(ts, tv);
    struct tm tm;
    ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(tv.tv_sec), &tm);
    const uint64_t *trace_id = ObCurTraceId::get();
    const int32_t errcode_buf_size = 32;
    char errcode_buf[errcode_buf_size];
    errcode_buf[0] = '\0';
    if (level == OB_LOG_LEVEL_DBA_ERROR
        || level == OB_LOG_LEVEL_DBA_WARN
        || level == OB_LOG_LEVEL_WARN
        || level == OB_LOG_LEVEL_ERROR) {
      snprintf(errcode_buf, errcode_buf_size, "[errcode=%d]", errcode);
    }
    if (get_fd_type(mod_name) == FD_TRACE_FILE) {
      //forbid modify the format of logdata_printf
      ret = logdata_printf(buf, buf_len, pos,
                           "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                           "[%ld][%s][T%lu][" TRACE_ID_FORMAT_V2 "] ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec, GETTID(), GETTNAME(), GET_TENANT_ID(), TRACE_ID_FORMAT_PARAM(trace_id));
    } else {
      constexpr int cluster_id_buf_len = 8;
      char cluster_id_buf[cluster_id_buf_len] = {'\0'};
      (void)snprintf(cluster_id_buf, cluster_id_buf_len, "[C%lu]", GET_CLUSTER_ID());
      ret = logdata_printf(buf, buf_len, pos,
                           "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                           "%-5s %s%s (%s:%d) [%ld][%s]%s[T%lu][" TRACE_ID_FORMAT_V2 "] [lt=%ld]%s ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec, errstr_[level], mod_name, function,
                           base_file_name, line, GETTID(), GETTNAME(), is_arb_replica_ ? cluster_id_buf : "",
                           is_arb_replica_ ? GET_ARB_TENANT_ID() : GET_TENANT_ID(), TRACE_ID_FORMAT_PARAM(trace_id),
                           last_logging_cost_time_us_, errcode_buf);
    }
  }
  return ret;
}

void ObLogger::rotate_log(const int64_t size, const bool redirect_flag,
    ObPLogFileStruct &log_struct, const ObPLogFDType fd_type)
{
  if (OB_LIKELY(size > 0) && max_file_size_ > 0 && log_struct.file_size_ >= max_file_size_) {
    if (OB_LIKELY(0 == pthread_mutex_trylock(&file_size_mutex_))) {
      rotate_log(log_struct.filename_, fd_type, redirect_flag, log_struct.fd_,
                 log_struct.wf_fd_, log_struct.file_list_, log_struct.wf_file_list_);
      (void)ATOMIC_SET(&log_struct.file_size_, 0);
      if (fd_type <= FD_ELEC_FILE) {
        (void)log_new_file_info(log_struct);
      }
      (void)pthread_mutex_unlock(&file_size_mutex_);
    }
  }
}

void ObLogger::rotate_log(const char *filename,
                          const ObPLogFDType fd_type,
                          const bool redirect_flag,
                          int32_t &fd,
                          int32_t &wf_fd,
                          std::deque<std::string> &file_list,
                          std::deque<std::string> &wf_file_list)
{
  int ret = OB_SUCCESS;
  if (NULL != filename) {
    if (ObPLogFDType::FD_AUDIT_FILE == fd_type) {
      if (access(filename, R_OK) == 0) {
        char old_log_file[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE] = {};
        struct timeval t;
        gettimeofday(&t, nullptr);
        struct tm tm;
        ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(t.tv_sec), &tm);
        (void)snprintf(old_log_file, sizeof(old_log_file), SECURITY_AUDIT_FILE_NAME_FORMAT,
                getpid(), tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(t.tv_usec/1000));

        ret = rename(filename, old_log_file); //If failed, TODO
        int tmp_fd = open(filename, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, ObPLogFileStruct::LOG_FILE_MODE);
        if (tmp_fd > 0) {
          if (fd > STDERR_FILENO) {
            (void)dup2(tmp_fd, fd);
            (void)close(tmp_fd);
          } else {
            fd = tmp_fd;
          }
        }
      }
    } else {
      char wf_filename[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
      memset(wf_filename, 0, sizeof(wf_filename));
      //Need to think how to deal failure.
      (void)snprintf(wf_filename, sizeof(wf_filename), "%s.wf", filename);
      if (access(filename, R_OK) == 0) {
        char old_log_file[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
        char old_wf_log_file[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
        memset(old_log_file, 0, sizeof(old_log_file));
        memset(old_wf_log_file, 0, sizeof(old_wf_log_file));
        struct timeval t;
        gettimeofday(&t, nullptr);
        struct tm tm;
        ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(t.tv_sec), &tm);
        (void)snprintf(old_log_file, sizeof(old_log_file), "%s.%04d%02d%02d%02d%02d%02d%03d",
                filename, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(t.tv_usec/1000));
        (void)snprintf(old_wf_log_file, sizeof(old_wf_log_file), "%s.%04d%02d%02d%02d%02d%02d%03d",
                 wf_filename, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(t.tv_usec/1000));

        if (max_file_index_ > 0) {
          if (OB_LIKELY(0 == pthread_mutex_lock(&file_index_mutex_))) {
            if (file_list.size() >= max_file_index_) {
              std::string oldFile = file_list.front();
              file_list.pop_front();
              unlink(oldFile.c_str());
            }
            file_list.push_back(old_log_file);
            (void)pthread_mutex_unlock(&file_index_mutex_);
          }
        }

        ret = rename(filename, old_log_file); //If failed, TODO
        int tmp_fd = open(filename, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, ObPLogFileStruct::LOG_FILE_MODE);
        if (tmp_fd > 0) {
          if (redirect_flag) {
            (void)dup2(tmp_fd, STDERR_FILENO);
            (void)dup2(tmp_fd, STDOUT_FILENO);
            if (fd > STDERR_FILENO) {
              (void)dup2(tmp_fd, fd);
              (void)close(tmp_fd);
            } else {
              fd = tmp_fd;
            }
          } else {
            if (fd > STDERR_FILENO) {
              (void)dup2(tmp_fd, fd);
              (void)close(tmp_fd);
            } else {
              fd = tmp_fd;
            }
          }
        }

        if (open_wf_flag_ && enable_wf_flag_) {
          if (max_file_index_ > 0) {
            if (OB_LIKELY(0 == pthread_mutex_lock(&file_index_mutex_))) {
              if (wf_file_list.size() >= max_file_index_) {
                std::string old_wf_file = wf_file_list.front();
                wf_file_list.pop_front();
                unlink(old_wf_file.c_str());
              }
              wf_file_list.push_back(old_wf_log_file);
              (void)pthread_mutex_unlock(&file_index_mutex_);
            }
          }
          ret = rename(wf_filename, old_wf_log_file); //If failed, TODO
          tmp_fd = open(wf_filename, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, ObPLogFileStruct::LOG_FILE_MODE);
          if (tmp_fd > 0) {
            if (wf_fd > STDERR_FILENO) {
              (void)dup2(tmp_fd, wf_fd);
              (void)close(tmp_fd);
            } else {
              wf_fd = tmp_fd;
            }
          }
        }
      }
    }
  }
  UNUSED(ret);
}

void ObLogger::check_file()
{
  check_file(log_file_[FD_SVR_FILE], redirect_flag_, open_wf_flag_);
  check_file(log_file_[FD_RS_FILE], false, false);
  check_file(log_file_[FD_AUDIT_FILE], false, false);
  check_file(log_file_[FD_ELEC_FILE], false, open_wf_flag_);
  check_file(log_file_[FD_TRACE_FILE], false, false);
}

void ObLogger::check_file(ObPLogFileStruct &log_struct, const bool redirect_flag, const bool open_wf_flag)
{
  if (log_struct.is_opened()) {
    struct stat st_file;
    int err = stat(log_struct.filename_, &st_file);
    if ((err == -1 && errno == ENOENT)
        || (err == 0 && (st_file.st_dev != log_struct.stat_.st_dev || st_file.st_ino != log_struct.stat_.st_ino))) {
      log_struct.reopen(redirect_flag);
    }

    if (open_wf_flag) {
      char wf_file_name[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
      memset(wf_file_name, 0, sizeof(wf_file_name));
      (void)snprintf(wf_file_name, sizeof(wf_file_name), "%s.wf", log_struct.filename_);
      err = stat(wf_file_name, &st_file);
      if ((err == -1 && errno == ENOENT)
          || (err == 0 && (st_file.st_dev != log_struct.wf_stat_.st_dev || st_file.st_ino != log_struct.wf_stat_.st_ino))) {
        (void)log_struct.reopen_wf();
      }
    }
  }
}

int ObLogger::register_mod(const uint64_t par_mod_id, const char *par_mod_name)
{
  return name_id_map_.register_mod(par_mod_id, par_mod_name);
}

int ObLogger::register_mod(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                           const char *sub_mod_name)
{
  return name_id_map_.register_mod(par_mod_id, sub_mod_id, sub_mod_name);
}

void ObLogger::set_max_file_size(int64_t max_file_size)
{
  //max file size 1GB
  if (max_file_size < 0x0 || max_file_size > 0x40000000) {
    max_file_size = 0x40000000;//1GB
  }
  max_file_size_ = max_file_size;
}

int ObLogger::set_max_file_index(int64_t max_file_index)
{
  int ret = OB_SUCCESS;
  if (max_file_index < 0x00) {
    max_file_index = 0x0F;//16
  }
  if (max_file_index > MAX_LOG_FILE_COUNT) {// 10 * 1024
    max_file_index = MAX_LOG_FILE_COUNT;// 10 * 1024
  }
  max_file_index_ = max_file_index;
  if (max_file_index_ > 0 && rec_old_file_flag_) {
    if (OB_FAIL(record_old_log_file())) {
      LOG_WARN("Record old log file error", K(ret));
    }
  }
  return ret;
}

int ObLogger::set_record_old_log_file(bool rec_old_file_flag)
{
  int ret = OB_SUCCESS;
  rec_old_file_flag_ = rec_old_file_flag;
  if (rec_old_file_flag_ && max_file_index_ > 0) {
    if (OB_FAIL(record_old_log_file())) {
      LOG_WARN("Record old log file error", K(ret));
    }
  }
  return ret;
}

//@brief string copy with dst's length and src's length checking and src trim.
int64_t str_copy_trim(char *dst,
                      const int64_t dst_length,
                      const char *src,
                      const int64_t src_length)
{
  int64_t length = 0;
  if (NULL != dst && NULL != src && dst_length > 0) {
    length = src_length;
    //left trim
    while (length != 0 && isspace(*src)) {
      length--;
      src++;
    }
    //right trim
    while (length != 0 && isspace(*(src + length - 1))) {
      length--;
    }
    length = (dst_length - 1) > length ? length : (dst_length - 1);
    MEMCPY(dst, src, length);
    dst[length] = '\0';
  }
  return length;
}

//@brief get sub-string from p_start to the location of delimiter
int get_delim_str(const char *&p_start,
                  const char *const p_end,
                  char delim,
                  char *dst_str,
                  const int32_t dst_str_size)
{
  int ret = 0;
  const char *p_delim = NULL;
  if ((p_start >= p_end) || (NULL == (p_delim = strchr(p_start, delim)))) {
    ret = -1;
  } else {
    str_copy_trim(dst_str, dst_str_size, p_start, p_delim - p_start);
    p_start = p_delim + 1;
  }
  return ret;
}

int ObLogger::parse_check(const char *str, const int32_t str_length)
{
  int32_t valid_length = 0;
  return parse_check(str, str_length, valid_length, NULL);
}

int ObLogger::parse_check(const char *str,
                          const int32_t str_length,
                          int32_t &valid_length,
                          void *mod_setting_list)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(char[OB_MAX_CONFIG_VALUE_LEN], buffer) {
    valid_length = 0;
    const int32_t MAX_MOD_NAME_LENGTH = 20;
    const int32_t MAX_LEVEL_NAME_LENGTH = 10;
    ObList<ModSetting> *list = NULL;
    if (NULL != mod_setting_list) {
      list = static_cast<ObList<ModSetting> *>(mod_setting_list);
    }
    if (NULL == str) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      str_copy_trim(buffer, OB_MAX_CONFIG_VALUE_LEN, str, str_length);
      //check log_level = "level", to set all modules.
      if (strchr(buffer, ':') == NULL) {
        int8_t level_int = 0;
        if (OB_FAIL(level_str2int(buffer, level_int))) {
          OB_LOG(WARN, "failed to get level_int", KCSTRING(buffer), K(str_length), K(ret));
        } else {
          if (NULL != list) {
            ModSetting mod_set(ModSetting::NON_SUBMOD_ID, ModSetting::NON_SUBMOD_ID, level_int);
            list->push_back(mod_set);
          }
          valid_length = str_length;
        }
      } else { //check log_level like "ALL.*:INFO, COMMON.*:ERROR", to set some modules
        char par_mod[MAX_MOD_NAME_LENGTH];
        char sub_mod[MAX_MOD_NAME_LENGTH];
        char level[MAX_LEVEL_NAME_LENGTH];
        const char *p_start = buffer;
        const char *const p_end = buffer + strlen(buffer);

        while (OB_SUCC(ret) && p_start < p_end) {
          //get par-module name
          if (0 != get_delim_str(p_start, p_end, '.', par_mod, MAX_MOD_NAME_LENGTH)) {
            ret = OB_LOG_PARSER_SYNTAX_ERR;
          } else if (0 != get_delim_str(p_start, p_end, ':', sub_mod, MAX_MOD_NAME_LENGTH)) {
            ret = OB_LOG_PARSER_SYNTAX_ERR;
          } else if (0 != get_delim_str(p_start, p_end, ',', level, MAX_LEVEL_NAME_LENGTH)
                     && 0 != get_delim_str(p_start, p_end, ';', level, MAX_LEVEL_NAME_LENGTH)) {
            if (p_start >= p_end) {
              ret = OB_LOG_PARSER_SYNTAX_ERR;
            } else {
              str_copy_trim(level, MAX_LEVEL_NAME_LENGTH, p_start, p_end - p_start);
              p_start = p_end;
            }
          } else {
            //do nothing
          }

          if (OB_SUCC(ret)) {
            ModSetting mod_set;
            if (OB_FAIL(get_mod_set(par_mod, sub_mod, level, mod_set))) {
              LOG_WARN("Get mod set error", K(ret));
              if (NULL != list) {
                list->reset();
              }
            } else {
              if (NULL != list) {
                if (OB_FAIL(list->push_back(mod_set))) {
                  LOG_WARN("Failed to add mod set to list", K(ret));
                }
              }
              int64_t valid_length_tmp = p_start - buffer;
              if (valid_length_tmp > 0 && valid_length_tmp <= static_cast<int64_t>(str_length)) {
                valid_length = static_cast<int32_t>(valid_length_tmp);
              }
            }
          }
        }//end of while
      }
    }
    if (OB_LOG_PARSER_SYNTAX_ERR == ret
        || OB_LOG_MODULE_UNKNOWN == ret
        || OB_LOG_LEVEL_INVALID == ret) {
      _LOG_WARN("invalid log_level=%s, ret=%d", str + valid_length, ret);
    } else if (OB_INVALID_ARGUMENT == ret) {
      LOG_WARN("invalid argument", K(ret));
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObLogger::parse_set(const char *str, const int32_t str_length, int64_t version)
{
  int ret = OB_SUCCESS;
  int valid_length = 0;
  if (check_and_set_level_version(version)) {
    ret = parse_set(str, str_length, valid_length, id_level_map_);
  }
  return ret;
}

int ObLogger::parse_set(const char *str,
                        const int32_t str_length,
                        int32_t &valid_length,
                        ObLogIdLevelMap &id_level_map)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_LOG);
  ObList<ModSetting, ObArenaAllocator> mod_setting_list(allocator);
  const ObString tmp_str(str_length, str);
  if (&id_level_map == &id_level_map_ && 0 == tmp_str.case_compare(PERF_LEVEL)) {
    enable_perf_mode_ = true;
  } else {
    if (OB_FAIL(parse_check(str, str_length, valid_length, &mod_setting_list))) {
      LOG_WARN("Failed to parse check log level", K(ret));
    } else if (OB_FAIL(setting_list_processing(id_level_map, &mod_setting_list))) {
      LOG_WARN("Failed to process setting list", K(ret));
    } else {
      enable_perf_mode_ = false;
      //do nothing
    }
  }
  update_easy_log_level();
  return ret;
}

int ObLogger::setting_list_processing(ObLogIdLevelMap &id_level_map, void *mod_setting_list)
{
  int ret = OB_SUCCESS;
  ModSetting mod_set;
  if (OB_ISNULL(mod_setting_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Mod setting list should not be NULL", K(ret));
  } else {
    ObList<ModSetting> *list = static_cast<ObList<ModSetting> *>(mod_setting_list);
    for (; OB_SUCC(ret) && list->size() > 0;) {
      if (OB_FAIL(list->pop_front(mod_set))) {
        LOG_WARN("Failed to pop mod set", K(ret));
      } else {
        if (ModSetting::NON_SUBMOD_ID == mod_set.par_mod_id_) {
          id_level_map.set_level(mod_set.level_);
        } else if (ModSetting::NON_SUBMOD_ID == mod_set.sub_mod_id_) {
          if (OB_FAIL(id_level_map.set_level(mod_set.par_mod_id_, mod_set.level_))) {
            LOG_WARN("Failed to set log level", K(ret));
          }
        } else {
          if (OB_FAIL(id_level_map.set_level(mod_set.par_mod_id_,
                                             mod_set.sub_mod_id_,
                                             mod_set.level_))) {
                LOG_WARN("Failed to set log level", K(ret));
          }
        }
      }
    }//end of for
  }
  return ret;
}

int ObLogger::get_mod_set(const char *par_mod, const char *sub_mod, const char *level,
                          ModSetting &mod_set)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(level_str2int(level, mod_set.level_))) {
    LOG_WARN("Failed to convert level", K(ret));
  } else {
    if (0 == STRCASECMP(par_mod, "ALL") &&  0 == STRCASECMP(sub_mod, "*")) {
      mod_set.par_mod_id_ = ModSetting::NON_SUBMOD_ID;
    } else if (0 == STRCASECMP(sub_mod, "*")) {
      if (OB_FAIL(name_id_map_.get_mod_id(par_mod, mod_set.par_mod_id_))) {
        LOG_WARN("Failed to get mod id", K(ret), KCSTRING(par_mod));
      } else {
        mod_set.sub_mod_id_ = ModSetting::NON_SUBMOD_ID;
      }
    } else {
      if (OB_FAIL(name_id_map_.get_mod_id(par_mod, sub_mod, mod_set.par_mod_id_,
                                          mod_set.sub_mod_id_))) {
        LOG_WARN("Failed to get mod id", K(ret), KCSTRING(par_mod), KCSTRING(sub_mod));
      }
    }
  }
  return ret;
}

int ObLogger::level_str2int(const char *level_name, int8_t &level_int)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(level_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KCSTRING(level_name));
  } else {
    bool find_level = false;
    int8_t level_num = sizeof(errstr_) / sizeof(char *);
    for (int8_t level_index = 0;!find_level && level_index < level_num; level_index++) {
      if (0 == STRCASECMP(level_name, errstr_[level_index])) {
        level_int = level_index;
        find_level = true;
      }
    }//end of for
    if (!find_level) {
      ret = OB_LOG_LEVEL_INVALID;
      LOG_WARN("Invalid log level", K(ret));
    } else if (OB_LOG_LEVEL_INFO == level_int && info_as_wdiag_) {
      level_int = OB_LOG_LEVEL_WARN;
    }
  }
  return ret;
}

void ObLogger::insert_warning_buffer(const UserMsgLevel user_msg_level, const int errcode,
                                     const char *data, const int64_t data_len)
{
  if (ObWarningBuffer::is_warn_log_on() && data_len > 0) {
    ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
    if (NULL != wb) {
      if (user_msg_level == USER_ERROR) {
        wb->set_error(data, errcode);
      } else if (user_msg_level == USER_WARN) {
        wb->append_warning(data, errcode);
      } else if (user_msg_level == USER_NOTE) {
        wb->append_note(data, errcode);
      }
    } else {
      // OB_LOG(WARN, "wb is NULL", K(errcode));
      // BACKTRACE(ERROR, 1, "wb");
    }
  }
}

void ObLogger::insert_warning_buffer_line_column_info(const UserMsgLevel user_msg_level,
                                                      const int line,
                                                      const int column)
{
  if (ObWarningBuffer::is_warn_log_on()) {
    ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
    if (NULL != wb) {
      if (user_msg_level == USER_ERROR) {
        wb->set_error_line_column(line, column);
      } else {
        // not support yet.
      }
    }
  }
}

void ObLogger::log_user_error_line_column(const UserMsgLevel user_msg_level,
                             const int line,
                             const int column)
{
  insert_warning_buffer_line_column_info(user_msg_level, line, column);
}

int ObLogger::record_old_log_file()
{
  int ret = OB_SUCCESS;
  if (max_file_index_ <= 0 || !rec_old_file_flag_) {
  } else {
    for (int type = FD_SVR_FILE; type < FD_AUDIT_FILE; ++type) {
      ObSEArray<FileName, 4> files;
      ObSEArray<FileName, 4> wf_files;
      if (OB_FAIL(get_log_files_in_dir(log_file_[type].filename_, &files, &wf_files))) {
        OB_LOG(WARN, "Get log files in log dir error", K(ret));
      } else if (OB_FAIL(add_files_to_list(&files, &wf_files, log_file_[type].file_list_, log_file_[type].wf_file_list_))) {
        OB_LOG(WARN, "Add files to list error", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObLogger::get_log_files_in_dir(const char *filename, void *files, void *wf_files)
{
  int ret = OB_SUCCESS;
  char *dirc = NULL;
  char *basec = NULL;
  if (OB_ISNULL(files) || OB_ISNULL(wf_files)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "Input should not be NULL", K(files), K(wf_files), K(ret));
  } else if (OB_ISNULL(filename)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "filename has not been set", KCSTRING(filename), K(ret));
  } else if (NULL == (dirc = strdup(filename))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "strdup filename error", K(ret));
  } else if (NULL == (basec = strdup(filename))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "strdup filename error", K(ret));
  } else {
    ObIArray<FileName> *files_arr = static_cast<ObIArray<FileName> *>(files);
    ObIArray<FileName> *wf_files_arr = static_cast<ObIArray<FileName> *>(wf_files);
    //get dir and base name
    char *dir_name = dirname(dirc);
    char *base_name = basename(basec);
    //get file_prefix, wf_file_prefix, wf_file names
    char file_prefix[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
    char wf_file_prefix[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
    char wf_file[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
    memset(file_prefix, 0, sizeof(file_prefix));
    memset(wf_file_prefix, 0, sizeof(wf_file_prefix));
    memset(wf_file, 0, sizeof(wf_file));
    (void)snprintf(file_prefix, sizeof(file_prefix), "%s.", base_name);
    (void)snprintf(wf_file_prefix, sizeof(wf_file_prefix), "%s.wf.", base_name);
    (void)snprintf(wf_file, sizeof(wf_file), "%s.wf", base_name);
    //open dir
    DIR *dir_pointer = opendir(dir_name);
    if (NULL == dir_pointer) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "Open dir error", KCSTRING(dir_name), K(ret));
    } else {
      FileName tmp_file;
      struct dirent *dir_entry = NULL;//dir_entry is from dir_pointer stream, need not to be freed.
      int64_t print_len = 0;
      while (OB_SUCC(ret) && (dir_entry = readdir(dir_pointer)) != NULL) {
        if (DT_DIR != dir_entry->d_type) {
          if (prefix_match(wf_file_prefix, dir_entry->d_name)) {
            print_len = snprintf(tmp_file.file_name_, ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE, "%s/%s", dir_name, dir_entry->d_name);
            if (OB_UNLIKELY(print_len <0) || OB_UNLIKELY(print_len >= ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE)) {
              //do nothing
            } else if (OB_FAIL(wf_files_arr->push_back(tmp_file))) {
              LOG_WARN("Add file to wf files error", K(ret));
            } else { }//do nothing
          } else if (prefix_match(wf_file, dir_entry->d_name)) {
            //.wf file, do nothing.
          } else if (prefix_match(file_prefix, dir_entry->d_name)) {
            print_len = snprintf(tmp_file.file_name_, ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE, "%s/%s", dir_name, dir_entry->d_name);
            if (OB_UNLIKELY(print_len <0) || OB_UNLIKELY(print_len >= ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE)) {
              //do nothing
            } else if (OB_FAIL(files_arr->push_back(tmp_file))) {
              LOG_WARN("Add file to files error", K(ret));
            } else { }//do nothing
          } else { } //do nothing
        }
      }//end of while
      if (0 != closedir(dir_pointer)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "Close dir error", K(ret));
      }
    }
  }
  if (NULL != dirc) {
    free(dirc);
  }
  if (NULL != basec) {
    free(basec);
  }
  return ret;
}

int ObLogger::add_files_to_list(void *files,
                                void *wf_files,
                                std::deque<std::string> &file_list,
                                std::deque<std::string> &wf_file_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(files) || OB_ISNULL(wf_files)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "Input should not be NULL", K(files), K(wf_files), K(ret));
  } else {
    ObIArray<FileName> *files_arr = static_cast<ObIArray<FileName> *>(files);
    ObIArray<FileName> *wf_files_arr = static_cast<ObIArray<FileName> *>(wf_files);
    //sort files
    if (files_arr->count() > 0) {
      qsort(&files_arr->at(0), files_arr->count(), sizeof(FileName), str_cmp);
    }
    if (wf_files_arr->count() > 0) {
      qsort(&wf_files_arr->at(0), wf_files_arr->count(), sizeof(FileName), str_cmp);
    }

    //Add to file_list
    if (OB_LIKELY(0 == pthread_mutex_lock(&file_index_mutex_))) {
      file_list.clear();
      std::string oldFile;
      for (int64_t i = 0; OB_SUCC(ret) && i < files_arr->count(); ++i) {
        if (file_list.size() >= max_file_index_) {
          oldFile = file_list.front();
          file_list.pop_front();
          unlink(oldFile.c_str());
        }
        file_list.push_back(files_arr->at(i).file_name_);
      }
      wf_file_list.clear();
      std::string old_wf_file;
      for (int64_t i = 0; OB_SUCC(ret) && i < wf_files_arr->count(); ++i) {
        if (wf_file_list.size() >= max_file_index_) {
          old_wf_file = wf_file_list.front();
          wf_file_list.pop_front();
          unlink(old_wf_file.c_str());
        }
        wf_file_list.push_back(wf_files_arr->at(i).file_name_);
      }
      (void)pthread_mutex_unlock(&file_index_mutex_);
    }
  }
  return ret;
}

int ObLogger::init(const ObBaseLogWriterCfg &log_cfg,
                   const bool is_arb_replica)
{
  int ret = OB_SUCCESS;

  static const char *thread_name = "OB_PLOG";
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_STDERR("ObLogger has inited twice");
  } else if (OB_UNLIKELY(!log_cfg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_STDERR("log_cfg is not valid");
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObBlockAllocMgr) + sizeof(ObVSliceAlloc) + sizeof(ObFIFOAllocator), "LoggerAlloc"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_STDERR("alloc failed");
  } else {
    for (int i = 0; i < ARRAYSIZEOF(per_log_limiters_); i++) {
      new (&per_log_limiters_[i])ObSyslogSampleRateLimiter(limiter_initial, limiter_thereafter);
    }
    const int64_t limit = ObBaseLogWriterCfg::DEFAULT_MAX_BUFFER_ITEM_CNT * OB_MALLOC_BIG_BLOCK_SIZE / 8; // 256M
    log_mem_limiter_ = new (buf) ObBlockAllocMgr(limit);
    allocator_ = new (log_mem_limiter_ + 1) ObVSliceAlloc();
    error_allocator_ = new (allocator_ + 1) ObFIFOAllocator();
    if (OB_FAIL(allocator_->init(OB_MALLOC_BIG_BLOCK_SIZE,
                                 *log_mem_limiter_,
                                 SET_USE_500(lib::ObMemAttr(OB_SERVER_TENANT_ID, "Logger",
                                                common::ObCtxIds::LOGGER_CTX_ID))))) {
      LOG_STDERR("init fifo error. ret=%d\n", ret);
    } else if (OB_FAIL(error_allocator_->init(lib::ObMallocAllocator::get_instance(),
                                              OB_MALLOC_BIG_BLOCK_SIZE,
                                              SET_USE_500(lib::ObMemAttr(OB_SERVER_TENANT_ID, "ErrorLogger",
                                                             common::ObCtxIds::LOGGER_CTX_ID)),
                                              ERROR_LOG_INIT_MEM,
                                              ERROR_LOG_INIT_MEM << 1,
                                              limit))) {
      LOG_STDERR("init error_fifo error. ret=%d\n", ret);
    }
    if (OB_SUCC(ret)) {
      allocator_->set_limit(limit);
      allocator_->set_nway(4);
      if (OB_FAIL(ObBaseLogWriter::init(log_cfg, thread_name))) {
        LOG_STDERR("init ObBaseLogWriter error. ret=%d\n", ret);
      } else if (OB_FAIL(ObBaseLogWriter::start())) {
        LOG_STDERR("start ObBaseLogWriter error ret=%d\n", ret);
      }
      is_arb_replica_ = is_arb_replica;
    }
  }

  if (OB_FAIL(ret)) {
    if (error_allocator_) {
      error_allocator_->~ObFIFOAllocator();
      error_allocator_ = nullptr;
    }
    if (allocator_) {
      allocator_->~ObVSliceAlloc();
      allocator_ = nullptr;
    }
    if (log_mem_limiter_) {
      log_mem_limiter_->~ObBlockAllocMgr();
      log_mem_limiter_ = nullptr;
    }
    if (buf) {
      ob_free(buf);
    }
    destroy();
  }
  return ret;
}

void ObLogger::process_log_items(ObIBaseLogItem **items, const int64_t item_cnt, int64_t &finish_cnt)
{
  finish_cnt = 0;
  if (OB_NOT_NULL(items) && OB_LIKELY(item_cnt > 0)) {
    static int64_t last_async_flush_ts = 0;
    static int64_t async_flush_log_count = 0;
    ObPLogItem **log_item = reinterpret_cast<ObPLogItem **>(items);

    if (item_cnt >= GROUP_COMMIT_MAX_ITEM_COUNT) {
      finish_cnt = GROUP_COMMIT_MAX_ITEM_COUNT - 1;
    } else {
      finish_cnt = item_cnt;
    }
    flush_logs_to_file(log_item, finish_cnt);
    async_flush_log_count += finish_cnt;
    if (log_item[finish_cnt - 1]->get_timestamp() > (last_async_flush_ts + FLUSH_SAMPLE_TIME)) {
      int64_t curr_ts = ObTimeUtility::current_time();
      if (curr_ts != last_async_flush_ts) {
        last_async_flush_count_per_sec_ = static_cast<int64_t>((double)(async_flush_log_count * 1000000) / (double)(curr_ts - last_async_flush_ts));
        last_async_flush_ts = curr_ts;
        async_flush_log_count = 0;
      }
    }
    // free_log_item adds the log allocator lock, if you log in the allocator again, it will deadlock
    bool old_val = set_disable_logging(true);
    DEFER(set_disable_logging(old_val));
    for (int64_t i = 0; i < finish_cnt; ++i) {
      free_log_item(log_item[i]);
      items[i] = NULL;
    }
  }
}

void ObLogger::flush_logs_to_file(ObPLogItem **log_item, const int64_t count)
{
  if (OB_NOT_NULL(log_item)
      && OB_LIKELY(count > 0)
      && OB_LIKELY(count < GROUP_COMMIT_MAX_ITEM_COUNT)
      && OB_NOT_NULL(log_item[0])) {
    if (log_item[0]->get_timestamp() > (last_check_disk_ts + DISK_SAMPLE_TIME)) {
      last_check_disk_ts = log_item[0]->get_timestamp();
      struct statfs disk_info;
      if (0 == statfs(log_file_[FD_SVR_FILE].filename_, &disk_info)) {
        can_print_ = ((disk_info.f_bfree * disk_info.f_bsize) > CAN_PRINT_DISK_SIZE);
      }
    }

    if (can_print_) {
      const int64_t lcf_ts = last_check_file_ts;
      if (force_check_ || log_item[0]->get_timestamp() > (lcf_ts + FILE_SAMPLE_TIME)) {
        if (ATOMIC_BCAS(&last_check_file_ts, lcf_ts, log_item[0]->get_timestamp())) {
          check_file();
        }
      }

      struct iovec vec[MAX_FD_FILE][GROUP_COMMIT_MAX_ITEM_COUNT];
      int iovcnt[MAX_FD_FILE] = {0};
      struct iovec wf_vec[MAX_FD_FILE][GROUP_COMMIT_MAX_ITEM_COUNT];
      int wf_iovcnt[MAX_FD_FILE] = {0};

      ObPLogFDType fd_type = MAX_FD_FILE;
      for (int64_t i = 0; i < count; ++i) {
        if (OB_ISNULL(log_item[i])) {
          LOG_STDERR("log_item is null, it should not happened, i=%ld, count=%ld, item[0]=%s\n",
                     i, count, log_item[0]->get_buf());
        } else if (OB_UNLIKELY(!log_item[i]->is_supported_file())) {
          LOG_STDERR("unknown log, it should not happened, item=%s\n", log_item[i]->get_buf());
        } else {
          fd_type = log_item[i]->get_fd_type();
          vec[fd_type][iovcnt[fd_type]].iov_base = log_item[i]->get_buf();
          vec[fd_type][iovcnt[fd_type]].iov_len = static_cast<size_t>(log_item[i]->get_data_len());
          iovcnt[fd_type] += 1;
          if ((enable_wf_flag_ && open_wf_flag_ && log_item[i]->get_log_level() <= wf_level_)) {
            wf_vec[fd_type][wf_iovcnt[fd_type]].iov_base = log_item[i]->get_buf();
            wf_vec[fd_type][wf_iovcnt[fd_type]].iov_len = static_cast<size_t>(log_item[i]->get_data_len());
            wf_iovcnt[fd_type] += 1;
          }
          if (log_item[i]->get_tl_type() >= 0 && log_item[i]->get_tl_type() < MAX_TASK_LOG_TYPE) {
            (void)ATOMIC_AAF(written_count_ + log_item[i]->get_tl_type(), 1);
            (void)ATOMIC_AAF(current_written_count_ + log_item[i]->get_tl_type(), -1);
          }
          if (log_item[i]->is_force_allow()) {
            (void)ATOMIC_AAF(written_count_ + MAX_TASK_LOG_TYPE, 1);
          }
        }
      }

      ssize_t size = 0;
      ssize_t writen[MAX_FD_FILE] = {0};
      for (int32_t i = 0; i < static_cast<int32_t>(MAX_FD_FILE); i++) {
        size = 0;
        if (iovcnt[i] > 0 && log_file_[i].fd_ > 0) {
          size = ::writev(log_file_[i].fd_, vec[i], iovcnt[i]);
        }
        if (size > 0) {
          writen[i] = size;
          (void)ATOMIC_AAF(&log_file_[i].write_size_, size);
          (void)ATOMIC_AAF(&log_file_[i].file_size_, size);
          (void)ATOMIC_AAF(&log_file_[i].write_count_, iovcnt[i]);
        }
        if (wf_iovcnt[i] > 0 && log_file_[i].wf_fd_ > 0) {
          (void)::writev(log_file_[i].wf_fd_, wf_vec[i], wf_iovcnt[i]);
        }
      }

      if (max_file_size_ > 0) {
        for (int32_t i = 0; i < static_cast<int32_t>(MAX_FD_FILE); i++) {
          const bool redirect_flag = (static_cast<int32_t>(FD_SVR_FILE) == i ? redirect_flag_ : false);
          rotate_log(writen[i], redirect_flag, log_file_[i], static_cast<ObPLogFDType>(i));
        }
      }
    }//can print
  }
}

bool ObLogger::is_force_allows() const
{
  bool bret = false;
  auto log_limiter = (nullptr != tl_log_limiter_) ? tl_log_limiter_ : default_log_limiter_;
  if (enable_log_limit_ && nullptr != log_limiter
      && log_limiter->is_force_allows()) {
    bret = true;
  }
  return bret;
}

void ObLogger::check_reset_force_allows()
{
  auto log_limiter = (nullptr != tl_log_limiter_ ? tl_log_limiter_ : default_log_limiter_);
  if (enable_log_limit_ && nullptr != log_limiter
      && log_limiter->is_force_allows()) {
    log_limiter->reset_force_allows();
  }
}

int64_t ObLogger::get_reimbursation_time()
{
  int64_t ret_time = 0;
  int64_t task_level = 0;
  if (tl_type_ >= 0 && tl_type_ < 5) {
    for (int64_t i = 0; i < 5; ++i) {
      if (ATOMIC_LOAD(current_written_count_ + tl_type_) < ATOMIC_LOAD(current_written_count_ + i)) {
        ++task_level;
      }
    }
  }
  ret_time = POP_COMPENSATED_TIME[task_level];//0, 1, 2, 3, 4us
  return ret_time;
}

int ObLogger::backtrace_if_needed(ObPLogItem &log_item, const bool force)
{
  const char* BACKTRACE_END = !force ? " BACKTRACE:" : " PROBE_BACKTRACE:";
  int ret = OB_SUCCESS;
  if (force || OB_UNLIKELY(OB_LOG_LEVEL_ERROR == log_item.get_log_level())) {
    int64_t pos = (log_item.get_data_len() > 0 ? log_item.get_data_len() - 1 : 0);
    char *buf = log_item.get_buf();
    const int64_t buf_size = log_item.get_buf_size();
    if (OB_FAIL(logdata_print_info(buf, buf_size, pos, BACKTRACE_END))) {
      //do nothing
    } else {
      char *bt = lbt();
      int size = strlen(bt);
      if (buf_size < pos + size + 1) {
        pos = buf_size - size - 1;
        if (pos < 0) {
          pos = 0;
        }
      }
      if (OB_FAIL(logdata_print_info(buf, buf_size, pos, bt))) {
        //do nothing
      } else {
        //do nothing
      }
    }
    check_log_end(log_item, pos);
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
      //treat it succ
      log_item.set_size_overflow();
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogger::check_tl_log_limiter(const uint64_t location_hash_val,
                                   const int32_t level,
                                   const int errcode,
                                   const int64_t log_size,
                                   bool &allow,
                                   const char *&limiter_info)
{
  int ret = OB_SUCCESS;
  allow = true;
  if (OB_LIKELY(is_inited())) {
    auto log_limiter = (nullptr != tl_log_limiter_ ? tl_log_limiter_ : default_log_limiter_);
    if (enable_log_limit_) {
      if (nullptr != log_limiter) {
        allow = OB_SUCCESS == log_limiter->try_acquire(log_size, level, errcode);
        if (!allow) { limiter_info = " REACH SYSLOG RATE LIMIT [bandwidth]"; }
      }
      if (allow) {
        int64_t idx0 = (location_hash_val >> 32) % N_LIMITER;
        int64_t idx1 = ((location_hash_val << 32) >> 32) % N_LIMITER;
        bool r0 = OB_SUCCESS == per_log_limiters_[idx0].try_acquire(1, level, errcode);
        bool r1 = OB_SUCCESS == per_log_limiters_[idx1].try_acquire(1, level, errcode);
        allow = r0 && r1;
        if (!allow) { limiter_info = " REACH SYSLOG RATE LIMIT [frequency]"; }
      }
      if (!allow && nullptr != log_limiter && log_limiter->is_force_allows()) {
        allow = true;
      }
    }
  }
  return ret;
}

bool ObLogger::need_print_log_limit_msg()
{
  return TC_REACH_TIME_INTERVAL(1L * 1000 * 1000);
}


int64_t ObLogger::get_wait_us(const int32_t level)
{
  int64_t ret_timeout_us = 0;
  if (is_force_allows()) {
    //if force allows, wait 100us
    ret_timeout_us = 100;//100us
  } else {
    switch (level) {
    case OB_LOG_LEVEL_DBA_ERROR: // pass
    case OB_LOG_LEVEL_DBA_WARN: // pass
    case OB_LOG_LEVEL_ERROR: {
      ret_timeout_us = 100;//100us
      break;
    }
    case OB_LOG_LEVEL_WARN: {
      ret_timeout_us = 10;//10us
      ret_timeout_us += 2 * get_reimbursation_time();//double it
      break;
    }
    case OB_LOG_LEVEL_INFO:
    case OB_LOG_LEVEL_TRACE: {
      ret_timeout_us = 2;//2us
      ret_timeout_us += get_reimbursation_time();
      break;
    }
    default: {
      ret_timeout_us = 0;//0us
      break;
      //do nothiong
    }
    }
  }
  return ret_timeout_us;
}

#ifdef OB_BUILD_AUDIT_SECURITY
int ObLogger::async_audit_dump(const common::ObBasebLogPrint &info)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_async_log_used())
      && OB_LIKELY(info.get_data_length() > 0)) {
    const int32_t level = OB_LOG_LEVEL_INFO;
    ObPLogItem *log_item = NULL;
    set_disable_logging(true);
    //1. fill log buffer
    if (OB_FAIL(alloc_log_item(level, MAX_LOG_SIZE, log_item))) {
      LOG_STDERR("alloc_log_item error, ret=%d\n", ret);
      ret = OB_SUCCESS; // ignore alloc memory fails
    } else if (OB_ISNULL(log_item)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      int64_t pos = log_item->get_data_len();
      log_item->set_timestamp(info.get_timestamp());
      log_item->set_fd_type(FD_AUDIT_FILE);
      if (OB_FAIL(info.print_data(log_item->get_buf(), log_item->get_buf_size(), pos))) {
        LOG_STDERR("print_data error ret = %d\n", ret);
      } else if (FALSE_IT(check_log_end(*log_item, pos))) {
      } else if (OB_FAIL(append_log(*log_item))) {
        LOG_STDERR("append_log error ret = %d\n", ret);
      }
    }

    //3. stat
    if (OB_FAIL(ret)) {
      inc_dropped_log_count(level);
      free_log_item(log_item);
      log_item = NULL;
    }
    set_disable_logging(false);
  }
  return ret;
}
#endif

int ObLogger::alloc_log_item(const int32_t level, const int64_t size, ObPLogItem *&log_item)
{
  UNUSED(level);
  int ret = OB_SUCCESS;
  log_item = NULL;
  if (!stop_append_log_) {
    char *buf = nullptr;
    auto *p_alloc = (level == OB_LOG_LEVEL_ERROR
                     || level == OB_LOG_LEVEL_DBA_WARN
                     || level == OB_LOG_LEVEL_DBA_ERROR)
        ? (ObIAllocator *)error_allocator_
        : (ObIAllocator *)allocator_;
    if (OB_UNLIKELY(nullptr == p_alloc)) {
      ret = OB_NOT_INIT;
      LOG_STDERR("uninit error, ret=%d, level=%d\n", ret, level);
    } else if (OB_UNLIKELY(nullptr == (buf = (char*)p_alloc->alloc(size)))) {
      int64_t wait_us = get_wait_us(level);
      const int64_t per_us = MIN(wait_us, 10);
      while (wait_us > 0) {
        if (nullptr != (buf = (char*)p_alloc->alloc(size))) {
          break;
        } else {
          usleep(per_us);
          wait_us -= per_us;
        }
      }
      if (nullptr == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_STDERR("alloc_log_item error, ret=%d level=%d\n", ret, level);
      }
    }
    if (OB_SUCC(ret)) {
      log_item = new (buf) ObPLogItem();
      log_item->set_buf_size(size - LOG_ITEM_SIZE);
      log_item->set_log_level(level);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_STDERR("can not alloc_log_item, ret=%d, stop_append_log=%d\n", ret, stop_append_log_);
  }
  return ret;
}

void ObLogger::free_log_item(ObPLogItem *log_item)
{
  if (NULL != log_item) {
    const int level = log_item->get_log_level();
    auto *p_alloc = (level == OB_LOG_LEVEL_ERROR
                     || level == OB_LOG_LEVEL_DBA_WARN
                     || level == OB_LOG_LEVEL_DBA_ERROR)
        ? (ObIAllocator *)error_allocator_
        : (ObIAllocator *)allocator_;
    abort_unless(p_alloc);
    log_item->~ObPLogItem();
    p_alloc->free(log_item);
  }
}

void ObLogger::inc_dropped_log_count(const int32_t level)
{
  if (OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG)
      && OB_LIKELY(level >= OB_LOG_LEVEL_DBA_ERROR)) {
    //recode dropped count
    ATOMIC_AAF(dropped_log_count_ + level, 1);
  }
  if (tl_type_ >= 0 && tl_type_ < MAX_TASK_LOG_TYPE) {
    ATOMIC_AAF(dropped_count_ + tl_type_, 1);
  }
  if (is_force_allows()) {
    ATOMIC_AAF(dropped_count_ + MAX_TASK_LOG_TYPE, 1);
  }
}

int ObLogger::log_new_file_info(const ObPLogFileStruct &log_file)
{
  int ret = OB_SUCCESS;
  if (nullptr != new_file_info_
      && (log_file.fd_ > 0 || log_file.wf_fd_ > 0)) {
    static const int64_t max_buf_len = 512;
    char buf[max_buf_len];
    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    struct tm tm;
    ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(tv.tv_sec), &tm);
    snprintf(buf, max_buf_len, "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] INFO  New syslog file info: [%s]\n",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
             tm.tm_sec, tv.tv_usec, new_file_info_);
    buf[max_buf_len - 2] = '\n';
    buf[max_buf_len - 1] = '\0';
    if (OB_SUCC(ret) && log_file.fd_ > STDOUT_FILENO) {
      if (-1 == ::write(log_file.fd_, buf, strlen(buf)) < 0) {
        ret = OB_ERR_SYS;
      }
    }
    if (OB_SUCC(ret) && log_file.wf_fd_ > STDERR_FILENO) {
      if (-1 == ::write(log_file.wf_fd_, buf, strlen(buf)) < 0) {
        ret = OB_ERR_SYS;
      }
    }
  }
  return ret;
}

void ObLogger::issue_dba_error(const int errcode, const char *file, const int line, const char *info_str)
{
  const char *base_file_name = strrchr(file, '/');
  base_file_name = (NULL != base_file_name) ? base_file_name + 1 : file;
  LOG_DBA_ERROR(OB_UNEXPECT_INTERNAL_ERROR,
                "errcode", errcode, "file", base_file_name, "line_no", line, "info", info_str);
}

}
}

#include "ob_log_module.ipp"
