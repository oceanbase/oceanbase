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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_ASYNC_LOG_
#define OCEANBASE_ELECTION_OB_ELECTION_ASYNC_LOG_

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <algorithm>
#include "lib/lock/Mutex.h"
#include "lib/lock/Monitor.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/atomic/atomic128.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace election {

template <typename Type, int size>
class ObRingBuffer {
public:
  ObRingBuffer() : destroyed_(false), size_(size), start_(0), end_(0)
  {
    memset(&elems_, 0, sizeof(elems_));
  }
  virtual ~ObRingBuffer()
  {
    destroy();
  }

public:
  bool is_full(void) const
  {
    return (end_ + 1) % size_ == start_;
  }
  bool is_empty(void) const
  {
    return end_ == start_;
  }
  int push(const Type& elem, Type& old_elem, bool& overwrite);
  int push(const Type& elem, Type& old_elem);
  int pop(Type& elem);
  void destroy();

private:
  bool destroyed_;
  int size_;
  int start_;
  int end_;
  Type elems_[size];
  tbutil::Monitor<tbutil::Mutex> monitor_;
};

template <typename Type, int size>
int ObRingBuffer<Type, size>::push(const Type& elem, Type& old_elem)
{
  bool overwrite = true;
  return push(elem, old_elem, overwrite);
}

template <typename Type, int size>
int ObRingBuffer<Type, size>::push(const Type& elem, Type& old_elem, bool& overwrite)
{
  int ret = common::OB_SUCCESS;

  tbutil::Monitor<tbutil::Mutex>::Lock guard(monitor_);
  if (destroyed_) {
    ret = common::OB_NOT_INIT;
  } else if (is_full() && !overwrite) {
    ret = common::OB_ELECTION_WARN_LOGBUF_FULL;
  } else {
    const bool empty = is_empty();
    elems_[end_] = elem;
    end_ = (end_ + 1) % size_;
    // full, overwrite
    if (end_ == start_) {
      old_elem = elems_[start_];
      start_ = (start_ + 1) % size_;
      overwrite = true;
    } else {
      overwrite = false;
    }
    if (empty) {
      monitor_.notify();
    }
    ret = common::OB_SUCCESS;
  }

  return ret;
}

template <typename Type, int size>
int ObRingBuffer<Type, size>::pop(Type& elem)
{
  int ret = common::OB_SUCCESS;

  tbutil::Monitor<tbutil::Mutex>::Lock guard(monitor_);
  while (!destroyed_ && is_empty()) {
    monitor_.wait();
  }
  if (destroyed_) {
    ret = common::OB_NOT_INIT;
  } else {
    elem = elems_[start_];
    start_ = (start_ + 1) % size_;
  }

  return ret;
}

template <typename Type, int size>
void ObRingBuffer<Type, size>::destroy()
{
  tbutil::Monitor<tbutil::Mutex>::Lock guard(monitor_);
  if (!destroyed_) {
    monitor_.notifyAll();
    destroyed_ = true;
  }
}

class ObLogItem {
public:
  static const int64_t MAX_LOG_SIZE = 4096;

  ObLogItem(const char* buf = NULL, int64_t size = 0, int log_level = OB_LOG_LEVEL_NONE);
  virtual ~ObLogItem()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }

public:
  char* get_buf()
  {
    return buf_;
  }
  const char* get_buf(void) const
  {
    return buf_;
  }
  int64_t get_size(void) const
  {
    return size_;
  }
  void set_size(const int64_t size)
  {
    size_ = size;
  }
  int get_log_level(void) const
  {
    return log_level_;
  }
  void set_log_level(const int log_level)
  {
    log_level_ = log_level;
  }
  ObLogItem& operator=(const ObLogItem& item);

public:
  static const int64_t OP_LOCAL_NUM = 4;

private:
  int64_t size_;
  int log_level_;
  char buf_[MAX_LOG_SIZE];
};

class ObLogItemFactory {
public:
  static ObLogItem* alloc();
  static void release(ObLogItem* item);
  static int64_t alloc_count_;
  static int64_t release_count_;
};

class ObLogFile {
public:
  ObLogFile();
  virtual ~ObLogFile()
  {
    close();
  }

public:
  int open(const char* log_file);
  int close();
  int write(const char* buf, const int64_t len);
  int set_max_file_size(const int64_t size);
  bool is_opened()
  {
    return (fd_ >= 0);
  }
  int64_t get_write_size()
  {
    return write_size_;
  }
  static int64_t& get_last_log_time()
  {
    static RLOCAL(int64_t, last_log_time);
    return last_log_time;
  }

private:
  char fname_[common::OB_MAX_FILE_NAME_LENGTH];
  int fd_;
  struct stat stat_;
  int64_t write_count_;
  int64_t write_size_;
  int64_t file_size_;
  int64_t max_file_size_;
};

#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1) : __FILE__)

#define ASYNC_LOG_LOGGER ::oceanbase::election::ObAsyncLog::getLogger()
#define ASYNC_LOG_LEVEL(level) OB_LOG_LEVEL_##level, #level, __FILENAME__, __LINE__, __FUNCTION__

#define ASYNC_LOG(mod_name, level, fmt, args...)                                                   \
  ((OB_LOGGER.need_to_print(::oceanbase::common::OB_LOG_ROOT::M_##mod_name, OB_LOG_LEVEL_##level)) \
          ? oceanbase::election::ObAsyncLog::getLogger().async_log_message_kv(                     \
                "[" #mod_name "] ", ASYNC_LOG_LEVEL(level), fmt, ##args)                           \
          : (void)0)

#define ASYNC_LOG_INIT(fname, level, open_wf) ObAsyncLog::getLogger().init(fname, level, open_wf)
#define ASYNC_LOG_DESTROY() ObAsyncLog::getLogger().destroy()
#define ELECT_ASYNC_LOG(level, fmt_, args...) OB_MOD_LOG(ELECT, level, fmt_, ##args)
#define FORCE_ELECT_LOG(level, fmt_, args...)                      \
  do {                                                             \
    oceanbase::share::ObTaskController::get().allow_next_syslog(); \
    OB_MOD_LOG(ELECT, level, fmt_, ##args);                        \
  } while (0)

#define ELECT_ASYNC_LIMIT_LOG(...)                                                    \
  do {                                                                                \
    if (EXECUTE_COUNT_PER_SEC(oceanbase::election::ObAsyncLog::OB_MAX_LOG_PER_SEC)) { \
      ELECT_ASYNC_LOG(__VA_ARGS__);                                                   \
    }                                                                                 \
  } while (0)

#define ELECT_ASYNC_LOG_(...)       \
  do {                              \
    if (!ignore_log_)               \
      ELECT_ASYNC_LOG(__VA_ARGS__); \
  } while (0)

#define ASYNC_TRACE_LOG(mod_name, level, force_print, fmt, args...)                                      \
  ((oceanbase::election::ObAsyncLog::getLogger().trace_need_to_print(OB_LOG_LEVEL_##level, force_print)) \
          ? oceanbase::election::ObAsyncLog::getLogger().async_log_message_kv(                           \
                "[" #mod_name "] ", ASYNC_LOG_LEVEL(level), fmt, ##args)                                 \
          : (void)0)

#define ELECT_ASYNC_TRACE_LOG(level, force_print, fmt_, args...) OB_MOD_LOG(ELECT, level, fmt_, ##args)

#define PRINT_ASYNC_TRACE_BUF(trace_recorder)                                   \
  do {                                                                          \
    ELECT_ASYNC_TRACE_LOG(INFO, true, "election", "trace log", trace_recorder); \
    trace_recorder.reset();                                                     \
  } while (0)

#define PRINT_ASYNC_TRACE_BUF_(trace_recorder)                                    \
  do {                                                                            \
    if (!ignore_log_)                                                             \
      ELECT_ASYNC_TRACE_LOG(INFO, true, "election", "trace log", trace_recorder); \
    trace_recorder.reset();                                                       \
  } while (0)

#define DEFINE_ASYNC_LOG_PRINT_KV(n)                                                                          \
  template <LOG_TYPENAME_TN##n>                                                                               \
  void async_log_message_kv(const char* mod_name,                                                             \
      const int level,                                                                                        \
      const char* levelstr,                                                                                   \
      const char* file,                                                                                       \
      const int32_t line,                                                                                     \
      const char* func,                                                                                       \
      const char* fmt,                                                                                        \
      LOG_PARAMETER_KV##n)                                                                                    \
  {                                                                                                           \
    int ret = common::OB_SUCCESS;                                                                             \
    ObLogItem* item = NULL;                                                                                   \
    const int64_t MAX_LOG_SIZE = ObLogItem::MAX_LOG_SIZE;                                                     \
                                                                                                              \
    if (!is_inited_ || destroyed_) {                                                                          \
      ret = common::OB_NOT_INIT;                                                                              \
    } else if (level <= OB_LOGGER.get_log_level()) {                                                          \
      bool overwrite = false;                                                                                 \
      const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();                          \
                                                                                                              \
      if (level <= OB_LOG_LEVEL_WARN) {                                                                       \
        overwrite = true;                                                                                     \
      }                                                                                                       \
      item = ObLogItemFactory::alloc();                                                                       \
      if (NULL == item) {                                                                                     \
        LOG_STDERR("op alloc ObLogItem error\n");                                                             \
        ret = common::OB_ALLOCATE_MEMORY_FAILED;                                                              \
      } else {                                                                                                \
        char* data = item->get_buf();                                                                         \
                                                                                                              \
        if (NULL == data) {                                                                                   \
          LOG_STDERR("item buf is NULL data = %p\n", data);                                                   \
          ret = common::OB_ERR_UNEXPECTED;                                                                    \
        } else {                                                                                              \
          int64_t pos = 0;                                                                                    \
          if (OB_FAIL(logdata_print_info_(                                                                    \
                  data, pos, mod_name, level, levelstr, file, line, func, ObLogFile::get_last_log_time()))) { \
            LOG_STDERR("logdata print info error ret = %d\n", ret);                                           \
          } else {                                                                                            \
            if (pos < ObLogItem::MAX_LOG_SIZE) {                                                              \
              int64_t num = snprintf(data + pos, ObLogItem::MAX_LOG_SIZE - pos, "%s", fmt);                   \
              pos += num;                                                                                     \
            }                                                                                                 \
            LOG_KV_BEGIN();                                                                                   \
            LOG_FUNC_BODY_##n;                                                                                \
            LOG_KV_END();                                                                                     \
            if (pos > ObLogItem::MAX_LOG_SIZE - 2) {                                                          \
              pos = ObLogItem::MAX_LOG_SIZE - 2;                                                              \
            }                                                                                                 \
            data[pos++] = '\n';                                                                               \
            data[pos] = '\0';                                                                                 \
            item->set_size(pos);                                                                              \
            item->set_log_level(level);                                                                       \
            ObLogItem* old_item = NULL;                                                                       \
            if (OB_FAIL(log_buffer_->push(item, old_item, overwrite))) {                                      \
              LOG_STDERR("push log item to buffer error ret = %d\n", ret);                                    \
            } else if (overwrite && NULL != old_item) {                                                       \
              ObLogItemFactory::release(old_item);                                                            \
              old_item = NULL;                                                                                \
            } else {                                                                                          \
            }                                                                                                 \
            ObLogFile::get_last_log_time() = ::oceanbase::common::ObTimeUtility::current_time() - start_time; \
          }                                                                                                   \
        }                                                                                                     \
      }                                                                                                       \
    }                                                                                                         \
    if (OB_FAIL(ret)) {                                                                                       \
      if (NULL != item) {                                                                                     \
        ObLogItemFactory::release(item);                                                                      \
        item = NULL;                                                                                          \
      }                                                                                                       \
    }                                                                                                         \
  }

class ObAsyncLog : public share::ObThreadPool {
public:
  ObAsyncLog();
  virtual ~ObAsyncLog()
  {
    destroy();
  }

public:
  static ObAsyncLog& getLogger(void);
  int init(const char* fname, const int level, const bool open_wf);
  void destroy(void);
  int64_t get_write_size();
  int set_max_file_size(const int64_t size);
  int set_log_level(const int level);
  bool need_to_print(const int log_level) const
  {
    return (log_level <= log_level_);
  }
  bool trace_need_to_print(const int log_level, const bool force_print) const
  {
    return (log_level <= log_level_ || force_print);
  }
  int get_log_level() const
  {
    return log_level_;
  }
  void down_log_level()
  {
    if (log_level_ < OB_LOG_LEVEL_NONE) {
      ++log_level_;
    }
  }
  void up_log_level()
  {
    if (log_level_ > OB_LOG_LEVEL_NP) {
      --log_level_;
    }
  }
  // whether log warn/error log to log.wf
  void set_log_warn(bool log_warn)
  {
    log_warn_ = log_warn;
  }

public:
  void async_log_message_kv(const char* mod_name, const int level, const char* levelstr, const char* file,
      const int32_t line, const char* func, const char* fmt, ...);
  DEFINE_ASYNC_LOG_PRINT_KV(1);
  DEFINE_ASYNC_LOG_PRINT_KV(2);
  DEFINE_ASYNC_LOG_PRINT_KV(3);
  DEFINE_ASYNC_LOG_PRINT_KV(4);
  DEFINE_ASYNC_LOG_PRINT_KV(5);
  DEFINE_ASYNC_LOG_PRINT_KV(6);
  DEFINE_ASYNC_LOG_PRINT_KV(7);
  DEFINE_ASYNC_LOG_PRINT_KV(8);
  DEFINE_ASYNC_LOG_PRINT_KV(9);
  DEFINE_ASYNC_LOG_PRINT_KV(10);
  DEFINE_ASYNC_LOG_PRINT_KV(11);
  DEFINE_ASYNC_LOG_PRINT_KV(12);
  DEFINE_ASYNC_LOG_PRINT_KV(13);
  DEFINE_ASYNC_LOG_PRINT_KV(14);
  DEFINE_ASYNC_LOG_PRINT_KV(15);
  DEFINE_ASYNC_LOG_PRINT_KV(16);

private:
  void run1() final;
  void do_flush_log_(void);
  int logdata_print_info_(char* buf, int64_t& pos, const char* mod_name, const int level, const char* levelstr,
      const char* file, const int32_t line, const char* func, const int64_t last_log_time);

public:
  static const int64_t OB_MAX_LOG_ITEM_COUNT = 81920;
  static const int64_t OB_MAX_LOG_PER_SEC = 1000;

private:
  typedef ObRingBuffer<ObLogItem*, OB_MAX_LOG_ITEM_COUNT> ObLogBuffer;
  bool is_inited_;
  bool destroyed_;
  pthread_t tid_;
  int log_level_;
  int wflog_level_;
  ObLogFile log_;
  ObLogFile wflog_;
  bool log_warn_;  // whether write waring log to wf log-file.
  ObLogBuffer* log_buffer_;
};

}  // namespace election
}  // namespace oceanbase
#endif  // OCEANBASE_ELECTION_OB_ELECTION_ASYNC_LOG_
