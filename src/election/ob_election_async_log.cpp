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

#include "ob_election_async_log.h"
#include <sys/prctl.h>
#include <linux/prctl.h>
#include "lib/profile/ob_trace_id.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/stat/ob_diagnose_info.h"

using namespace tbutil;

namespace oceanbase {
using namespace common;

namespace election {
static const int64_t DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;  // default max log file size
static const int64_t LOG_FILE_MODE = 0644;
static const int64_t ASYNC_LOG_WRITE_LOG_COUNT = 100;
static const int64_t ELECTION_LOG_ITEM_STAT_INTERVAL = 60 * 1000 * 1000;
int64_t ObLogItemFactory::alloc_count_ = 0;
int64_t ObLogItemFactory::release_count_ = 0;

ObLogItem::ObLogItem(const char* buf, int64_t size, int log_level) : log_level_(log_level)
{
  int64_t tmp_size = 0;
  if (size > 0) {
    tmp_size = size;
  }
  if (tmp_size > MAX_LOG_SIZE) {
    tmp_size = MAX_LOG_SIZE;
  }
  if (NULL != buf && 0 < tmp_size) {
    MEMCPY(buf_, buf, tmp_size);
  }
  size_ = tmp_size;
}

void ObLogItem::reset()
{
  size_ = 0;
  log_level_ = OB_LOG_LEVEL_NONE;
  buf_[0] = '\0';
}

ObLogItem& ObLogItem::operator=(const ObLogItem& item)
{
  if (this != &item) {
    MEMCPY(buf_, item.get_buf(), item.get_size());
    size_ = item.get_size();
    log_level_ = item.get_log_level();
  }
  return *this;
}

ObLogItem* ObLogItemFactory::alloc()
{
  ObLogItem* item = NULL;

  if (REACH_TIME_INTERVAL(ELECTION_LOG_ITEM_STAT_INTERVAL)) {
    ELECT_ASYNC_LOG(
        INFO, "ObLogItemFactory statistics", K_(alloc_count), K_(release_count), "used", alloc_count_ - release_count_);
  }
  if (NULL == (item = op_reclaim_alloc(ObLogItem))) {
    LOG_STDERR("ObLogItemFactory alloc error.");
  } else {
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }

  return item;
}

void ObLogItemFactory::release(ObLogItem* item)
{
  if (NULL == item) {
    LOG_STDERR("ObLogItem is NULL. item = %p\n", item);
  } else {
    op_reclaim_free(item);
    item = NULL;
    (void)ATOMIC_FAA(&release_count_, 1);
  }
}

ObLogFile::ObLogFile() : fd_(-1), write_count_(0), write_size_(0), file_size_(0), max_file_size_(DEFAULT_MAX_FILE_SIZE)
{
  fname_[0] = '\0';
  fname_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
  memset(&stat_, 0, sizeof(struct stat));
}

int ObLogFile::open(const char* log_file)
{
  int ret = OB_SUCCESS;
  int fd = -1;

  if (NULL == log_file) {
    LOG_STDERR("invalid argument log_file = %p\n", log_file);
    ret = OB_INVALID_ARGUMENT;
  } else if (is_opened()) {
    LOG_STDERR("log_file is opened already fname = %s log_file = %s\n", fname_, log_file);
    ret = OB_ERR_UNEXPECTED;
  } else {
    strncpy(fname_, log_file, OB_MAX_FILE_NAME_LENGTH - 1);
    if (fname_[0] == '\0') {
      LOG_STDERR("strncpy log filename = %s error\n", log_file);
      ret = OB_ERR_UNEXPECTED;
    } else if ((fd = ::open(fname_, O_WRONLY | O_CREAT | O_APPEND, LOG_FILE_MODE)) < 0) {
      LOG_STDERR("open file = %s errno = %d error = %m\n", fname_, errno);
      ret = OB_ERR_UNEXPECTED;
    } else if (fstat(fd, &stat_) != 0) {
      LOG_STDERR("fstat file = %s error\n", fname_);
      (void)::close(fd);
      fd = -1;
      ret = OB_ERR_UNEXPECTED;
    } else {
      fd_ = fd;
      file_size_ = stat_.st_size;
    }
  }

  return ret;
}

int ObLogFile::close()
{
  int ret = OB_SUCCESS;
  if (fd_ >= 0) {
    (void)::close(fd_);
    fd_ = -1;
  }
  return ret;
}

int ObLogFile::write(const char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  ssize_t n = 0;
  bool need_reopen = false;
  char newfile[OB_MAX_FILE_NAME_LENGTH];

  if (OB_ISNULL(buf) || len <= 0) {
    LOG_STDERR("invalid argument. buf = %p, len = %ld\n", buf, len);
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_opened()) {
    LOG_STDERR("log file = %s is not opened\n", fname_);
    ret = OB_ERR_UNEXPECTED;
  } else {
    // check file size.
    if (max_file_size_ > 0 && (max_file_size_ - file_size_) < len) {
      char timebuf[64] = {0};
      time_t t = 0;
      struct tm tm;

      t = time(NULL);
      ob_fast_localtime(
          ObLogger::get_logger().last_unix_sec_, ObLogger::get_logger().last_localtime_, static_cast<time_t>(t), &tm);

      size_t strftime_len = strftime(timebuf, sizeof(timebuf), "%Y%m%d%H%M%S", &tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_STDERR("strftime error");
      } else {
        size_t snprintf_len = snprintf(newfile, sizeof(newfile), "%s.%s", fname_, timebuf);
        if (OB_UNLIKELY(snprintf_len <= 0) || OB_UNLIKELY(snprintf_len >= sizeof(newfile))) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_STDERR("snprintf error");
        }
      }

      if (OB_SUCC(ret)) {
        if (rename(fname_, newfile) != 0) {
          LOG_STDERR("rename error. from:%s, to:%s\n", fname_, newfile);
          ret = OB_ERR_UNEXPECTED;
        } else {
          need_reopen = true;
        }
      }
    }
    // check file stat
    if (OB_SUCC(ret) && write_count_ > 0 && (write_count_ % ASYNC_LOG_WRITE_LOG_COUNT == 0)) {
      struct stat statbuf;
      if (stat(fname_, &statbuf) != 0) {
        if (ENOENT == errno) {
          need_reopen = true;
        } else {
          LOG_STDERR("stat error. log file = %s\n", fname_);
          ret = OB_ERR_UNEXPECTED;
        }
      } else if (statbuf.st_dev != stat_.st_dev || statbuf.st_ino != stat_.st_ino) {
        need_reopen = true;
      } else {
        // do nothing
      }
    }
    // need reopen
    if (OB_SUCC(ret) && need_reopen) {
      if (is_opened()) {
        (void)::close(fd_);
      }
      int fd = -1;
      if ((fd = ::open(fname_, O_WRONLY | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE)) < 0) {
        LOG_STDERR("reopen %s error\n", fname_);
        ret = OB_ERR_UNEXPECTED;
        fd = -1;
      } else {
        struct stat stat;
        if (fstat(fd, &stat) != 0) {
          LOG_STDERR("fstat %s error\n", fname_);
          ret = OB_ERR_UNEXPECTED;
          (void)::close(fd);
          fd = -1;
        } else {
          fd_ = fd;
          stat_ = stat;
          file_size_ = stat_.st_size;
        }
      }
    }
    // write
    if (OB_SUCC(ret)) {
      if (is_opened()) {
        if ((n = ::write(fd_, buf, len)) != len) {
          LOG_STDERR("write file error fd_ = %d file = %s buf = %s len = %ld n = %ld\n", fd_, fname_, buf, len, n);
          ret = OB_ERR_UNEXPECTED;
        } else {
          write_count_++;
          file_size_ += n;

          write_size_ += ((n > 0) ? n : 0);
          ObPLogFileStruct& log_struct = OB_LOGGER.get_elec_log();
          log_struct.write_size_ += ((n > 0) ? n : 0);
          ++log_struct.write_count_;
        }

      } else {
        LOG_STDERR("log file is not opened. %s\n", fname_);
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }

  return ret;
}

int ObLogFile::set_max_file_size(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (size <= 0) {
    LOG_STDERR("invalid argument. size = %ld\n", size);
    ret = OB_INVALID_ARGUMENT;
  } else {
    max_file_size_ = size;
  }

  return ret;
}

ObAsyncLog::ObAsyncLog()
    : is_inited_(false),
      destroyed_(false),
      tid_(0),
      log_level_(OB_LOG_LEVEL_INFO),
      wflog_level_(OB_LOG_LEVEL_WARN),
      log_warn_(true),
      log_buffer_(NULL)
{}

int ObAsyncLog::init(const char* fname, const int level, const bool open_wf)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;

  if (is_inited_) {
    LOG_STDERR("ObAsyncLog init twice\n");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(fname) || level > OB_LOG_LEVEL_DEBUG || level < OB_LOG_LEVEL_ERROR) {
    LOG_STDERR("invalid argument fname = %p level = %d\n", fname, level);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(log_.open(fname))) {
      LOG_STDERR("log open error. log = %s\n", fname);
    } else if (open_wf) {
      char wfname[OB_MAX_FILE_NAME_LENGTH] = {0};
      wfname[sizeof(wfname) - 1] = '\0';
      size_t n = snprintf(wfname, sizeof(wfname) - 1, "%s.%s", fname, "wf");
      if (OB_UNLIKELY(n <= 0) || OB_UNLIKELY(n >= sizeof(wfname) - 1)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_STDERR("snprintf error");
      } else if (wfname[0] == '\0') {
        LOG_STDERR("strncpy log filename = %s error\n", fname);
        (void)log_.close();
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(wflog_.open(wfname))) {
        LOG_STDERR("wflog open error log=%s\n", wfname);
        (void)log_.close();
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == (ptr = ob_malloc(sizeof(ObLogBuffer), ObModIds::OB_ELECTION_ASYNC_LOG))) {
        LOG_STDERR("ob malloc ObAsyncLog error. ptr = %p\n", ptr);
        (void)log_.close();
        if (open_wf) {
          (void)wflog_.close();
        }
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        log_buffer_ = new (ptr) ObLogBuffer();

        if (OB_FAIL(start())) {
          LOG_STDERR("ObAsyncLog pthread create error");
          (void)log_.close();
          if (open_wf) {
            (void)wflog_.close();
          }
          ret = OB_ERR_UNEXPECTED;
        } else {
          log_level_ = level;
          is_inited_ = true;
          destroyed_ = false;
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != log_buffer_) {
        log_buffer_->~ObLogBuffer();
        ob_free(log_buffer_);
        log_buffer_ = NULL;
      }
    }
  }

  return ret;
}

void ObAsyncLog::destroy(void)
{
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    tmp_ret = OB_NOT_INIT;
  } else if (destroyed_) {
    LOG_STDERR("ObAsyncLog destroyed\n");
    tmp_ret = OB_NOT_RUNNING;
  } else if (NULL == log_buffer_) {
    LOG_STDERR("ObAsyncLog log buffer is NULL\n");
    tmp_ret = OB_ERR_UNEXPECTED;
  } else {
    ObThreadPool::stop();
    is_inited_ = false;
    log_buffer_->destroy();
    destroyed_ = true;
    log_warn_ = true;

    ObThreadPool::wait();

    if (log_.is_opened()) {
      (void)log_.close();
    }
    if (wflog_.is_opened()) {
      (void)wflog_.close();
    }
    if (NULL != log_buffer_) {
      log_buffer_->~ObLogBuffer();
      ob_free(log_buffer_);
      log_buffer_ = NULL;
    }
    ObThreadPool::destroy();
  }
  UNUSED(tmp_ret);
}

int ObAsyncLog::set_log_level(const int level)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    LOG_STDERR("ObAsyncLog not inited\n");
    ret = OB_NOT_INIT;
  } else if (destroyed_) {
    LOG_STDERR("ObAsyncLog destroyed\n");
    ret = OB_NOT_RUNNING;
  } else if (OB_LOG_LEVEL_DEBUG < level || OB_LOG_LEVEL_ERROR > level) {
    LOG_STDERR("invalid argument level = %d\n", level);
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_level_ = level;
  }

  return ret;
}

int ObAsyncLog::set_max_file_size(const int64_t size)
{

  int ret = OB_SUCCESS;

  if (!is_inited_) {
    LOG_STDERR("ObAsyncLog not inited\n");
    ret = OB_NOT_INIT;
  } else if (destroyed_) {
    LOG_STDERR("ObAsyncLog destroyed\n");
    ret = OB_NOT_RUNNING;
  } else if (size <= 0) {
    LOG_STDERR("invalid argument, size = %ld\n", size);
  } else if (OB_FAIL(log_.set_max_file_size(size))) {
    LOG_STDERR("log file set max file size error. ret = %d size = %ld\n", ret, size);
  } else if (wflog_.is_opened() && OB_FAIL(wflog_.set_max_file_size(size))) {
    LOG_STDERR("wflog file set max file size error. ret = %d size = %ld\n", ret, size);
  } else {
    // do nothing
  }

  return ret;
}

void ObAsyncLog::run1()
{
  int err_no = 0;
  if (OB_UNLIKELY(0 != (err_no = prctl(PR_SET_NAME, "ELE_ALOG", 0, 0, 0)))) {
    LOG_STDERR("failed to prctl PR_SET_NAME, err_no=%d", err_no);
  } else {
    do_flush_log_();
  }
}

void ObAsyncLog::do_flush_log_(void)
{
  ObLogItem* item = NULL;
  int tmp_ret = OB_SUCCESS;

  while (is_inited_ && !destroyed_) {
    if (OB_SUCCESS != (tmp_ret = log_buffer_->pop(item))) {
      LOG_STDERR("log buffer pop error = %d\n", tmp_ret);
    } else if (NULL != item && NULL != item->get_buf() && item->get_size() > 0) {
      if (log_.is_opened()) {
        log_.write(item->get_buf(), item->get_size());
      }
      if (wflog_.is_opened() && (item->get_log_level() <= wflog_level_) && log_warn_) {
        wflog_.write(item->get_buf(), item->get_size());
      }
      ObLogItemFactory::release(item);
      item = NULL;
    } else {
      LOG_STDERR("ObAsyncLog do flush log error\n");
    }
  }
}

int ObAsyncLog::logdata_print_info_(char* buf, int64_t& pos, const char* mod_name, const int level,
    const char* levelstr, const char* file, const int32_t line, const char* func, const int64_t last_log_time)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    LOG_STDERR("ObAsyncLog not inited\n");
    ret = OB_NOT_INIT;
  } else if (destroyed_) {
    LOG_STDERR("ObAsyncLog destroyed\n");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(buf) || pos < 0 || line < 0) {
    LOG_STDERR("invalid argument buf = %p, pos = %ld, line = %d\n", buf, pos, line);
    ret = OB_INVALID_ARGUMENT;
  } else if (level <= OB_LOGGER.get_log_level()) {
    int n = 0;
    time_t t = 0;
    struct tm tm;
    struct timeval tv;
    char timebuf[64] = {0};

    if (pos < ObLogItem::MAX_LOG_SIZE) {
      t = time(NULL);
      ob_fast_localtime(
          ObLogger::get_logger().last_unix_sec_, ObLogger::get_logger().last_localtime_, static_cast<time_t>(t), &tm);

      size_t strftime_len = strftime(timebuf, sizeof(timebuf), "%F %T", &tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_STDERR("strftime error");
      } else {
        if (OB_UNLIKELY(0 != gettimeofday(&tv, NULL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_STDERR("gettimeofday error");
        } else {
          const uint64_t* trace_id = ObCurTraceId::get();
          n = snprintf(buf + pos,
              ObLogItem::MAX_LOG_SIZE - pos,
              "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
              "%-5s %s%s (%s:%d) [%ld][" TRACE_ID_FORMAT "] [log=%ld] ",
              tm.tm_year + 1900,
              tm.tm_mon + 1,
              tm.tm_mday,
              tm.tm_hour,
              tm.tm_min,
              tm.tm_sec,
              tv.tv_usec,
              levelstr,
              mod_name,
              func,
              file,
              line,
              GETTID(),
              trace_id[0],
              trace_id[1],
              last_log_time);
          if (OB_UNLIKELY(n <= 0) || OB_UNLIKELY(n >= ObLogItem::MAX_LOG_SIZE - pos)) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_STDERR("snprintf error");
          } else {
            pos += n;
          }
        }
      }
    }
  }

  return ret;
}

void ObAsyncLog::async_log_message_kv(const char* mod_name, const int level, const char* levelstr, const char* file,
    const int32_t line, const char* func, const char* fmt, ...)
{
  int n = 0;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  va_list ap;
  va_start(ap, fmt);
  ObLogItem* item = NULL;
  static RLOCAL(int64_t, last_log_time);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (destroyed_) {
    LOG_STDERR("ObAsyncLog destroyed\n");
    ret = OB_NOT_RUNNING;
  } else if (level <= OB_LOGGER.get_log_level()) {
    if (OB_ISNULL(mod_name) || OB_ISNULL(levelstr) || OB_ISNULL(file) || OB_ISNULL(func) || OB_ISNULL(fmt)) {
      LOG_STDERR("invalid argument, mod_name = %p, levelstr = %p, file = %p, func = %p, fmt = %p",
          mod_name,
          levelstr,
          file,
          func,
          fmt);
    } else {
      bool overwrite = false;
      const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
      if (level <= OB_LOG_LEVEL_WARN) {
        overwrite = true;
      }
      item = ObLogItemFactory::alloc();
      if (NULL == item) {
        LOG_STDERR("op alloc ObLogItem error\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        char* buf = item->get_buf();
        if (NULL == buf) {
          LOG_STDERR("log item buf is NULL buf = %p", buf);
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(logdata_print_info_(buf, pos, mod_name, level, levelstr, file, line, func, last_log_time))) {
          LOG_STDERR("logdata print info error ret = %d\n", ret);
        } else {
          if (pos < ObLogItem::MAX_LOG_SIZE) {
            n = vsnprintf(buf + pos, ObLogItem::MAX_LOG_SIZE - pos, fmt, ap);
            pos += n;
          }
          if (pos > ObLogItem::MAX_LOG_SIZE - 2) {
            pos = ObLogItem::MAX_LOG_SIZE - 2;
          }
          buf[pos++] = '\n';
          buf[pos] = '\0';
          item->set_size(pos);
          item->set_log_level(level);
          ObLogItem* old_item = NULL;
          if (OB_FAIL(log_buffer_->push(item, old_item, overwrite))) {
            LOG_STDERR("push log item to buffer error ret = %d\n", ret);
          } else if (overwrite && NULL != old_item) {
            ObLogItemFactory::release(old_item);
            old_item = NULL;
          } else {
            // do nothing
          }
          last_log_time = ::oceanbase::common::ObTimeUtility::current_time() - start_time;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != item) {
      ObLogItemFactory::release(item);
      item = NULL;
    }
  }
  va_end(ap);
}

ObAsyncLog& ObAsyncLog::getLogger(void)
{
  static ObAsyncLog logger;
  return logger;
}

int64_t ObAsyncLog::get_write_size()
{
  return log_.get_write_size() + wflog_.get_write_size();
}

}  // namespace election
}  // namespace oceanbase
