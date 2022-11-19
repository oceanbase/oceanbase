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

#if PERF_ATOMIC_OP
#define USING_LOG_PREFIX LIB
#include "ob_atomic_event.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace common
{
const char* const ObAtomicOpEventPerfConfig::data_filename_ = "atomic_op.pfda";
bool ObAtomicOpEventPerfConfig::enabled_ = false;
int64_t ObAtomicOpEventPerfConfig::sampling_period_ = 10;

void ObAtomicOpEventPerfConfig::start()
{
  int err = OB_SUCCESS;
  ObAtomicOpEventWriter &perf_writer = get_the_atomic_event_writer();
  if (!perf_writer.is_opened()) {
    if (OB_SUCCESS != (err = perf_writer.open(data_filename_))) {
      LOG_WARN("failed to open obperf data file", K(err));
    } else {
      enabled_ = true;
    }
  }
}

void ObAtomicOpEventPerfConfig::stop()
{
  enabled_ = false;
  ObAtomicOpEventWriter &perf_writer = get_the_atomic_event_writer();
  int err = OB_SUCCESS;
  if (perf_writer.is_opened()) {
    if (OB_SUCCESS != (err = perf_writer.close())) {
      LOG_WARN("failed to close obperf data file", K(err));
    }
  }
}

int64_t ObAtomicOpEventRecorder::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t prev_ts = 0;
  int64_t total_time = 0;
  const char* event_name = NULL;
  for (int64_t i = 0; i < next_idx_; ++i) {
    const ObAtomicOpEvent &ev = events_[i];
    event_name = NAME(ev.id_);
    if (prev_ts == 0) {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "cat_id=%lu begin_ts=%ld ",
                                                 cat_id_, ev.timestamp_);
      prev_ts = ev.timestamp_;
    }
    if (NULL == event_name) {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%hu] u=%ld @%s:%d",
                                                 ev.id_, ev.timestamp_ - prev_ts, ev.filename_, ev.line_);
    } else {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%s] u=%ld @%s:%d",
                                                 event_name, ev.timestamp_ - prev_ts, ev.filename_, ev.line_);
    }
    total_time += ev.timestamp_ - prev_ts;
    prev_ts = ev.timestamp_;
  }
  (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|total_timeu=%ld", total_time);
  if (dropped_events_ > 0) {
    (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, " DROPPED_EVENTS=%ld", dropped_events_);
  }
  return pos;
}

char ObAtomicOpEventWriter::ATOMIC_MAGIC[8] = {'#', 'a', 't', 'p', 'f', 'd', 'a', '\0'};
int ObAtomicOpEventWriter::open(const char* filename)
{
  int ret = OB_SUCCESS;
  if (-1 == (fd_ = ::open(filename, O_CREAT|O_TRUNC|O_WRONLY|O_APPEND, S_IWUSR|S_IRUSR|S_IRGRP))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to open perf data file", K(ret), K(filename), K(errno));
  } else {
    ssize_t len = ::write(fd_, ATOMIC_MAGIC, sizeof(ATOMIC_MAGIC));
    if (len != SIZEOF(ATOMIC_MAGIC)) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to write header", K(ret), K(len));
    } else {
      LOG_INFO("open file succ", K_(fd), K(filename));
    }
  }
  return ret;
}

int ObAtomicOpEventWriter::write(const ObAtomicOpEventRecorder &rec)
{
  int ret = OB_SUCCESS;
  if (-1 != fd_) {
    RLOCAL(char *, rec_buf);
    if (NULL == rec_buf) {
      rec_buf = new char[40*ObAtomicOpEventRecorder::MAX_EVENT_COUNT];  // never free
    }
    char *p = rec_buf;
    char *p_end = rec_buf + 8192;
    int64_t N = rec.count();
    int64_t namelen = 0;
    const char* basename = NULL;
    for (int64_t i = 0; i < N && (p + 40 <= p_end); i++) {
      const ObAtomicOpEvent &ev = rec.get_event(i);
      (*(int32_t*)(p)) = ev.id_;
      p += sizeof(int32_t);  // 4
      (*(int32_t*)(p)) = ev.line_;
      p += sizeof(int32_t);  // 4
      (*(int64_t*)(p)) = ev.timestamp_;
      p += sizeof(int64_t);  // 8
      basename = strrchr(ev.filename_, '/');
      basename = (NULL != basename) ? basename + 1 : ev.filename_;
      namelen = MIN(strlen(basename), 23);
      memcpy(p, basename, namelen);
      p[namelen] = '\0';
      p += 24;  // total 40
    }
    int64_t rec_size = p - rec_buf;
    uint64_t cat_id = rec.get_cat_id();
    if (rec_size > 0) {
      struct ::iovec buf2[3];
      buf2[0].iov_base = &cat_id;
      buf2[0].iov_len = sizeof(cat_id);
      buf2[1].iov_base = &rec_size;
      buf2[1].iov_len = sizeof(rec_size);
      buf2[2].iov_base = rec_buf;
      buf2[2].iov_len = rec_size;
      ssize_t len = ::writev(fd_, buf2, 3);  // atomic
      if (len < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to write file", K(ret), K_(fd), K(errno));
      } else if (len != (SIZEOF(rec_size)+SIZEOF(cat_id)+rec_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write file", K(ret), K_(fd), K(len), K(rec_size));
      }
    }
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObAtomicOpEventWriter::close()
{
  int ret = OB_SUCCESS;
  if (-1 != fd_) {
    (void)fdatasync(fd_);
    if (0 != ::close(fd_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to close perf data file", K(ret), K_(fd), K(errno));
    } else {
      LOG_INFO("close file succ", K_(fd));
      fd_ = -1;
    }
  }
  return ret;
}

int ObAtomicOpEventReader::open(const char* filename)
{
  int ret = OB_SUCCESS;
  char buf[sizeof(ObAtomicOpEventWriter::ATOMIC_MAGIC)];
  if (-1 == (fd_ = ::open(filename, O_RDONLY))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to open perf data file", K(ret), K(filename), K(errno));
  } else {
    ssize_t len = ::read(fd_, buf, sizeof(ObAtomicOpEventWriter::ATOMIC_MAGIC));
    if (len != SIZEOF(ObAtomicOpEventWriter::ATOMIC_MAGIC)) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to read header", K(ret), K(len));
    } else if (0 != memcmp(buf, ObAtomicOpEventWriter::ATOMIC_MAGIC, sizeof(ObAtomicOpEventWriter::ATOMIC_MAGIC))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file header magic is wrong for atomic_op event reader");
    } else {
      LOG_INFO("open file succ", K_(fd), K(filename));
    }
  }
  return ret;
}

int ObAtomicOpEventReader::read(ObAtomicOpEventRecorder &rec)
{
  int ret = OB_SUCCESS;
  int64_t rec_len = 0;
  uint64_t cat_id = 0;
  struct ::iovec buf2[2];
  buf2[0].iov_base = &cat_id;
  buf2[0].iov_len = sizeof(cat_id);
  buf2[1].iov_base = &rec_len;
  buf2[1].iov_len = sizeof(rec_len);

  ssize_t len = ::readv(fd_, buf2, 2);
  if (0 == len) {
    ret = OB_ITER_END;
  } else if (len != sizeof(rec_len)+sizeof(cat_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to read record length", K(ret), K(len), "expected", sizeof(rec_len));
  } else {
    if (rec_len <= 0 || rec_len % 40 != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read invalid record length", K(ret), K(rec_len));
    } else if (rec_len > REC_BUF_SIZE) {
      off_t offset = lseek(fd_, 0, SEEK_CUR);
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("read buffer is not enough",
               K(ret), K(rec_len), K(offset));
    } else {
      len = ::read(fd_, rec_buf_, rec_len);
      if (len < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to read perf record", K(ret), K(len), K(errno));
      } else if (len != rec_len) {
        off_t offset = lseek(fd_, 0, SEEK_CUR);
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to read record", K(ret), K(len), K(rec_len), K(offset));
      } else {
        rec.set_cat_id(cat_id);
        char *p = rec_buf_;
        int64_t N = rec_len / 40;
        for (int64_t i = 0; i < N; ++i) {
          ObAtomicOpEvent &ev = rec.add_event();
          ev.id_ = static_cast<ObEventID>((*(int32_t*)(p)));
          p += sizeof(int32_t);  // 4
          ev.line_ = (*(int32_t*)(p));
          p += sizeof(int32_t);  // 4
          ev.timestamp_ = (*(int64_t*)(p));
          p += sizeof(int64_t);  // 8
          ev.filename_ = p;
          p += 24;  // total 40
        }
      }
    }
  }
  return ret;
}

int ObAtomicOpEventReader::close()
{
  int ret = OB_SUCCESS;
  if (-1 != fd_) {
    if (0 != ::close(fd_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to close perf data file", K(ret), K_(fd), K(errno));
    } else {
      LOG_INFO("close file succ", K_(fd));
      fd_ = -1;
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif
