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
#include "lib/profile/ob_perf_event.h"
#include "lib/utility/ob_print_utils.h"
#include <unistd.h>
#include <sys/uio.h>  // for iov
namespace oceanbase
{
namespace common
{
const char* const ObPerfConfig::data_filename_ = "observer.pfda";
bool ObPerfConfig::enabled_ = false;
int64_t ObPerfConfig::sampling_period_ = 10;

void ObPerfConfig::start()
{
  int err = OB_SUCCESS;
  ObPerfWriter *perf_writer = get_perf_writer();
  if (OB_NOT_NULL(perf_writer) && !perf_writer->is_opened()) {
    if (OB_SUCCESS != (err = perf_writer->open(data_filename_))) {
      LOG_WARN_RET(err, "failed to open obperf data file", K(err));
    } else {
      enabled_ = true;
    }
  }
}

void ObPerfConfig::stop()
{
  enabled_ = false;
  ObPerfWriter *perf_writer = get_perf_writer();
  int err = OB_SUCCESS;
  if (OB_NOT_NULL(perf_writer) && perf_writer->is_opened()) {
    if (OB_SUCCESS != (err = perf_writer->close())) {
      LOG_WARN_RET(err, "failed to close obperf data file", K(err));
    }
  }
}

int64_t ObPerfEventRecorder::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t prev_ts = 0;
  int64_t total_time = 0;
  const char* event_name = NULL;
  for (int64_t i = 0; i < next_idx_; ++i) {
    const ObTimestampEvent &ev = events_[i];
    event_name = NAME(ev.id_);
    if (prev_ts == 0) {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "cat_id=%lu begin_ts=%ld ",
                                                 cat_id_, ev.timestamp_);
      prev_ts = ev.timestamp_;
    }
    if (NULL == event_name) {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%hu] u=%ld ",
                                                 ev.id_, ev.timestamp_ - prev_ts);
    } else {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%s] u=%ld ",
                                                 event_name, ev.timestamp_ - prev_ts);
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

char ObPerfWriter::PERF_MAGIC[8] = {'#', 'p', 'e', 'r', 'f', 'd', 'a', '\0'};
int ObPerfWriter::open(const char* filename)
{
  int ret = OB_SUCCESS;
  if (-1 == (fd_ = ::open(filename, O_CREAT|O_TRUNC|O_WRONLY|O_APPEND, S_IWUSR|S_IRUSR|S_IRGRP))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to open perf data file", K(ret), KCSTRING(filename), K(errno));
  } else {
    ssize_t len = ::write(fd_, PERF_MAGIC, sizeof(PERF_MAGIC));
    if (len != SIZEOF(PERF_MAGIC)) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to write header", K(ret), K(len));
    } else {
      LOG_INFO("open file succ", K_(fd), KCSTRING(filename));
    }
  }
  return ret;
}

int ObPerfWriter::write(const ObPerfEventRecorder &rec)
{
  int ret = OB_SUCCESS;
  if (-1 != fd_) {
    int64_t rec_size = sizeof(ObTimestampEvent)*rec.count();
    uint64_t cat_id = rec.get_cat_id();
    if (rec_size > 0) {
      struct ::iovec buf2[3];
      buf2[0].iov_base = &cat_id;
      buf2[0].iov_len = sizeof(cat_id);
      buf2[1].iov_base = &rec_size;
      buf2[1].iov_len = sizeof(rec_size);
      buf2[2].iov_base = const_cast<ObTimestampEvent*>(rec.get_events_array());
      buf2[2].iov_len = rec_size;
      ssize_t len = ::writev(fd_, buf2, 3);
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

int ObPerfWriter::close()
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

int ObPerfReader::open(const char* filename)
{
  int ret = OB_SUCCESS;
  char buf[sizeof(ObPerfWriter::PERF_MAGIC)];
  if (-1 == (fd_ = ::open(filename, O_RDONLY))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to open perf data file", K(ret), KCSTRING(filename), K(errno));
  } else {
    ssize_t len = ::read(fd_, buf, sizeof(ObPerfWriter::PERF_MAGIC));
    if (len != SIZEOF(ObPerfWriter::PERF_MAGIC)) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to read header", K(ret), K(len));
    } else if (0 != memcmp(buf, ObPerfWriter::PERF_MAGIC, sizeof(ObPerfWriter::PERF_MAGIC))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file header magic is wrong for perf reader");
    } else {
      LOG_INFO("open file succ", K_(fd), KCSTRING(filename));
    }
  }
  return ret;
}

int ObPerfReader::read(ObPerfEventRecorder &rec)
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
    if (rec_len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read invalid record length", K(ret), K(rec_len));
    } else if (rec_len > SIZEOF(ObTimestampEvent)*rec.MAX_EVENT_COUNT) {
      off_t offset = lseek(fd_, 0, SEEK_CUR);
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("the PerfEventRecorder's event buffer is not enough",
               K(ret), K(rec_len), K(offset),
               "buf_len", SIZEOF(ObTimestampEvent)*rec.MAX_EVENT_COUNT);
    } else {
      len = ::read(fd_, rec.get_events_array(), rec_len);
      if (len < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to read perf record", K(ret), K(len), K(errno));
      } else if (len != rec_len) {
        off_t offset = lseek(fd_, 0, SEEK_CUR);
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to read record", K(ret), K(len), K(rec_len), K(offset));
      } else {
        rec.set_cat_id(cat_id);
        rec.set_count(rec_len/(sizeof(ObTimestampEvent)));
      }
    }
  }
  return ret;
}

int ObPerfReader::close()
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
