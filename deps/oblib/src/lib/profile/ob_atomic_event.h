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

#ifndef _OB_ATOMIC_EVENT_H
#define _OB_ATOMIC_EVENT_H 1
#define PERF_ATOMIC_OP 0
#if PERF_ATOMIC_OP
#include <stdint.h>
#include "lib/ob_name_id_def.h"
#include "lib/time/ob_time_utility.h"
#include "lib/trace/ob_seq_event_recorder.h"
// private interface
#define ATOMIC_EVENT_RECORDER ::oceanbase::common::get_the_atomic_event_recorder()
#define ATOMIC_EVENT_WRITER ::oceanbase::common::get_the_atomic_event_writer()
// public interface
#define OB_ENABLE_ATOMIC_EVENT() (::oceanbase::common::ObAtomicOpEventPerfConfig::start())
#define OB_DISABLE_ATOMIC_EVENT() (::oceanbase::common::ObAtomicOpEventPerfConfig::stop())
#define OB_ATOMIC_EVENT_IS_ENABLED() (::oceanbase::common::ObAtomicOpEventPerfConfig::enabled_)
#define OB_ATOMIC_EVENT(ev)\
  ((::oceanbase::common::ObAtomicOpEventPerfConfig::enabled_)?ATOMIC_EVENT_RECORDER.add_event().set_id_time(ID(ev), __LINE__, __FILE__):(void)0)
#define OB_ATOMIC_EVENT_SET_CAT_ID(id)\
  ((::oceanbase::common::ObAtomicOpEventPerfConfig::enabled_)?ATOMIC_EVENT_RECORDER.set_cat_id(id):(void)0)

#define OB_ATOMIC_EVENT_RESET_RECORDER() ATOMIC_EVENT_RECORDER.reset()
#define OB_ATOMIC_EVENT_GATHER_DATA()\
  ((::oceanbase::common::ObAtomicOpEventPerfConfig::enabled_ && ATOMIC_EVENT_RECORDER.need_write(ObAtomicOpEventPerfConfig::sampling_period_))? (void)ATOMIC_EVENT_WRITER.write(ATOMIC_EVENT_RECORDER) : (void)(0))

namespace oceanbase
{
namespace common
{
struct ObAtomicOpEvent
{
  ObEventID id_;
  int32_t line_;
  int64_t timestamp_;
  const char* filename_;
  inline void set_id_time(ObEventID id, int32_t line, const char* filename)
  {
    id_ = id;
    timestamp_ = ObTimeUtility::current_time();
    filename_ = filename;
    line_ = line;
  }
};

class ObAtomicOpEventRecorder: public ObSeqEventRecorder<ObAtomicOpEvent, 339, 8>
{
public:
  ObAtomicOpEventRecorder()
      :sampling_counter_(0),
       cat_id_(0)
  {
    STATIC_ASSERT(sizeof(ObAtomicOpEvent)==24, "sizeof(ObAtomicOpEvent)=24");
    STATIC_ASSERT(sizeof(*this)<=8192, "sizeof(*this)<=8192");
  }
  void reset()
  {
    ObSeqEventRecorder<ObAtomicOpEvent, 339, 8>::reset();
    cat_id_ = 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  inline bool need_write(int64_t sampling_period)
  {
    bool bret = false;
    if (0 != cat_id_ && count() > 2) {
      bret = (++sampling_counter_ >= sampling_period);
      if (bret) {
        sampling_counter_ = 0;
      }
    }
    return bret;
  }
  void set_cat_id(uint64_t cat_id) { cat_id_ = cat_id; }
  uint64_t get_cat_id() const { return cat_id_; }
private:
  int64_t sampling_counter_;
  uint64_t cat_id_;  // category id
};

// binary output of perf data
class ObAtomicOpEventWriter
{
public:
  ObAtomicOpEventWriter()
      :fd_(-1) {}
  ~ObAtomicOpEventWriter() { (void)close(); }

  int open(const char* filename);
  int close();
  int write(const ObAtomicOpEventRecorder &rec);
  bool is_opened() const { return fd_ != -1; }
public:
  static char ATOMIC_MAGIC[8];
private:
  int fd_;
};

// binary input of perf data
class ObAtomicOpEventReader
{
public:
  ObAtomicOpEventReader()
      :fd_(-1) {}
  ~ObAtomicOpEventReader() { (void)close(); }

  int open(const char* filename);
  int close();
  int read(ObAtomicOpEventRecorder &rec);
private:
  static const int64_t REC_BUF_SIZE = 8192;
  int fd_;
  char rec_buf_[REC_BUF_SIZE];
};

struct ObAtomicOpEventPerfConfig
{
  static void start();
  static void stop();

  static const char* const data_filename_;
  static bool enabled_;
  static int64_t sampling_period_;  // event period to sample
};
// get the thread-local recorder instance
ObAtomicOpEventRecorder &get_the_atomic_event_recorder();
// get the global perf_writer
ObAtomicOpEventWriter &get_the_atomic_event_writer();

inline ObAtomicOpEventRecorder &get_the_atomic_event_recorder()
{
  auto *ptr = GET_TSI(ObAtomicOpEventRecorder);
  OB_ASSERT(ptr);
  return *ptr;
}

inline ObAtomicOpEventWriter &get_the_atomic_event_writer()
{
  static ObAtomicOpEventWriter *writer = NULL;
  if (OB_UNLIKELY(NULL == writer)) {
    writer = new ObAtomicOpEventWriter();
    LIB_LOG(INFO, "new atomic event perf writer", KP(writer));
  }
  return *writer;
}

} // end namespace common
} // end namespace oceanbase
#else
#define OB_ENABLE_ATOMIC_EVENT() (void)(0)
#define OB_DISABLE_ATOMIC_EVENT() (void)(0)
#define OB_ATOMIC_EVENT_IS_ENABLED() (false)
#define OB_ATOMIC_EVENT(ev) (void)(0)
#define OB_ATOMIC_EVENT_SET_CAT_ID(id) (void)(0)
#define OB_ATOMIC_EVENT_RESET_RECORDER() (void)(0)
#define OB_ATOMIC_EVENT_GATHER_DATA() (void)(0)
#endif
#endif /* _OB_ATOMIC_EVENT_H */
