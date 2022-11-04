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

#ifndef _OB_PERF_EVENT_H
#define _OB_PERF_EVENT_H 1
#include <stdint.h>
#include "lib/ob_name_id_def.h"
#include "lib/time/ob_time_utility.h"
#include "lib/trace/ob_seq_event_recorder.h"
#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase
{
namespace common
{
// <event_id, timestamp>
struct ObTimestampEvent
{
  ObTimestampEvent()
  {
    id_ = 0;
    timestamp_ = 0;
  }
  void set_id_time(ObEventID id) {id_ = id; timestamp_ = ObTimeUtility::current_time(); }
  ObEventID id_;
  int64_t timestamp_;
};

struct ObPerfConfig
{
  static void start();
  static void stop();

  static const char* const data_filename_;
  static bool enabled_;
  static int64_t sampling_period_;  // event period to sample
};

class ObPerfEventRecorder: public ObSeqEventRecorder<ObTimestampEvent, 500, 8>
{
public:
  ObPerfEventRecorder()
      :sampling_counter_(0),
       cat_id_(0)
  {
    STATIC_ASSERT(sizeof(ObTimestampEvent)==16, "sizeof(ObTimestampEvent)=16");
    STATIC_ASSERT(sizeof(*this)<=8192, "sizeof(*this)<=8192");
  }
  void reset()
  {
    ObSeqEventRecorder<ObTimestampEvent, 500, 8>::reset();
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
// get the thread-local recorder instance
inline ObPerfEventRecorder *get_perf_recorder()
{
  return GET_TSI(ObPerfEventRecorder);
}

// binary output of perf data
class ObPerfWriter
{
public:
  ObPerfWriter()
      :fd_(-1) {}
  ~ObPerfWriter() { (void)close(); }

  int open(const char* filename);
  int close();
  int write(const ObPerfEventRecorder &rec);
  bool is_opened() const { return fd_ != -1; }
public:
  static char PERF_MAGIC[8];
private:
  int fd_;
};
// get the global perf_writer
inline ObPerfWriter *get_perf_writer()
{
  static ObPerfWriter *writer = NULL;
  if (OB_UNLIKELY(NULL == writer)) {
    writer = new ObPerfWriter();
    LIB_LOG(INFO, "new perf writer", KP(writer));
  }
  return writer;
}

// binary input of perf data
class ObPerfReader
{
public:
  ObPerfReader()
      :fd_(-1) {}
  ~ObPerfReader() { (void)close(); }

  int open(const char* filename);
  int close();
  int read(ObPerfEventRecorder &rec);
private:
  int fd_;
};

} // end namespace common
} // end namespace oceanbase
#define REC_PERF_EVENT(recorder, perf_event) (recorder).add_event().set_id_time(OB_ID(perf_event))
#define REC_PERF_CAT_ID(recorder, cat_id) (recorder).set_cat_id(cat_id)
#define THE_PERF ::oceanbase::common::get_perf_recorder()
#define THE_PERF_WRITER ::oceanbase::common::get_perf_writer()

////////////////////////////////////////////////////////////////
// perf event APIs
////////////////////////////////////////////////////////////////
#define ENABLE_PERF_EVENT() oceanbase::common::ObPerfConfig::start()
#define DISABLE_PERF_EVENT() oceanbase::common::ObPerfConfig::stop()
#define IS_PERF_EVENT_ENABLED() oceanbase::common::ObPerfConfig::enabled_

    // single event point
#define OB_PERF_EVENT(event)                            \
  (IS_PERF_EVENT_ENABLED() && OB_NOT_NULL(THE_PERF)?    \
   REC_PERF_EVENT(*THE_PERF, event):                     \
   (void)0)

// the following macros should be used in pairs
#define OB_PERF_EVENT_BEGIN(event) OB_PERF_EVENT(event ## _begin)
#define OB_PERF_EVENT_END(event) OB_PERF_EVENT(event ## _end)
// misc
#define PERF_RESET_RECORDER()                           \
  do {                                                  \
    auto perf = THE_PERF;                               \
    if (IS_PERF_EVENT_ENABLED() && OB_NOT_NULL(perf)) { \
      THE_PERF->reset();                                \
    }                                                   \
  } while (0)

#define PERF_SET_CAT_ID(id)                             \
  (IS_PERF_EVENT_ENABLED() && OB_NOT_NULL(THE_PERF)?    \
   REC_PERF_CAT_ID(*THE_PERF, id) :                      \
   (void)(0))                                           \

#define PERF_GATHER_DATA()                                      \
  ((IS_PERF_EVENT_ENABLED() && OB_NOT_NULL(THE_PERF) &&         \
    THE_PERF->need_write(ObPerfConfig::sampling_period_))?      \
   (void)THE_PERF_WRITER->write(*THE_PERF) :                    \
   (void)(0))

#endif /* _OB_PERF_EVENT_H */
