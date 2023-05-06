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
 *
 * OBCDC Utilities
 */

#ifndef OCEANBASE_LOG_FETCHER_UTILS_H__
#define OCEANBASE_LOG_FETCHER_UTILS_H__

#include "lib/allocator/ob_allocator.h"       // ObIAllocator
#include "lib/allocator/ob_malloc.h"          // ob_malloc
#include "lib/allocator/ob_mod_define.h"      // ObModIds
#include "lib/container/ob_iarray.h"          // ObIArray
#include "lib/container/ob_array.h"           // ObArray
#include "common/object/ob_object.h"          // ObObj
#include "storage/blocksstable/ob_datum_row.h"// ObRowDml

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace common
{
class ObString;
}

namespace logfetcher
{
/*
 * Memory size.
 */
static const int64_t _K_ = (1L << 10);
static const int64_t _M_ = (1L << 20);
static const int64_t _G_ = (1L << 30);
static const int64_t _T_ = (1L << 40);

/*
 * Time utils.
 * Microsecond Timestamp Generator.
 * Time Constants.
 * Stop Watch.
 * Time Marker.
*/
inline void usec_sleep(const int64_t u) { ob_usleep(static_cast<int32_t>(u)); }

typedef common::ObSEArray<uint64_t, 16> ObLogIdArray;
typedef common::ObSEArray<palf::LSN, 16> ObLogLSNArray;
#define TS_TO_STR(tstamp) HumanTstampConverter(tstamp).str()
#define NTS_TO_STR(tstamp) HumanTstampConverter(tstamp/NS_CONVERSION).str()
#define TVAL_TO_STR(tval) HumanTimevalConverter(tval).str()

const int64_t NS_CONVERSION = 1000L;
const int64_t _MSEC_ = 1000L;
const int64_t _SEC_ = 1000L * _MSEC_;
const int64_t _MIN_ = 60L * _SEC_;
const int64_t _HOUR_ = 60L * _MIN_;
const int64_t _DAY_ = 24L * _HOUR_;
const int64_t _YEAR_ = 365L * _DAY_;

int print_human_tstamp(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t usec_tstamp);

int print_human_timeval(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t usec_tval);

class HumanTstampConverter
{
public:
  explicit HumanTstampConverter(const int64_t usec_tstamp)
  {
    buf_[0] = '\0';
    int64_t pos = 0;
    (void)print_human_tstamp(buf_, BufLen, pos, usec_tstamp);
  }
  virtual ~HumanTstampConverter()
  {
    buf_[0] = '\0';
  }
  const char* str() const
  {
    return buf_;
  }
private:
  const static int64_t BufLen = 64;
  char buf_[BufLen];
};

class HumanTimevalConverter
{
public:
  explicit HumanTimevalConverter(const int64_t usec_tval)
  {
    buf_[0] = '\0';
    int64_t pos = 0;
    (void)print_human_timeval(buf_, BufLen, pos, usec_tval);
  }
  virtual ~HumanTimevalConverter()
  {
    buf_[0] = '\0';
  }
  const char *str() const
  {
    return buf_;
  }
private:
  const static int64_t BufLen = 64;
  char buf_[BufLen];
};

inline int64_t get_timestamp() { return ::oceanbase::common::ObTimeUtility::current_time(); }

class HumanDataSizeConverter
{
  static const int64_t BufSize = 128;
public:
  explicit HumanDataSizeConverter(const int64_t bytes) : bytes_(bytes) {}
  ~HumanDataSizeConverter() {}
  const char* to_data_size_cstr()
  {
    double val = 0;
    int64_t pos = 0;
    const char *unit = "";
    if (bytes_ < _K_) {
      val = (double)bytes_;
      unit = "B";
    }
    else if (bytes_ < _M_) {
      val = (double)bytes_ / (double)_K_;
      unit = "KB";
    }
    else if (bytes_ < _G_) {
      val = (double)bytes_ / (double)_M_;
      unit = "MB";
    }
    else {
      val = (double)bytes_ / (double)_G_;
      unit = "GB";
    }

    (void)common::databuff_printf(buf_, BufSize, pos, "%.2f%s", val, unit);

    return buf_;
  }
  const char *str()
  {
    return to_data_size_cstr();
  }
private:
  int64_t bytes_;
  char buf_[BufSize];
};

// Converting data sizes to strings
#define SIZE_TO_STR(size) HumanDataSizeConverter(size).str()

class TstampToDelay
{
public:
  explicit TstampToDelay(const int64_t tstamp)
  {
    if (common::OB_INVALID_TIMESTAMP == tstamp) {
      (void)snprintf(buf_, sizeof(buf_), "[INVALID]");
    } else {
      int64_t cur_time = get_timestamp();
      int64_t delay_us = (cur_time - tstamp) % _SEC_;
      int64_t delay_sec = (cur_time - tstamp) / _SEC_;

      buf_[0] = '\0';

      (void)snprintf(buf_, sizeof(buf_), "[%ld.%.06ld sec]", delay_sec, delay_us);
    }
  }
  ~TstampToDelay() { buf_[0] = '\0'; }
  const char *str() const
  {
    return buf_;
  }
private:
  const static int64_t BufLen = 64;
  char buf_[BufLen];
};

// Converting timestamps to DELAY strings.[1000.0001 sec]
#define TS_TO_DELAY(tstamp) TstampToDelay(tstamp).str()
#define NTS_TO_DELAY(tstamp) TstampToDelay(tstamp/NS_CONVERSION).str()

class StopWatch
{
public:
  StopWatch() : start_(0), elapsed_(0) { }
  virtual ~StopWatch() { }
public:
  void start() { start_ = get_timestamp(); }
  void pause() { elapsed_ += (get_timestamp() - start_); }
  void reset() { start_ = 0; elapsed_ = 0; }
  double elapsed_sec() const { return static_cast<double>(
                                      elapsed_msec()) / 1000.0; }
  int64_t elapsed_msec() const { return elapsed_usec() / 1000; }
  int64_t elapsed_usec() const { return elapsed_; }

private:
  int64_t start_;
  int64_t elapsed_;
};
int get_local_ip(ObString &local_ip);

double get_delay_sec(const int64_t tstamp);

template <class T, class CompareFunc>
int top_k(const common::ObArray<T> &in_array,
    const int64_t k_num,
    common::ObArray<T> &out_array,
    CompareFunc &compare_func)
{
  int ret = common::OB_SUCCESS;
  int64_t array_cnt = in_array.count();
  int64_t cnt = std::min(k_num, array_cnt);

  for (int64_t idx = 0; common::OB_SUCCESS == ret && idx < cnt; ++idx) {
    if (OB_FAIL(out_array.push_back(in_array.at(idx)))) {
      OBLOG_LOG(ERROR, "push back into slow array fail", KR(ret), K(idx));
    } else {
      // do nothing
    }
  }

  if (common::OB_SUCCESS == ret && array_cnt > 0) {
    if (array_cnt <= k_num) {
      // Construct a big top heap, with the top of the heap being the maximum value in the current out_array
      std::make_heap(out_array.begin(), out_array.end(), compare_func);
    } else {
      // Construct a big top heap, with the top of the heap being the maximum value in the current out_array
      std::make_heap(out_array.begin(), out_array.end(), compare_func);

      for (int64_t idx = k_num; common::OB_SUCCESS == ret && idx < array_cnt; ++idx) {
        // If the current element is smaller than the heap top element, replace the heap top element and re-std::make_heap
        if (compare_func(in_array.at(idx), out_array.at(0))) {
          out_array[0] = in_array.at(idx);
          std::make_heap(out_array.begin(), out_array.end(), compare_func);
        } else {
          // do nothing
        }
      } // for
    }

    if (common::OB_SUCCESS == ret) {
      std::sort_heap(out_array.begin(), out_array.end(), compare_func);
    }
  }

  return ret;
}

char *lbt_oblog();

class ObLogTimeMonitor
{
public:
  ObLogTimeMonitor(const char* log_msg_prefix, bool enable = true);
  ~ObLogTimeMonitor();
  // return cost to last mark time
  int64_t mark_and_get_cost(const char *log_msg_suffix, bool need_print = false);
private:
  bool enable_;
  const char* log_msg_prefix_;
  int64_t start_time_usec_;
  int64_t last_mark_time_usec_;
};

typedef int32_t offset_t;

} // namespace logfetcher
} // namespace oceanbase
#endif /* OCEANBASE_LOG_FETCHER_UTILS_H__ */
