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

#ifndef OCEANBASE_OB_LOG_UTILITY_H_
#define OCEANBASE_OB_LOG_UTILITY_H_

#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace logservice
{
/*
 * Memory size.
 */
static const int64_t _K_ = (1L << 10);
static const int64_t _M_ = (1L << 20);
static const int64_t _G_ = (1L << 30);
static const int64_t _T_ = (1L << 40);

#define TS_TO_STR(tstamp) HumanTstampConverter(tstamp).str()
#define TVAL_TO_STR(tval) HumanTimevalConverter(tval).str()

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

} // namespace logservice
} // namespace oceanbase

#endif

