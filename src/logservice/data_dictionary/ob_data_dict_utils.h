/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Meta Dict Struct Define
* This file defines Struct of Meta Dict
*/

#ifndef OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_UTILS_
#define OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_UTILS_

#include "lib/string/ob_string.h"       // ObString
#include "lib/time/ob_time_utility.h"   // ObTimeUtility
#include "common/ob_clock_generator.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
}
namespace datadict
{

#define REACH_TIME_INTERVAL_THREAD_LOCAL(i) \
  ({ \
    bool bret = false; \
    static thread_local volatile int64_t last_time = 0; \
    int64_t cur_time = common::ObClockGenerator::getClock(); \
    int64_t old_time = last_time; \
    if (OB_UNLIKELY((i + last_time) < cur_time) \
        && old_time == ATOMIC_CAS(&last_time, old_time, cur_time)) \
    { \
      bret = true; \
    } \
    bret; \
  })

#define RETRY_FUNC_ON_ERROR_WITH_SLEEP(err_no, sleep_us, stop_flag, var, func, args...) \
  do {\
    if (OB_SUCC(ret)) \
    { \
      int64_t _retry_func_on_error_last_print_time = common::ObClockGenerator::getClock();\
      int64_t _retry_func_on_error_cur_print_time = 0;\
      const int64_t _PRINT_RETRY_FUNC_INTERVAL = 10 * _SEC_;\
      ret = (err_no); \
      while ((err_no) == ret && ! (stop_flag)) \
      { \
        ret = ::oceanbase::common::OB_SUCCESS; \
        ret = (var).func(args); \
        if (err_no == ret) { \
          ob_usleep(sleep_us); \
        }\
        _retry_func_on_error_cur_print_time = common::ObClockGenerator::getClock();\
        if (_retry_func_on_error_cur_print_time - _retry_func_on_error_last_print_time >= _PRINT_RETRY_FUNC_INTERVAL) {\
          _DATA_DICT_LOG(INFO, "It has been %ld us since last print, last_print_time=%ld, func_name=%s", \
              _PRINT_RETRY_FUNC_INTERVAL, _retry_func_on_error_last_print_time, #func);\
          _retry_func_on_error_last_print_time = _retry_func_on_error_cur_print_time;\
        }\
      } \
      if ((stop_flag)) \
      { \
        ret = OB_IN_STOP_STATE; \
      } \
    } \
  } while (0)

/*
 * Memory size.
 */
static const int64_t _K_ = (1L << 10);
static const int64_t _M_ = (1L << 20);
static const int64_t _G_ = (1L << 30);
static const int64_t _T_ = (1L << 40);

// time units.
const int64_t NS_CONVERSION = 1000L;
const int64_t _MSEC_ = 1000L;
const int64_t _SEC_ = 1000L * _MSEC_;
const int64_t _MIN_ = 60L * _SEC_;
const int64_t _HOUR_ = 60L * _MIN_;
const int64_t _DAY_ = 24L * _HOUR_;
const int64_t _YEAR_ = 365L * _DAY_;

void *ob_dict_malloc(const int64_t nbyte, const uint64_t tenant_id);
void ob_dict_free(void *ptr);

OB_INLINE int64_t get_timestamp_ns()
{
  return common::ObTimeUtility::current_time_ns();
}

OB_INLINE int64_t get_timestamp_us()
{
  return common::ObTimeUtility::current_time();
}

int deserialize_string_array(
    const char *buf,
    const int64_t data_len,
    int64_t &pos,
    ObIArray<ObString> &string_array,
    ObIAllocator &allocator);

int deep_copy_str(
    const ObString &src,
    ObString &dest,
    common::ObIAllocator &allocator);

int deep_copy_str_array(
    const ObIArray<ObString> &src_arr,
    ObIArray<ObString> &dest_arr,
    common::ObIAllocator &allocator);
OB_INLINE const char *extract_str(const ObString &str)
{
  return str.empty() ? "" : str.ptr();
}

int check_ls_leader(logservice::ObLogHandler *handler, bool &is_leader, int64_t &proposal_id);

} // namespace datadict
} // namespace oceanbase

#endif
