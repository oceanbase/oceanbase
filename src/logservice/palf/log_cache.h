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

#ifndef OCEANBASE_PALF_LOG_CACHE_
#define OCEANBASE_PALF_LOG_CACHE_

#include <cstdint>                                       // int64_t

namespace oceanbase
{
namespace palf
{
class LSN;
class IPalfHandleImpl;

class LogHotCache
{
public:
  LogHotCache();
  ~LogHotCache();
  void destroy();
  void reset();
  int init(const int64_t palf_id, IPalfHandleImpl *palf_handle_impl);
  int read(const LSN &read_begin_lsn,
           const int64_t in_read_size,
           char *buf,
           int64_t &out_read_size) const;
private:
  int64_t palf_id_;
  IPalfHandleImpl *palf_handle_impl_;
  mutable int64_t read_size_;
  mutable int64_t hit_count_;
  mutable int64_t read_count_;
  mutable int64_t last_print_time_;
  bool is_inited_;
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_CACHE_
