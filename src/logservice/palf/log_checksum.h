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

#ifndef OCEANBASE_LOGSERVICE_LOG_CHECKSUM_
#define OCEANBASE_LOGSERVICE_LOG_CHECKSUM_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace palf
{
class LogChecksum
{
public:
  LogChecksum();
  virtual ~LogChecksum() {}
public:
  int init(const int64_t id, const int64_t accum_checksum);
  void destroy();
  virtual int acquire_accum_checksum(const int64_t data_checksum,
                                     int64_t &accum_checksum);
  virtual int verify_accum_checksum(const int64_t data_checksum,
                                    const int64_t accum_checksum);
  static int verify_accum_checksum(const int64_t old_accum_checksum,
                                   const int64_t data_checksum,
                                   const int64_t expected_accum_checksum,
                                   int64_t &new_accum_checksum);
  virtual void set_accum_checksum(const int64_t accum_checksum);
  virtual void set_verify_checksum(const int64_t verify_checksum);
  virtual int rollback_accum_checksum(const int64_t curr_accum_checksum);
private:
  bool is_inited_;
  int64_t palf_id_;
  int64_t prev_accum_checksum_;
  int64_t accum_checksum_;
  int64_t verify_checksum_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogChecksum);
};
} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_CHECKSUM_
