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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_UTIL_H_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_UTIL_H_

#include <stdint.h>
#include "ob_cdc_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace cdc
{
class ObExtRpcQit
{
public:
  ObExtRpcQit() : deadline_(common::OB_INVALID_TIMESTAMP) {}
  int init(const int64_t deadline);
  // check should_hurry_quit when start to perform a time-consuming operation
  bool should_hurry_quit() const;

  TO_STRING_KV(K(deadline_));
private:
  static const int64_t RESERVED_INTERVAL = 1 * 1000 * 1000; // 1 second
  int64_t deadline_;
};

} // namespace cd
} // namespace oceanbase

#endif

