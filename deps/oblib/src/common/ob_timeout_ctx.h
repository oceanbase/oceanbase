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

#ifndef OCEANBASE_COMMON_OB_TIMEOUT_CTX_H_
#define OCEANBASE_COMMON_OB_TIMEOUT_CTX_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
// ObTimeoutCtx has two usage:
// 1. user set abs_timeout_us, and modify query_timeout and trx_timeout of current thread inner_sql;
// To single statement transaction.
// 2. user set abs_timeout_us to control query_timeout, and set trx_timeout_us to control tx_timeout;
// To multi statement transaction.
// Pass timeout argument by thread local variable,
// to avoid add timeout argument to all functions.
class ObTimeoutCtx
{
public:
  ObTimeoutCtx();
  virtual ~ObTimeoutCtx();

  int set_timeout(const int64_t timeout_interval_us);
  int set_abs_timeout(const int64_t abs_timeout_us);

  // return timeout interval, return %def_timeout_interval_us if timeout not set
  int64_t get_timeout(const int64_t def_timeout_interval_us = -1) const;
  // return absolute timeout timestamp, return %def_abs_timeout_us if timeout not set
  int64_t get_abs_timeout(const int64_t def_abs_timeout_us = -1) const;

  bool is_timeout_set() const { return abs_timeout_us_ > 0; }
  bool is_timeouted() const;

  int set_trx_timeout_us(int64_t trx_timeout_us);
  bool is_trx_timeout_set() const;
  int64_t get_trx_timeout_us() const;
  // return previous timeout context or an initialized one if no previous context.
  static const ObTimeoutCtx &get_ctx();

  TO_STRING_KV(K_(abs_timeout_us));

private:
  explicit ObTimeoutCtx(bool link_self);
  void do_link_self();

private:
  static ObTimeoutCtx *&header();

private:
  int64_t abs_timeout_us_;  //query_timeout
  int64_t trx_timeout_us_; //trx_timeout
  ObTimeoutCtx *next_;

  DISALLOW_COPY_AND_ASSIGN(ObTimeoutCtx);
};

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_COMMON_OB_TIMEOUT_CTX_H_
