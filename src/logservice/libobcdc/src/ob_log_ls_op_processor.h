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

#ifndef OCEANBASE_LIBOBCDC_LOG_OP_PROCESSOR_H_
#define OCEANBASE_LIBOBCDC_LOG_OP_PROCESSOR_H_

#include "share/ls/ob_ls_operator.h"          // ObLSAttr
#include "logservice/palf/lsn.h"              // LSN
#include "ob_log_tenant.h"                    // ObLogTenant

namespace oceanbase
{
namespace libobcdc
{
class ObLogLSOpProcessor
{
public:
  ObLogLSOpProcessor() : inited_(false) {}
  virtual ~ObLogLSOpProcessor() { inited_ = false; }
  int init();
  void destroy();

  static int process_ls_op(
      const uint64_t tenant_id,
      const palf::LSN &lsn,
      const int64_t start_tstamp_ns,
      const share::ObLSAttr &ls_attr);

private:
  static int create_new_ls_(
      ObLogTenant *tenant,
      const int64_t start_tstamp_ns,
      const share::ObLSAttr &ls_attr);

private:
  bool inited_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
