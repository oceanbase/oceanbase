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

#ifndef OCEANBASE_LOGSERVICE_OB_REPORTER_ADAPTER_H_
#define OCEANBASE_LOGSERVICE_OB_REPORTER_ADAPTER_H_

#include <stdint.h>
#include "observer/ob_service.h"
#include "logservice/palf/palf_callback.h"

namespace oceanbase
{
namespace logservice
{
class ObLogReporterAdapter
{
public:
  ObLogReporterAdapter();
  virtual ~ObLogReporterAdapter();
  int init(observer::ObIMetaReport *reporter);
  void destroy();
public:
  // report the replica info to RS.
  int report_replica_info(const int64_t palf_id);
private:
  bool is_inited_;
  observer::ObIMetaReport *rs_reporter_;
};

} // logservice
} // oceanbase

#endif
