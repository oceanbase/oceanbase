/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
