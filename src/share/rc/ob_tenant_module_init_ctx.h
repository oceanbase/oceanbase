/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_TENANT_MODULE_INIT_CTX_H_
#define OCEANBASE_SHARE_OB_TENANT_MODULE_INIT_CTX_H_

#include "lib/ob_define.h"
#include "logservice/palf/palf_options.h"
namespace oceanbase
{

namespace share
{
class DagSchedulerConfig;

// 租户模块初始化参数
class ObTenantModuleInitCtx
{
public:
  ObTenantModuleInitCtx() : palf_options_()
  {}

  // for logservice
  palf::PalfOptions palf_options_;
  char tenant_clog_dir_[common::MAX_PATH_SIZE] = {'\0'};

#ifdef OB_BUILD_SHARED_STORAGE
  // for SS mode ObTenantDiskSpaceManager
  int64_t init_data_disk_size_ = 0;
#endif

  // TODO init DagSchedulerConfig, which will be used to config the params in ObTenantDagScheduler
  // share::DagSchedulerConfig *scheduler_config_;
};


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_TENANT_MODULE_INIT_CTX_H_
