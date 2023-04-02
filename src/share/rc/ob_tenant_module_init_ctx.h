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

  // TODO init DagSchedulerConfig, which will be used to config the params in ObTenantDagScheduler
  // share::DagSchedulerConfig *scheduler_config_;
};


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_TENANT_MODULE_INIT_CTX_H_
