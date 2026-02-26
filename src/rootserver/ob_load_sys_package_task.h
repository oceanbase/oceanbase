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
#ifndef _OCEANBASE_ROOTSERVER_OB_LOAD_SYS_PACKAGE_TASK_H_
#define _OCEANBASE_ROOTSERVER_OB_LOAD_SYS_PACKAGE_TASK_H_ 1

#include "deps/oblib/src/lib/thread/ob_work_queue.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"
#include "deps/oblib/src/common/ob_timeout_ctx.h"
#include "src/share/ob_define.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;
class ObLoadSysPackageTask : public common::ObAsyncTimerTask
{
public:
  explicit ObLoadSysPackageTask(ObRootService &root_service, int64_t fail_count = 0);
  virtual ~ObLoadSysPackageTask() {}
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  virtual int process();
  static int wait_sys_package_ready(
      common::ObMySQLProxy &sql_proxy,
      const common::ObTimeoutCtx &ctx,
      ObCompatibilityMode mode);
private:
  int load_package(const ObCompatibilityMode &compat_mode);
private:
  ObRootService &root_service_;
  int64_t fail_count_;
};
}
}

#endif // _OCEANBASE_ROOTSERVER_OB_LOAD_SYS_PACKAGE_TASK_H_
