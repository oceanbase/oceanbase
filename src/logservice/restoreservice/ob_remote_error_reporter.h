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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_ERROR_REPORTER_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_ERROR_REPORTER_H_

#include <cstdint>
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"
namespace oceanbase
{
namespace storage
{
class ObLSService;
class ObLS;
}
namespace logservice
{
class ObLogService;
class ObRemoteErrorReporter
{
public:
  ObRemoteErrorReporter();
  virtual ~ObRemoteErrorReporter();
  int init(const uint64_t tenant_id, storage::ObLSService *ls_svr);
  void destroy();
  int report_error();

private:
  int do_report_(storage::ObLS &ls);
  int report_restore_error_(storage::ObLS &ls, share::ObTaskId &trace_id, const int ret_code);
  int report_standby_error_(storage::ObLS &ls, share::ObTaskId &trace_id, const int ret_code);
private:
  static const int64_t CHECK_ERROR_INTERVAL = 5 * 1000 * 1000L;
private:
  bool inited_;
  uint64_t tenant_id_;
  int64_t last_check_ts_;
  storage::ObLSService *ls_svr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteErrorReporter);
};
} // namespace logservice
} // namespace oceanbase

#endif
