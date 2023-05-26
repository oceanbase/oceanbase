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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_H_

#include <cstdint>
#include "common/ob_role.h"     // ObRole
#include "lib/utility/ob_macro_utils.h"
#include "lib/compress/ob_compress_util.h"
#include "share/restore/ob_log_restore_source.h"

namespace oceanbase
{

namespace logservice
{
class ObLogRestoreArchiveDriver;
class ObLogRestoreNetDriver;
class ObRemoteFetchLogImpl
{
  static const int64_t FETCH_LOG_AHEAD_THRESHOLD_US = 3 * 1000 * 1000L;  // 3s
public:
  ObRemoteFetchLogImpl();
  ~ObRemoteFetchLogImpl();

  int init(const uint64_t tenant_id,
      ObLogRestoreArchiveDriver *archive_driver,
      ObLogRestoreNetDriver *net_driver);
  void destroy();
  int do_schedule(const share::ObLogRestoreSourceItem &source);
  void clean_resource();
  void update_restore_upper_limit();
  void set_compressor_type(const common::ObCompressorType &compressor_type);

private:
  bool inited_;
  uint64_t tenant_id_;
  ObLogRestoreArchiveDriver *archive_driver_;
  ObLogRestoreNetDriver *net_driver_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteFetchLogImpl);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_H_ */
