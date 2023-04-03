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

#ifndef OCEANBASE_SHARE_OB_DISK_USAGE_TABLE_OPERATOR_
#define OCEANBASE_SHARE_OB_DISK_USAGE_TABLE_OPERATOR_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

namespace storage
{
enum class ObDiskReportFileType : uint8_t
{
  OB_DISK_REPORT_TENANT_DATA = 0,
  OB_DISK_REPORT_TENANT_META_DATA = 1,
  OB_DISK_REPORT_TENANT_INDEX_DATA = 2,
  OB_DISK_REPORT_TENANT_TMP_DATA = 3,
  OB_DISK_REPORT_TENANT_SLOG_DATA = 4,
  OB_DISK_REPORT_TENANT_CLOG_DATA = 5,
  OB_DISK_REPORT_TYPE_NUM
};
struct ObTenantDiskUsage;
}

namespace share
{

class ObDiskUsageTableOperator
{
public:
  ObDiskUsageTableOperator();
  ~ObDiskUsageTableOperator();
public:
  int init(common::ObMySQLProxy &proxy);
  int update_tenant_space_usage(const uint64_t tenant_id,
                                const char *svr_ip,
                                const int32_t svr_port,
                                const int64_t seq_num,
                                const enum storage::ObDiskReportFileType file_type,
                                const uint64_t data_size,
                                const uint64_t used_size);
  int delete_tenant_space_usage(const uint64_t tenant_id,
                                const char *svr_ip,
                                const int32_t svr_port,
                                const int64_t seq_num,
                                const storage::ObDiskReportFileType file_type);
  int delete_tenant_all(const uint64_t tenant_id,
                        const char *svr_ip,
                        const int32_t svr_port,
                        const int64_t seq_num);
  int delete_tenant_all(const uint64_t tenant_id);

  int get_all_tenant_ids(const char *svr_ip,
                         const int32_t svr_port,
                         const int64_t seq_num,
                         common::ObIArray<uint64_t> &tenant_ids);
private:
  bool is_inited_;
  common::ObMySQLProxy *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDiskUsageTableOperator);
};
} // namespace share
} // namespace oceanbase



# endif // OCEANBASE_SHARE_OB_DISK_USAGE_TABLE_OPERATOR_
