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

#include <cstdint>
#include "lib/time/ob_time_utility.h"
#include "ob_ls_meta_record_task.h"
#include "observer/ob_server_struct.h"
#include "logservice/data_dictionary/ob_data_dict_meta_info.h"

namespace oceanbase
{
namespace archive
{
// ================== ObArchiveLSSchemaMeta ================== //
int64_t ObArchiveLSSchemaMeta::get_record_interval()
{
  // 30 min
  static const int64_t LS_SCHEMA_RECORD_INTERVAL = 30L * 60 * 1000 * 1000 * 1000;
  return LS_SCHEMA_RECORD_INTERVAL;
}

int ObArchiveLSSchemaMeta::get_ls_array(common::ObIArray<share::ObLSID> &array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(array.push_back(share::ObLSID(share::ObLSID::SYS_LS_ID)))) {
    ARCHIVE_LOG(WARN, "get ks array failed");
  }
  return ret;
}

int ObArchiveLSSchemaMeta::get_data(const share::ObLSID &id,
    const share::SCN &base_scn,
    char *data,
    const int64_t data_size,
    int64_t &real_size,
    share::SCN &scn)
{
  int ret = OB_SUCCESS;
  UNUSED(id);
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "sql proxy is null, unexpected", KR(ret), K(sql_proxy), K(tenant_id));
  } else {
    datadict::MetaInfoQueryHelper helper(*sql_proxy, tenant_id);
    if (OB_FAIL(helper.get_data(base_scn, data, data_size, real_size, scn))) {
      ARCHIVE_LOG(WARN, "MetaInfoQueryHelper get data failed", KR(ret), K(tenant_id), K(base_scn), K(real_size));
    }
  }
  return ret;
}
} // namespace archive
} // namespace oceanbase
