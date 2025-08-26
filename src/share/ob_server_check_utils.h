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

#ifndef OCEANBASE_ROOTSERVER_OB_SERVER_CHECK_UTILS_H
#define OCEANBASE_ROOTSERVER_OB_SERVER_CHECK_UTILS_H
#include "lib/net/ob_addr.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ls/ob_ls_info.h"//ObLSInfo

namespace oceanbase
{
namespace share
{
class ObServerCheckUtils
{
public:
  /**
   * @description:
   *    check if the given tenant has ls replicas on servers
   * @param[in] tenant_id the tenant which need to be checked
   * @param[in] servers on which the tenant might have ls replicas
   * @param[out] exists true if at least one of the servers has tenant ls replicas
   * @return return code
   */
  static int check_if_tenant_ls_replicas_exist_in_servers(
    const uint64_t tenant_id,
    const common::ObArray<common::ObAddr> &servers,
    bool &exist);
  static int check_server_empty_by_ls(
      const share::ObLSInfo &ls_info,
      common::ObArray<common::ObAddr> &empty_servers);
  static int check_tenant_server_online(const uint64_t tenant_id, const char * const op_str);
  static int get_tenants_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &tenant_servers);
  // 1. target_servers does not include parmanent offline servers
  // 2. if include_temp_offline is false, but there exists temporary offline servers
  //    an error will be returned
  static int check_offline_and_get_tenants_servers(
      const uint64_t tenant_id,
      const bool include_temp_offline,
      common::ObIArray<common::ObAddr> &target_servers,
      const char * const op_str = NULL);
};
}
}

#endif /* !OCEANBASE_ROOTSERVER_OB_SERVER_CHECK_UTILS_H */