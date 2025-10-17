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

#ifndef OCEANBASE_SHARE_OB_LICENSE_UTILS_H
#define OCEANBASE_SHARE_OB_LICENSE_UTILS_H

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace share
{
class ObLicenseUtils
{
public:
  static int load_license(const ObString &file_path);
  static int get_login_message(char *buf, int64_t buf_len);
  static int check_for_create_tenant(int current_user_tenant_num, bool is_create_standby, const ObString &tenant_name);
  static void clear_license_table_if_need();
  static int start_license_mgr();
  static int check_dml_allowed();
  static int check_add_server_allowed(int64_t add_num, obrpc::ObAdminServerArg::AdminServerOp arg = obrpc::ObAdminServerArg::ADD);
  static int check_standby_allowed();
  static int check_olap_allowed(const int64_t tenant_id);
  static int check_add_tenant_allowed(int current_user_tenant_num, const ObString &tenant_name);
  static int check_create_table_allowed(uint64_t tenant_id);
};

} // namespace share
} // namespace oceanbase

#endif