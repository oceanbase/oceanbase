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
#define USING_LOG_PREFIX SHARE
#include "ob_license_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{
using namespace common;

const char *CLEAR_LICENSE_TABLE_SQL = "delete from __all_license";

int ObLicenseUtils::check_dml_allowed()
{
  return OB_SUCCESS;
}

int ObLicenseUtils::get_login_message(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (buf_len > 0) {
    buf[0] = '\0';
  }

  return ret;
}
int ObLicenseUtils::check_add_server_allowed(int64_t add_num, obrpc::ObAdminServerArg::AdminServerOp arg)
{
  return OB_SUCCESS;
}

int ObLicenseUtils::check_olap_allowed(const int64_t tenant_id)
{
  return OB_SUCCESS;
}
int ObLicenseUtils::check_add_tenant_allowed(int user_tenant_num, const ObString &tenant_name)
{
  return OB_SUCCESS;
}

int ObLicenseUtils::check_standby_allowed()
{
  return OB_SUCCESS;
}

int ObLicenseUtils::check_for_create_tenant(int current_user_tenant_num, bool is_create_standby, const ObString &tenant_name) {
  return OB_SUCCESS;
}

void ObLicenseUtils::clear_license_table_if_need() {
  static bool inner_table_cleared = false;

  if (!inner_table_cleared) {
    ObMySQLProxy *proxy = GCTX.sql_proxy_;
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    int64_t affected_row = 0;

    if (OB_TMP_FAIL(proxy->write(OB_SYS_TENANT_ID, CLEAR_LICENSE_TABLE_SQL, affected_row))) {
      LOG_WARN("failed to clear inner table __all_license", KR(tmp_ret));
    } else {
      LOG_INFO("successfully clear inner table", KR(tmp_ret), K(CLEAR_LICENSE_TABLE_SQL), K(affected_row));
      inner_table_cleared = true;
    }
  }
}

int ObLicenseUtils::load_license(const ObString &file_path)
{
  int ret = OB_OP_NOT_ALLOW;
  LOG_WARN("load license is not allowed in current version");
  return ret;
}

int ObLicenseUtils::start_license_mgr()
{
  return OB_SUCCESS;
}


int ObLicenseUtils::check_create_table_allowed(uint64_t tenant_id)
{
  return OB_SUCCESS;
}

}
}