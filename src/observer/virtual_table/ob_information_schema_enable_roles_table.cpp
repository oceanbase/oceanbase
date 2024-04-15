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

#define USING_LOG_PREFIX SERVER
#include "ob_information_schema_enable_roles_table.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {
ObInfoSchemaEnableRolesTable::ObInfoSchemaEnableRolesTable()
{
}

ObInfoSchemaEnableRolesTable::~ObInfoSchemaEnableRolesTable()
{
  reset();
}

void ObInfoSchemaEnableRolesTable::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaEnableRolesTable::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(prepare_scan())) {
      LOG_WARN("fail to prepare scan", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}
int ObInfoSchemaEnableRolesTable::prepare_scan()
{
  int ret = OB_SUCCESS;
  enum {
    ROLE_NAME = OB_APP_MIN_COLUMN_ID,
    ROLE_HOST,
    IS_DEFAULT,
    IS_MANDATORY,
    MAX_COL_ID
  };

  ObObj *cells = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const int64_t col_count = output_column_ids_.count();


  if (0 > col_count || col_count > (MAX_COL_ID - OB_APP_MIN_COLUMN_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count error ", K(ret), K(col_count));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  } else if (col_count > cur_row_.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column num not match", K(ret), K(cur_row_), K(output_column_ids_));
  } else if (OB_ISNULL(session_) || OB_ISNULL(schema_guard = get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < session_->get_enable_role_array().count(); ++i) {
      const ObUserInfo *user_info = NULL;
      const uint64_t role_user_id = session_->get_enable_role_array().at(i);
      const ObUserInfo *cur_user_info = NULL;
      const uint64_t cur_user_id = session_->get_priv_user_id();
      const uint64_t tenant_id = session_->get_effective_tenant_id();
      if (OB_FAIL(schema_guard->get_user_info(tenant_id, role_user_id, user_info))) {
        LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(role_user_id));
      } else if (OB_FAIL(schema_guard->get_user_info(tenant_id, cur_user_id, cur_user_info))) {
        LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(role_user_id));
      } else if (OB_ISNULL(user_info) || OB_ISNULL(cur_user_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user info is null", K(ret), KP(user_info), KP(cur_user_info),
                 K(tenant_id), K(cur_user_id), K(role_user_id));
      }

      for (int j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
        switch (col_id){
          case ROLE_NAME:{
            cells[j].set_varchar(user_info->get_user_name_str());
            cells[j].set_collation_type(ObCharset::get_system_collation());
            break;
          }
          case ROLE_HOST: {
            cells[j].set_varchar(user_info->get_host_name_str());
            cells[j].set_collation_type(ObCharset::get_system_collation());
            break;
          }
          case IS_DEFAULT: {
            int64_t idx = 0;
            bool is_default_role = false;
            if (has_exist_in_array(cur_user_info->get_role_id_array(), role_user_id, &idx)
                && cur_user_info->get_disable_option(
                             cur_user_info->get_role_id_option_array().at(idx)) == 0) {
              is_default_role = true;
            }
            cells[j].set_varchar(is_default_role ? "YES" : "NO");
            cells[j].set_collation_type(ObCharset::get_system_collation());
            break;
          }
          case IS_MANDATORY: {
            cells[j].set_varchar("NO");
            cells[j].set_collation_type(ObCharset::get_system_collation());
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column", K(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("fail to add row", K(ret), K(cur_row_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
