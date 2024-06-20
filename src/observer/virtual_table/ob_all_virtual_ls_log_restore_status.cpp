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

#include "ob_all_virtual_ls_log_restore_status.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h"   // ObLSService
#include "rootserver/ob_recovery_ls_service.h" //ObLSRecoveryService

using namespace oceanbase::share;

namespace oceanbase
{
namespace observer
{
ObVirtualLSLogRestoreStatus::ObVirtualLSLogRestoreStatus() :
  is_inited_(false)
{}

ObVirtualLSLogRestoreStatus::~ObVirtualLSLogRestoreStatus()
{
  destroy();
}

int ObVirtualLSLogRestoreStatus::init(omt::ObMultiTenant *omt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(omt)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid arguments, omt is null", K(ret), K(omt));
  } else {
    omt_ = omt;
    is_inited_ = true;
  }
  return ret;
}

int ObVirtualLSLogRestoreStatus::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;

  if (false == start_to_read_) {
    auto func_iterate_tenant = [&]() -> int
    {
      int ret = OB_SUCCESS;
      ObLSService *ls_svr = MTL(ObLSService*);
      share::ObTenantRole::Role tenant_role = MTL_GET_TENANT_ROLE_CACHE();
      if (! is_restore_tenant(tenant_role) && ! is_standby_tenant(tenant_role)) {
        SERVER_LOG(TRACE, "not restore or standby tenant", K(tenant_role), K(MTL_ID()));
      } else if (is_user_tenant(MTL_ID())) {
        if (OB_ISNULL(ls_svr)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "mtl ObLSService should not be null", K(ret));
        } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
          SERVER_LOG(WARN, "get ls iter failed", K(ret));
        } else if (OB_ISNULL(iter = guard.get_ptr())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ls iter is NULL", K(ret), K(iter));
        } else {
          while (OB_SUCC(ret)) {
            ObLS *ls = NULL;
            logservice::ObLogRestoreHandler *restore_handler;
            if (OB_FAIL(iter->get_next(ls))) {
              if (OB_ITER_END != ret) {
                SERVER_LOG(WARN, "iter ls get next failed", K(ret));
              } else {
                SERVER_LOG(WARN, "iter to end", K(ret));
              }
            } else if (OB_ISNULL(ls)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "ls is NULL", K(ret), K(ls));
            } else {
              SERVER_LOG(TRACE, "start to iterate this log_stream", K(MTL_ID()), K(ls->get_ls_id()));
              logservice::RestoreStatusInfo restore_status_info;
              logservice::RestoreStatusInfo sys_restore_status_info;
              restore_handler = ls->get_log_restore_handler();
              if (OB_ISNULL(restore_handler)) {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "restore handler is NULL", K(ret), K(ls));
              } else if (OB_FAIL(restore_handler->get_ls_restore_status_info(restore_status_info))) {
                SERVER_LOG(WARN, "fail to get ls restore status info");
                ret = OB_SUCCESS;
              } else if (!restore_status_info.is_valid()) {
                SERVER_LOG(WARN, "restore status info is invalid", K(restore_status_info));
              } else if (!ls->is_sys_ls()) {  // not sys ls
                if (OB_FAIL(insert_ls_restore_status_info_(restore_status_info))) {
                  SERVER_LOG(WARN, "fail to insert ls restore status info", K(restore_status_info));
                } else {
                  SERVER_LOG(TRACE, "iterate user log_stream success", K(ls));
                  scanner_.add_row(cur_row_);
                }
              } else {  // sys ls
                rootserver::ObRecoveryLSService *ls_recovery_svr = MTL(rootserver::ObRecoveryLSService*);
                if (OB_ISNULL(ls_recovery_svr)) {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "ls recovery service is NULL", K(ret), K(ls));
                } else if (OB_FAIL(ls_recovery_svr->get_sys_restore_status(sys_restore_status_info))) {
                  SERVER_LOG(WARN, "get sys restore status failed", K(ls));
                  // use restore_status_info if get sys ls restore status failed
                  if (OB_FAIL(insert_ls_restore_status_info_(restore_status_info))) {
                    SERVER_LOG(WARN, "fail to insert ls restore status info", K(restore_status_info));
                  } else {
                    SERVER_LOG(TRACE, "insert ls restore status info success after get sys restore status failed", K(ls));
                    scanner_.add_row(cur_row_);
                  }
                } else if (sys_restore_status_info.is_valid()
                            && OB_FAIL(insert_ls_restore_status_info_(sys_restore_status_info))) {
                  SERVER_LOG(WARN, "fail to insert ls restore status info", K(sys_restore_status_info));
                } else if (!sys_restore_status_info.is_valid()
                            && OB_FAIL(insert_ls_restore_status_info_(restore_status_info))) {
                  SERVER_LOG(WARN, "fail to insert ls restore status info", K(restore_status_info));
                } else {
                  SERVER_LOG(TRACE, "iterate sys log_stream success", K(ls->get_ls_id()));
                  scanner_.add_row(cur_row_);
                }
              }
            }
          } // while
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
      return ret;
    };
    if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
      SERVER_LOG(WARN, "ObMultiTenant operate_each_tenant_for_sys_or_self failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }// start to read
  if (OB_SUCC(ret) && true == start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", KR(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

void ObVirtualLSLogRestoreStatus::destroy()
{
  is_inited_ = false;
  omt_ = NULL;
}

int ObVirtualLSLogRestoreStatus::insert_ls_restore_status_info_(logservice::RestoreStatusInfo &restore_status_info)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {
        if (false == GCTX.self_addr().ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
          break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {
        cur_row_.cells_[i].set_int(restore_status_info.ls_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {
        cur_row_.cells_[i].set_uint64(restore_status_info.sync_lsn_ < 0 ? 0 : restore_status_info.sync_lsn_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {
        cur_row_.cells_[i].set_uint64((restore_status_info.sync_scn_.get_val_for_inner_table_field()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {
        if (OB_FAIL(restore_status_info.restore_sync_status_to_string(restore_status_str_, sizeof(restore_status_str_)))) {
          SERVER_LOG(WARN, "restore_sync_status to string failed", K(restore_status_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(restore_status_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {
        cur_row_.cells_[i].set_int(restore_status_info.err_code_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {
        if (OB_FAIL(databuff_printf(comment_, sizeof(comment_), "%.*s",
            static_cast<int>(restore_status_info.comment_.length()), restore_status_info.comment_.ptr()))) {
          SERVER_LOG(WARN, "restore_sync_status comment to string failed", K(restore_status_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(comment_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
    }
  }
  SERVER_LOG(TRACE, "insert restore status info success", K(restore_status_info));
  return ret;
}
} // namespace observer
} // namespace oceanbase