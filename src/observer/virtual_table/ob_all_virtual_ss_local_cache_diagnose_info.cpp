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

#include "ob_all_virtual_ss_local_cache_diagnose_info.h"
#include "share/ob_server_struct.h"
#include "share/ash/ob_di_util.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#endif

using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

ObAllVirtualSSLocalCacheDiagnoseInfo::ObAllVirtualSSLocalCacheDiagnoseInfo()
    : str_buf_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      tenant_di_info_(),
      cur_idx_(0)
{
  ip_buf_[0] = '\0';
#ifdef OB_BUILD_SHARED_STORAGE
  diag_info_list_.reset();
#endif
}

ObAllVirtualSSLocalCacheDiagnoseInfo::~ObAllVirtualSSLocalCacheDiagnoseInfo()
{
  reset();
}

void ObAllVirtualSSLocalCacheDiagnoseInfo::reset()
{
  ip_buf_[0] = '\0';
  str_buf_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_di_info_.reset();
  cur_idx_ = 0;
#ifdef OB_BUILD_SHARED_STORAGE
  diag_info_list_.reset();
#endif
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSSLocalCacheDiagnoseInfo::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}


int ObAllVirtualSSLocalCacheDiagnoseInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSSLocalCacheDiagnoseInfo::get_the_diag_info(
    const uint64_t tenant_id,
    common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  diag_info.reset();
  if (OB_FAIL(oceanbase::share::ObDiagnosticInfoUtil::get_the_diag_info(tenant_id, diag_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "Fail to get tenant stat event", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSLocalCacheDiagnoseInfo::get_diagnose_info_list_()
{
  int ret = OB_SUCCESS;
  diag_info_list_.reset();
  if (!GCTX.is_shared_storage_mode() || !oceanbase::lib::is_diagnose_info_enabled()) {
    // skip
  } else {
    ObSSLocalCacheService *local_cache_service = nullptr;
    if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "local_cache_service is null", KR(ret));
    } else {
      ObSSLocalCacheDiagnoseStat &diagnose_stat = local_cache_service->get_local_cache_diagnose_stat();
      if (OB_FAIL(diagnose_stat.get_diagnose_info_list(diag_info_list_))) {
        SERVER_LOG(WARN, "fail to get diagnose info list", KR(ret), K(diagnose_stat));
      }
    }
  }
  return ret;
}
#endif

int ObAllVirtualSSLocalCacheDiagnoseInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  ObAddr addr = GCTX.self_addr();
  const int64_t col_count = output_column_ids_.count();
  if (OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", KR(ret), K(cells));
  } else if (oceanbase::lib::is_diagnose_info_enabled()) {
    if (MTL_ID() != tenant_id_) {
      tenant_id_ = MTL_ID();
      if (OB_FAIL(get_the_diag_info(tenant_id_, tenant_di_info_))) {
        SERVER_LOG(WARN, "get diag info fail", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(get_diagnose_info_list_())) {
        SERVER_LOG(WARN, "get diag info list fail", KR(ret), K(tenant_id_));
      } else {
        cur_idx_ = 0;
      }
    }

    if (OB_SUCC(ret) && cur_idx_ >= diag_info_list_.count()) {
      ret = OB_ITER_END;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case SVR_IP: {
        if (addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret));
        }
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(addr.get_port());
        break;
      }
      case TENANT_ID: {
        cells[i].set_int(tenant_id_);
        break;
      }
      case MOD_NAME: {
        cells[i].set_varchar(diag_info_list_[cur_idx_].mod_name_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SUB_MOD_NAME: {
        cells[i].set_varchar(diag_info_list_[cur_idx_].sub_mod_name_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case STATUS: {
        cells[i].set_int(static_cast<uint8_t>(diag_info_list_[cur_idx_].status_));
        break;
      }
      case MODIFY_TIME: {
        cells[i].set_timestamp(diag_info_list_[cur_idx_].modify_time_);
        break;
      }
      case DIAGNOSE_INFO: {
        cells[i].set_varchar(diag_info_list_[cur_idx_].diagnose_info_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case EXTRA_INFO: {
        cells[i].set_varchar(diag_info_list_[cur_idx_].extra_info_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      } // end switch
    } // end for
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  } else {
    ret = OB_ITER_END;
  }
#endif
  return ret;
}

void ObAllVirtualSSLocalCacheDiagnoseInfo::release_last_tenant()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
}

bool ObAllVirtualSSLocalCacheDiagnoseInfo::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_;
}

} // namespace observer
} // namespace oceanbase