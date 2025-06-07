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

#include "ob_all_virtual_logservice_cluster_info.h"
#include "share/config/ob_server_config.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "close_modules/shared_log_service/logservice/libpalf/libpalf_env_ffi_instance.h"
#include "close_modules/shared_log_service/logservice/libpalf/libpalf_common_define.h"
#endif

namespace oceanbase
{
namespace observer
{
using namespace libpalf;
int ObAllVirtualLogServiceClusterInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_LOG_SERVICE
  ret = OB_ITER_END;
#else
  if (!GCONF.enable_logservice) {
    ret = OB_ITER_END;
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(fill_scanner_())) {
        SERVER_LOG(WARN, "fill scanner failed", K(ret));
      } else {
        start_to_read_ = true;
      }
    }

    if (OB_SUCC(ret) && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
#endif
  return ret;
}

int ObAllVirtualLogServiceClusterInfo::fill_scanner_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(fill_row_())) {
    SERVER_LOG(WARN, "fill_row for logservice_cluster_info failed", K(ret));
  } else {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }

  return ret;
}

int ObAllVirtualLogServiceClusterInfo::fill_row_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  const int64_t count = output_column_ids_.count();
  const LibPalfEnvFFI *ffi = nullptr;

  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(ffi))) {
    PALF_LOG(WARN, "fail to get ffi", KR(ret));
  } else if (OB_ISNULL(ffi)) {
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "fail to get ffi", KR(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_lm_info(
      ffi,
      common::OB_IP_PORT_STR_BUFF,
      &logservice_cluster_id_,
      cluster_version_,
      lm_rpc_addr_,
      lm_http_addr_)))) {
    PALF_LOG(WARN, "fail to get cluster info", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case CLUSTER_ID: {
          cur_row_.cells_[i].set_uint64(logservice_cluster_id_);
          break;
        }
        case CLUSTER_VERSION: {
          cur_row_.cells_[i].set_varchar(ObString::make_string(cluster_version_));
          break;
        }
        case LM_RPC_ADDR: {
          cur_row_.cells_[i].set_varchar(ObString::make_string(lm_rpc_addr_));
          break;
        }
        case LM_HTTP_ADDR: {
          cur_row_.cells_[i].set_varchar(ObString::make_string(lm_http_addr_));
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          PALF_LOG(WARN, "invalid column id", K(ret), K(col_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    scanner_.add_row(cur_row_);
  }

#endif
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
