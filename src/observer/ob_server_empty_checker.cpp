/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/ob_server_empty_checker.h"
#include "observer/ob_server.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "common/ob_tenant_data_version_mgr.h"

namespace oceanbase
{
namespace observer
{

ERRSIM_POINT_DEF(ERRSIM_CHECK_SERVER_EMPTY_HIDE_SERVER_ID);
ERRSIM_POINT_DEF(ERRSIM_CHECK_SERVER_EMPTY_FAKE_DATA_VERSION);

int ObServerEmptyChecker::check_server_empty(bool &is_empty)
{
  ObServerEmptyCheckInfo result;
  return check_server_empty(is_empty, result);
}

int ObServerEmptyChecker::check_server_empty(bool &is_empty, ObServerEmptyCheckInfo &result)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  result.reset();

  const uint64_t server_id = ERRSIM_CHECK_SERVER_EMPTY_HIDE_SERVER_ID
                             ? OB_INVALID_ID : GCTX.get_server_id();
  const uint64_t server_id_in_gconf = ERRSIM_CHECK_SERVER_EMPTY_HIDE_SERVER_ID
                                      ? OB_INVALID_ID : GCONF.observer_id;
  bool has_log_dir = !OBSERVER.is_log_dir_empty();
  bool has_wallet = false;
#ifdef OB_BUILD_TDE_SECURITY
  has_wallet = ObMasterKeyGetter::instance().is_wallet_exist();
#endif
  bool has_data_version_file = ODV_MGR.get_file_exists_when_loading()
                               || ERRSIM_CHECK_SERVER_EMPTY_FAKE_DATA_VERSION;

  result.init(server_id, server_id_in_gconf, has_log_dir, has_wallet, has_data_version_file);
  is_empty = result.is_empty();

  if (result.has_server_id()) {
    FLOG_WARN("[CHECK_SERVER_EMPTY] server_id exists", "server_id", result.get_server_id());
  }
  if (result.has_log_dir()) {
    FLOG_WARN("[CHECK_SERVER_EMPTY] log dir is not empty");
  }
  if (result.has_wallet()) {
    FLOG_WARN("[CHECK_SERVER_EMPTY] master_key file exists");
  }
  if (result.has_data_version_file()) {
    FLOG_WARN("[CHECK_SERVER_EMPTY] data_version file exists");
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
