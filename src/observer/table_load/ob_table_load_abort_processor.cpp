// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <jianming.cjq@alipay.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_abort_processor.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

/**
 * ObTableLoadAbortP
 */

int ObTableLoadAbortP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadKey key(credential_.tenant_id_, arg_.table_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        LOG_WARN("fail to get table ctx", KR(ret), K(key));
      }
    } else {
      if (OB_FAIL(ObTableLoadCoordinator::abort_ctx(table_ctx))) {
        LOG_WARN("fail to abort coordinator ctx", KR(ret));
      } else if (OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
        LOG_WARN("fail to remove table ctx", KR(ret), K(key));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadAbortP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadAbortPeerP
 */

int ObTableLoadAbortPeerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadKey key(credential_.tenant_id_, arg_.table_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        LOG_WARN("fail to get table ctx", KR(ret), K(key));
      }
    } else {
      if (OB_FAIL(ObTableLoadStore::abort_ctx(table_ctx))) {
        LOG_WARN("fail to abort store ctx", KR(ret));
      } else if (OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
        LOG_WARN("fail to remove table ctx", KR(ret), K(key));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadAbortPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

} // namespace observer
} // namespace oceanbase
