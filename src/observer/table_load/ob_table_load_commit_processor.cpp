// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_commit_processor.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace observer
{
using namespace table;
using namespace sql;

/**
 * ObTableLoadCommitP
 */

int ObTableLoadCommitP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadCoordinator coordinator(table_ctx);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (OB_FAIL(coordinator.commit(result_.result_info_))) {
        LOG_WARN("fail to coordinator commit", KR(ret));
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

int ObTableLoadCommitP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadCommitPeerP
 */

int ObTableLoadCommitPeerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.commit(result_.result_info_, result_.sql_statistics_))) {
        LOG_WARN("fail to store commit", KR(ret));
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

int ObTableLoadCommitPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

} // namespace observer
} // namespace oceanbase
