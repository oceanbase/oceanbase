// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_trans_processor.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_service.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

/**
 * ObTableLoadStartTransP
 */

int ObTableLoadStartTransP::process()
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
      } else if (OB_FAIL(coordinator.start_trans(arg_.segment_id_, result_.trans_id_))) {
        LOG_WARN("fail to coordinator start trans", KR(ret));
      } else if (OB_FAIL(coordinator.get_trans_status(result_.trans_id_, result_.trans_status_,
                                                      result_.error_code_))) {
        LOG_WARN("fail to coordinator get trans status", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadStartTransP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadPreStartTransPeerP
 */

int ObTableLoadPreStartTransPeerP::process()
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
      } else if (OB_FAIL(store.pre_start_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store pre start trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadPreStartTransPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadConfirmStartTransPeerP
 */

int ObTableLoadConfirmStartTransPeerP::process()
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
      } else if (OB_FAIL(store.confirm_start_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store confirm start trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadConfirmStartTransPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

//////////////////////////////////////////////////////////////////////

/**
 * ObTableLoadFinishTransP
 */

int ObTableLoadFinishTransP::process()
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
      } else if (OB_FAIL(coordinator.finish_trans(arg_.trans_id_))) {
        LOG_WARN("fail to coordinator finish trans", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadFinishTransP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadPreFinishTransPeerP
 */

int ObTableLoadPreFinishTransPeerP::process()
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
      } else if (OB_FAIL(store.pre_finish_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store pre finish trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadPreFinishTransPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadConfirmFinishTransPeerP
 */

int ObTableLoadConfirmFinishTransPeerP::process()
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
      } else if (OB_FAIL(store.confirm_finish_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store confirm finish trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadConfirmFinishTransPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

//////////////////////////////////////////////////////////////////////

/**
 * ObTableLoadAbandonTransP
 */

int ObTableLoadAbandonTransP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadCoordinator coordinator(table_ctx);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (OB_FAIL(coordinator.abandon_trans(arg_.trans_id_))) {
        LOG_WARN("fail to coordinator abandon trans", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadAbandonTransP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadAbandonTransPeerP
 */

int ObTableLoadAbandonTransPeerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.abandon_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store abandon trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadAbandonTransPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

//////////////////////////////////////////////////////////////////////

/**
 * ObTableLoadGetTransStatusP
 */

int ObTableLoadGetTransStatusP::process()
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
      } else if (OB_FAIL(coordinator.get_trans_status(arg_.trans_id_, result_.trans_status_,
                                                      result_.error_code_))) {
        LOG_WARN("fail to coordinator get trans status", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadGetTransStatusP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}


/**
 * ObTableLoadGetTransStatusPeerP
 */

int ObTableLoadGetTransStatusPeerP::process()
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
      } else if (OB_FAIL(store.get_trans_status(arg_.trans_id_, result_.trans_status_,
                                                result_.error_code_))) {
        LOG_WARN("fail to store get trans status", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadGetTransStatusPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

}  // namespace observer
}  // namespace oceanbase
