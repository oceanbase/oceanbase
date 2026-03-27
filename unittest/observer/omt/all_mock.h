/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _ALL_MOCK_H_
#define _ALL_MOCK_H_

#ifndef private
#define private public
#endif
#ifndef protected
#define protected public
#endif

#include "observer/ob_server.h"

using namespace oceanbase::storage;

void all_mock_init()
{
  static bool inited = false;
  if (!inited) {
    OBSERVER.init_global_context();
    OBSERVER.init_schema();
    OBSERVER.init_tz_info_mgr();
    GCTX.sql_engine_->plan_cache_manager_.init(GCTX.self_addr());
    GCTX.sql_engine_->plan_cache_manager_.inited_ = false;
    inited = true;
  }
}

#endif /* _ALL_MOCK_H_ */
