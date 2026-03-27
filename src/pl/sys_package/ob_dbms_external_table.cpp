/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL
#include "ob_dbms_external_table.h"
#include "share/external_table/ob_external_table_file_mgr.h"

namespace oceanbase
{
using namespace sql;
using namespace common;

namespace pl
{


int ObDBMSExternalTable::auto_refresh_external_table(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  int32_t interval = 0;
  if (params.count() == 0) {
    interval = 0;
  } else {
    const ObObjParam &param0 = params.at(0);
    if (param0.is_null()) {
      //do nothing
    } else if (OB_FAIL(param0.get_int32(interval))) {
      LOG_WARN("failed to get number", K(ret), K(param0));
    }
  }

  if (OB_SUCC(ret)) {
    OZ (ObExternalTableFileManager::get_instance().auto_refresh_external_table(exec_ctx, interval));
  }
  return ret;
}

} // end of pl
} // end oceanbase
