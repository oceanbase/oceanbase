/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_UNDO_TASK_H_
#define OCEANBASE_LOG_UNDO_TASK_H_

#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerRecord;

class ObLogMinerUndoTask {
public:
  ObLogMinerUndoTask();
  int init(ObLogMinerRecord *logminer_rec);

private:
  bool                is_filtered_;
  int64_t             stmt_timestamp_;
  common::ObSqlString *undo_stmt_;
};
}
}

#endif