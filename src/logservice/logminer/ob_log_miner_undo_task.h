/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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