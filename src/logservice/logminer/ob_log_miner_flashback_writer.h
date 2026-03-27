/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_FLASHBACK_WRITER_H_
#define OCEANBASE_LOG_MINER_FLASHBACK_WRITER_H_

#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerFlashbackWriter {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void destroy() = 0;
  virtual int push(common::ObSqlString *sql) = 0;
};

}
}

#endif