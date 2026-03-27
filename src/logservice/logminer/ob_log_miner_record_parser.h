/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_RECORD_PARSER_H_
#define OCEANBASE_LOG_MINER_RECORD_PARSER_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerRecordParser {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void destroy() = 0;
  virtual int push(const common::ObString &record_str) = 0;
};

}
}

#endif