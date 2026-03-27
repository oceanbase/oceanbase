/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_ERROR_HANDLER_H_
#define OCEANBASE_LOG_MINER_ERROR_HANDLER_H_

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerErrorHandler
{
public:
  virtual void handle_error(int err_code, const char *fmt, ...) = 0;
};

}
}

#endif