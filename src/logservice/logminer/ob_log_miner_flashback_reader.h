/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_FLASHBACK_READER_H_
#define OCEANBASE_LOG_MINER_FLASHBACK_READER_H_

namespace oceanbase
{
namespace oblogminer
{
class ILogMinerFlashbackReader {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void destroy() = 0;
};
}
}

#endif