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

#ifndef OCEANBASE_LOG_MINER_RECORD_FILTER_H_
#define OCEANBASE_LOG_MINER_RECORD_FILTER_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerRecord;

class ILogMinerRecordFilter {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void destroy() = 0;
  virtual int push(const ObLogMinerRecord &record) = 0;
};

}
}

#endif