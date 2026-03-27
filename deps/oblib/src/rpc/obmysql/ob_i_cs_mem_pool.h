/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBMYSQL_OB_I_CS_MEM_POOL_H_
#define OCEANBASE_OBMYSQL_OB_I_CS_MEM_POOL_H_
#include <stdint.h>

namespace oceanbase
{
namespace obmysql
{
class ObICSMemPool
{
public:
  ObICSMemPool() {}
  virtual ~ObICSMemPool() {}
  virtual void* alloc(int64_t sz) = 0;
};

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_I_CS_MEM_POOL_H_ */

