/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_SERVICE_H
#define _OB_TABLE_SERVICE_H 1
#include "observer/ob_server_struct.h"
#include "object_pool/ob_table_object_pool.h"
namespace oceanbase
{
namespace observer
{
/// table service
class ObTableService
{
public:
  ObTableService() {}
  virtual ~ObTableService() = default;
  int init();
  void stop();
  void wait();
  void destroy();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableService);
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_SERVICE_H */
