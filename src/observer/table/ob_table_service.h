/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_SERVICE_H
#define _OB_TABLE_SERVICE_H 1
#include "observer/ob_server_struct.h"
#include "ob_table_session_pool.h"
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
