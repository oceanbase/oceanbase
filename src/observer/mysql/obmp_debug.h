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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_DEBUG_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_DEBUG_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPDebug : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_DEBUG;
  explicit ObMPDebug(const ObGlobalContext &gctx);
  virtual ~ObMPDebug();

protected:
  int process();
  int deserialize();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPDebug);
}; // end of class ObMPDebug

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_DEBUG_H_
