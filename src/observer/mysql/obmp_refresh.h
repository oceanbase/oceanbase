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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_REFRESH_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_REFRESH_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPRefresh : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_REFRESH;
  explicit ObMPRefresh(const ObGlobalContext &gctx);
  virtual ~ObMPRefresh();

protected:
  int process();
  int deserialize();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPRefresh);
}; // end of class ObMPRefresh

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_REFRESH_H_
