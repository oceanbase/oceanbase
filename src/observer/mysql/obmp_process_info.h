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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_INFO_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_INFO_H_

#include "observer/mysql/obmp_query.h"

namespace oceanbase
{
namespace observer
{

class ObMPProcessInfo : public ObMPQuery
{
public:
  //static const obmysql::ObMySQLCmd COM = obmysql::COM_PROCESS_INFO;
  explicit ObMPProcessInfo(const ObGlobalContext &gctx);
  virtual ~ObMPProcessInfo();
protected:
  int deserialize() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPProcessInfo);
}; // end of class ObMPProcessInfo

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_INFO_H_
