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

#ifndef OCEANBASE_MYSQLCLIENT_OB_ISQL_RESULT_HANDLER_H_
#define OCEANBASE_MYSQLCLIENT_OB_ISQL_RESULT_HANDLER_H_

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

class ObMySQLResult;

class ObISQLResultHandler
{
public:
  ObISQLResultHandler() {}
  virtual ~ObISQLResultHandler() {}

  virtual ObMySQLResult *mysql_result() = 0;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase


#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_RESULT_HANDLER_H_
