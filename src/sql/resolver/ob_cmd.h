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

#ifndef OCEANBASE_SQL_RESOLVER_OB_CMD_
#define OCEANBASE_SQL_RESOLVER_OB_CMD_

namespace oceanbase
{
namespace sql
{

class ObICmd
{
public:
  virtual int get_cmd_type() const = 0;
  virtual bool cause_implicit_commit() const { return false; }
};

}
}
#endif /* OCEANBASE_SQL_RESOLVER_OB_CMD_ */
//// end of header file

