/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

