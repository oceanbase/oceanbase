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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_DROP_EVENT_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_DROP_EVENT_STMT_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "lib/string/ob_strings.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObDropEventStmt: public ObCMDStmt
{
public:
  explicit ObDropEventStmt(common::ObIAllocator *name_pool);
  ObDropEventStmt();
  virtual ~ObDropEventStmt() = default;

  inline void set_if_exists(const bool if_exists) { if_exists_ = if_exists; }
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_event_name(const common::ObString &event_name) { event_name_ = event_name; }
  inline void set_event_database(const common::ObString &event_database) { event_database_ = event_database; }

  uint64_t get_tenant_id()                          { return tenant_id_; }
  bool get_if_exists()        { return if_exists_; }
  inline const common::ObString &get_event_name() const { return event_name_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  uint64_t tenant_id_;
  bool if_exists_;
  common::ObString event_database_;
  common::ObString event_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropEventStmt);
};
} // end namespace sql
} // end namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_CMD_OB_DROP_EVENT_STMT_
