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

#ifndef __OB_SHARE_SEQUENCE_SEQUENCE_DDL_PROXY_H__
#define __OB_SHARE_SEQUENCE_SEQUENCE_DDL_PROXY_H__

#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObMySQLTransaction;
}  // namespace common
namespace share {
namespace schema {
class ObSchemaGetterGuard;
class ObSequenceSchema;
class ObMultiVersionSchemaService;
}  // namespace schema

class ObSequenceDDLProxy {
public:
  ObSequenceDDLProxy(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy);
  virtual ~ObSequenceDDLProxy();
  int create_sequence(share::schema::ObSequenceSchema& seq_schema, const common::ObBitSet<>& opt_bitset,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObString* ddl_stmt_str);
  int alter_sequence(share::schema::ObSequenceSchema& seq_schema, const common::ObBitSet<>& opt_bitset,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObString* ddl_stmt_str);
  int drop_sequence(share::schema::ObSequenceSchema& seq_schema, common::ObMySQLTransaction& trans,
      share::schema::ObSchemaGetterGuard& schema_guard, const common::ObString* ddl_stmt_str);
  int rename_sequence(share::schema::ObSequenceSchema& seq_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str);

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObSequenceDDLProxy);
  share::schema::ObMultiVersionSchemaService& schema_service_;
  common::ObMySQLProxy& sql_proxy_;
};
}  // namespace share
}  // namespace oceanbase
#endif /* __OB_SHARE_SEQUENCE_SEQUENCE_DDL_PROXY_H__ */
//// end of header file
