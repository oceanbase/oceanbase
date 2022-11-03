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

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObSequenceSchema;
class ObMultiVersionSchemaService;
}

enum ObSeqActionType {
  FROM_TABLE_DDL,
  FROM_SEQUENCE_DDL
};

class ObSequenceDDLProxy
{
public:
  ObSequenceDDLProxy(share::schema::ObMultiVersionSchemaService &schema_service);
  virtual ~ObSequenceDDLProxy();
  int create_sequence_without_bitset(share::schema::ObSequenceSchema &seq_schema,
                                     common::ObMySQLTransaction &trans,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     const common::ObString *ddl_stmt_str);
  int create_sequence(share::schema::ObSequenceSchema &seq_schema,
                      const common::ObBitSet<> &opt_bitset,
                      common::ObMySQLTransaction &trans,
                      share::schema::ObSchemaGetterGuard &schema_guard,
                      const common::ObString *ddl_stmt_str);
  // Notice that, offline ddl and truncate operation, sequence object is inherited from origin one.
  // And need_sync_seq_val is used to judge whether to synchronize origin next value of sequence to the inherited one.
  int inner_create_sequence(share::schema::ObSequenceSchema &seq_schema,
                            common::ObMySQLTransaction &trans,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            const common::ObString *ddl_stmt_str,
                            const bool need_sync_seq_val);
  int alter_sequence(share::schema::ObSequenceSchema &seq_schema,
                     const common::ObBitSet<> &opt_bitset,
                     common::ObMySQLTransaction &trans,
                     share::schema::ObSchemaGetterGuard &schema_guard,
                     const common::ObString *ddl_stmt_str,
                     ObSeqActionType seq_action_type);
  int drop_sequence(share::schema::ObSequenceSchema &seq_schema,
                    common::ObMySQLTransaction &trans,
                    share::schema::ObSchemaGetterGuard &schema_guard,
                    const common::ObString *ddl_stmt_str,
                    ObSeqActionType seq_action_type);
  int rename_sequence(share::schema::ObSequenceSchema &seq_schema,
                      common::ObMySQLTransaction &trans,
                      const common::ObString *ddl_stmt_str);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObSequenceDDLProxy);
  share::schema::ObMultiVersionSchemaService &schema_service_;
};
}
}
#endif /* __OB_SHARE_SEQUENCE_SEQUENCE_DDL_PROXY_H__ */
//// end of header file

