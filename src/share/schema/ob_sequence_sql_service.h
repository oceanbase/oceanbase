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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SEQUENCE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SEQUENCE_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObISQLClient;
}
namespace share
{
namespace schema
{
class ObSequenceSchema;

class ObSequenceSqlService : public ObDDLSqlService
{
public:
  ObSequenceSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObSequenceSqlService() {}

  virtual int insert_sequence(const ObSequenceSchema &sequence_schema,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str,
                              const uint64_t *old_sequence_id);
  virtual int replace_sequence(const ObSequenceSchema &sequence_schema,
                               const bool is_rename,
                               common::ObISQLClient *sql_client,
                               bool alter_start_with,
                               bool need_clean_cache,
                               const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_sequence(const uint64_t tenant_id,
                              const uint64_t database_id,
                              const uint64_t sequence_id,
                              const int64_t new_schema_version,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int drop_sequence(const ObSequenceSchema &sequence_schema,
                            const int64_t new_schema_version,
                            common::ObISQLClient *sql_client,
                            const common::ObString *ddl_stmt_str = NULL);
private:
  int add_sequence(common::ObISQLClient &sql_client, const ObSequenceSchema &sequence_schema,
                   const bool only_history, const uint64_t *old_sequence_id);
  int add_sequence_to_value_table(const uint64_t tenant_id,
                                  const uint64_t exec_tenant_id,
                                  const uint64_t old_sequence_id,
                                  const uint64_t new_sequence_id,
                                  common::ObISQLClient &sql_client,
                                  ObIAllocator &allocator);
  int alter_sequence_start_with(const ObSequenceSchema &sequence_schema,
                                common::ObISQLClient &sql_client);
  int get_sequence_sync_value(const uint64_t tenant_id,
                              const uint64_t sequence_id,
                              common::ObISQLClient &sql_client,
                              ObIAllocator &allocator,
                              common::number::ObNumber &next_value);
  int clean_sequence_cache(uint64_t tenant_id, uint64_t sequence_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSequenceSqlService);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_SEQUENCE_SQL_SERVICE_H_
