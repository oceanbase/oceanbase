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

#ifndef SRC_PL_OB_PL_DBLINK_GUARD_H_
#define SRC_PL_OB_PL_DBLINK_GUARD_H_

#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "pl/ob_pl_user_type.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_getter_guard.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/dblink/ob_pl_dblink_info.h"
#endif

namespace oceanbase
{
using namespace sql;
namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObIRoutineInfo;
class ObDbLinkSchema;
}
}
namespace pl
{

typedef share::schema::ObDbLinkSchema ObDbLinkSchema;
typedef share::schema::ObObjectType ObObjectType;
typedef share::schema::ObIRoutineInfo ObIRoutineInfo;
typedef share::schema::ObRoutineInfo ObRoutineInfo;
typedef share::schema::ObRoutineType ObRoutineType;
typedef share::schema::ObRoutineParam ObRoutineParam;
typedef share::schema::ObSchemaUtils ObSchemaUtils;
typedef oceanbase::common::ObSqlString ObSqlString;
class ObPLDbLinkGuard
{
public:
  ObPLDbLinkGuard(common::ObArenaAllocator &alloc) : alloc_(alloc)
  {
    reset();
  }
  ~ObPLDbLinkGuard()
  {
    reset();
  }
  void reset()
  {
#ifdef OB_BUILD_ORACLE_PL
    dblink_infos_.reset();
#endif
    next_link_object_id_ = 1;
  }
  int get_routine_infos_with_synonym(sql::ObSQLSessionInfo &session_info,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     const common::ObString &dblink_name,
                                     const common::ObString &part1,
                                     const common::ObString &part2,
                                     const common::ObString &part3,
                                     common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos);
  int get_dblink_type_with_synonym(sql::ObSQLSessionInfo &session_info,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   const common::ObString &dblink_name,
                                   const common::ObString &part1,
                                   const common::ObString &part2,
                                   const common::ObString &part3,
                                   const pl::ObUserDefinedType *&udt);

  int get_dblink_routine_info(uint64_t dblink_id,
                              uint64_t pkg_id,
                              uint64_t routine_id,
                              const share::schema::ObRoutineInfo *&routine_info);

  int get_dblink_type_by_id(const uint64_t mask_dblink_id,
                            const uint64_t udt_id,
                            const pl::ObUserDefinedType *&udt);

  int get_dblink_type_by_name(const uint64_t dblink_id,
                              const common::ObString &db_name,
                              const common::ObString &pkg_name,
                              const common::ObString &udt_name,
                              const pl::ObUserDefinedType *&udt);
#ifdef OB_BUILD_ORACLE_PL
  int get_dblink_info(const uint64_t dblink_id,
                      const ObPLDbLinkInfo *&dblink_info);
#endif

private:
  int dblink_name_resolve(common::ObDbLinkProxy *dblink_proxy,
                          common::sqlclient::ObISQLConnection *dblink_conn,
                          const ObDbLinkSchema *dblink_schema,
                          const common::ObString &full_name,
                          common::ObString &schema,
                          common::ObString &object_name,
                          common::ObString &sub_object_name,
                          int64_t &object_type,
                          ObIAllocator &alloctor);

  int get_dblink_routine_infos(common::ObDbLinkProxy *dblink_proxy,
                               common::sqlclient::ObISQLConnection *dblink_conn,
                               sql::ObSQLSessionInfo &session_info,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const common::ObString &dblink_name,
                               const common::ObString &db_name,
                               const common::ObString &pkg_name,
                               const common::ObString &routine_name,
                               common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos);

  int get_dblink_type_by_name(common::ObDbLinkProxy *dblink_proxy,
                              common::sqlclient::ObISQLConnection *dblink_conn,
                              sql::ObSQLSessionInfo &session_info,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const common::ObString &dblink_name,
                              const common::ObString &db_name,
                              const common::ObString &pkg_name,
                              const common::ObString &udt_name,
                              const pl::ObUserDefinedType *&udt);

private:
  uint64_t next_link_object_id_;
  common::ObArenaAllocator &alloc_;
#ifdef OB_BUILD_ORACLE_PL
  common::ObSEArray<const ObPLDbLinkInfo *, 1> dblink_infos_;
#endif
};


}
}

#endif
