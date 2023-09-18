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

#ifndef OCEANBASE_SHARE_IMPORT_TABLE_UTIL_H
#define OCEANBASE_SHARE_IMPORT_TABLE_UTIL_H
#include "lib/ob_define.h"
#include "share/schema/ob_multi_version_schema_service.h"
namespace oceanbase
{
namespace share
{
class ObImportTableUtil final
{
public:
static bool can_retrieable_err(const int err_code);
static int get_tenant_schema_guard(share::schema::ObMultiVersionSchemaService &schema_service, uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard &guard);
static int check_database_schema_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &db_name, bool &is_exist);
static int check_table_schema_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &db_name, const ObString &table_name, bool &is_exist);
static int check_tablegroup_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &tablegroup, bool &is_exist);
static int check_tablespace_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &tablespace, bool &is_exist);
static int get_tenant_name_case_mode(const uint64_t tenant_id, ObNameCaseMode &name_case_mode);
static int check_is_recover_table_aux_tenant(
    share::schema::ObMultiVersionSchemaService &schema_service, const uint64_t tenant_id, bool &is_recover_table_aux_tenant);
static int check_is_recover_table_aux_tenant_name(const ObString &tenant_name, bool &is_recover_table_aux_tenant);
};

}
}

#endif