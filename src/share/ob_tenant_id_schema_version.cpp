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

#define USING_LOG_PREFIX SHARE

#include "share/ob_tenant_id_schema_version.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
namespace share
{

OB_SERIALIZE_MEMBER(TenantIdAndSchemaVersion, tenant_id_, schema_version_);

} // end namespace share
} // end namespace oceanbase
