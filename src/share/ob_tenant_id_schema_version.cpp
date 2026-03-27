/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_tenant_id_schema_version.h"
#include "rootserver/ob_root_utils.h"

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
