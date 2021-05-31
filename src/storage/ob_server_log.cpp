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

#include "ob_server_log.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

OB_SERIALIZE_MEMBER(ObUpdateTenantFileSuperBlockLogEntry, file_key_, super_block_);

ObUpdateTenantFileSuperBlockLogEntry::ObUpdateTenantFileSuperBlockLogEntry() : file_key_(), super_block_()
{}

OB_SERIALIZE_MEMBER(ObRemoveTenantFileSuperBlockLogEntry, key_, delete_file_);

OB_SERIALIZE_MEMBER(ObAddPGToTenantFileLogEntry, file_key_, pg_key_);

OB_SERIALIZE_MEMBER(ObRemovePGFromTenantFileLogEntry, file_key_, pg_key_);

OB_SERIALIZE_MEMBER(ObUpdateTenantFileInfoLogEntry, file_info_);
