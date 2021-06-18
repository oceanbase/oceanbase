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
int64_t ObUpdateTenantFileInfoLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(file_info));
  J_OBJ_END();
  return pos;
}
int64_t ObRemovePGFromTenantFileLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(file_key), K_(pg_key));
  J_OBJ_END();
  return pos;
}
int64_t ObAddPGToTenantFileLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(file_key), K_(pg_key));
  J_OBJ_END();
  return pos;
}
int64_t ObRemoveTenantFileSuperBlockLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(key));
  J_OBJ_END();
  return pos;
}
int64_t ObUpdateTenantFileSuperBlockLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(file_key), K_(super_block));
  J_OBJ_END();
  return pos;
}
