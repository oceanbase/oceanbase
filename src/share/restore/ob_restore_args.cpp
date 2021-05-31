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

#include "share/restore/ob_restore_args.h"
#include "lib/utility/ob_print_utils.h"
#include <memory.h>

using namespace oceanbase::common;
using namespace oceanbase::share;

ObRestoreArgs::ObRestoreArgs()
    : base_data_version_(OB_INVALID_VERSION),
      curr_data_version_(OB_INVALID_VERSION),
      tenant_id_(OB_INVALID_TENANT_ID),
      restore_timeu_(0),
      backup_schema_id_(OB_INVALID_ID),
      partition_id_(-1),
      schema_id_list_(),
      sql_info_(),
      schema_version_(0),
      cluster_version_(OB_INVALID_ID),
      schema_id_pair_()
{
  MEMSET(uri_header_, '\0', sizeof(uri_header_));
  MEMSET(cluster_name_, '\0', sizeof(cluster_name_));
  MEMSET(storage_info_, '\0', OB_MAX_URI_LENGTH);
  MEMSET(primary_zone_, '\0', MAX_ZONE_LENGTH);
  MEMSET(locality_, '\0', MAX_LOCALITY_LENGTH);
  MEMSET(pool_list_, '\0', OB_MAX_POOL_LIST_LENGTH);
  MEMSET(restore_user_, '\0', OB_MAX_USERNAME_LENGTH);
  MEMSET(restore_pass_, '\0', OB_MAX_PASSWORD_LENGTH);
  MEMSET(tcp_invited_nodes_, '\0', OB_MAX_TCP_INVITED_NODES_LENGTH);
}

void ObRestoreArgs::reset()
{
  MEMSET(uri_header_, '\0', sizeof(uri_header_));
  MEMSET(cluster_name_, '\0', sizeof(cluster_name_));
  MEMSET(storage_info_, '\0', OB_MAX_URI_LENGTH);
  MEMSET(primary_zone_, '\0', MAX_ZONE_LENGTH);
  MEMSET(locality_, '\0', MAX_LOCALITY_LENGTH);
  MEMSET(pool_list_, '\0', OB_MAX_POOL_LIST_LENGTH);
  MEMSET(restore_user_, '\0', OB_MAX_USERNAME_LENGTH);
  MEMSET(restore_pass_, '\0', OB_MAX_PASSWORD_LENGTH);
  MEMSET(tcp_invited_nodes_, '\0', OB_MAX_TCP_INVITED_NODES_LENGTH);
  base_data_version_ = OB_INVALID_VERSION;
  curr_data_version_ = OB_INVALID_VERSION;
  tenant_id_ = OB_INVALID_TENANT_ID;
  backup_schema_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  restore_timeu_ = 0;
  schema_version_ = 0;
  cluster_version_ = OB_INVALID_ID;
}

bool ObRestoreArgs::is_parse_ok() const
{
  bool ret = true;
  if (STRLEN(storage_info_) <= 0 || STRLEN(uri_header_) <= 0 || STRLEN(cluster_name_) <= 0 ||
      STRLEN(restore_user_) <= 0 || OB_INVALID_TENANT_ID == tenant_id_ || 0 == restore_timeu_) {
    ret = false;
  }
  return ret;
}

bool ObRestoreArgs::is_valid() const
{
  bool ret = true;
  if (!is_parse_ok() || OB_INVALID_VERSION == base_data_version_ || OB_INVALID_VERSION == curr_data_version_) {
    ret = false;
  }
  return ret;
}

bool ObRestoreArgs::is_inplace_restore() const
{
  // The only parameter that must be specified when creating a tenant is pool_list
  // The rest include: primary zone, locality, etc. are optional
  ObString pool(pool_list_);
  return pool.empty();
}

// TODO: fill when use parse
const char* ObRestoreArgs::get_uri_header() const
{
  return uri_header_;
}

const char* ObRestoreArgs::get_cluster_name() const
{
  return cluster_name_;
}
// TODO: fill when use parse
const char* ObRestoreArgs::get_storage_info() const
{
  return storage_info_;
}

OB_SERIALIZE_MEMBER(ObRestoreArgs, primary_zone_, locality_, pool_list_, base_data_version_, curr_data_version_,
    tenant_id_, restore_timeu_, backup_schema_id_, partition_id_, schema_id_list_, schema_version_, uri_header_,
    cluster_name_, storage_info_, restore_user_, restore_pass_, tcp_invited_nodes_, cluster_version_, schema_id_pair_);

OB_SERIALIZE_MEMBER(ObSchemaIdPair, schema_id_, backup_schema_id_);

int ObRestoreArgs::set_schema_id_pairs(const common::ObIArray<ObSchemaIdPair>& pairs)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(pairs, idx, cnt, OB_SUCC(ret))
  {
    const ObSchemaIdPair& p = pairs.at(idx);
    if (OB_FAIL(add_schema_id_pair(p.schema_id_, p.backup_schema_id_))) {
      LOG_WARN("fail add schema id pair", K(p), K(ret));
    }
  }
  return ret;
}

int ObRestoreArgs::add_schema_id_pair(const uint64_t index_id, const uint64_t backup_index_id)
{
  int ret = OB_SUCCESS;
  ObSchemaIdPair tmp;

  tmp.schema_id_ = index_id;
  tmp.backup_schema_id_ = backup_index_id;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_id_list_.count(); ++i) {
      if (schema_id_list_.at(i).schema_id_ == index_id || schema_id_list_.at(i).backup_schema_id_ == backup_index_id) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "index id has exist", K(ret), K(schema_id_list_), K(index_id), K(backup_index_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_id_list_.push_back(tmp))) {
      STORAGE_LOG(WARN, "failed to add index id list", K(ret));
    }
  }
  return ret;
}

int ObRestoreArgs::trans_to_backup_schema_id(const uint64_t schema_id, uint64_t& backup_schema_id) const
{
  int ret = OB_ENTRY_NOT_EXIST;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < schema_id_list_.count(); ++i) {
      if (schema_id_list_.at(i).schema_id_ == schema_id) {
        ret = OB_SUCCESS;
        backup_schema_id = schema_id_list_.at(i).backup_schema_id_;
      }
    }
  }
  return ret;
}

int ObRestoreArgs::trans_from_backup_schema_id(const uint64_t backup_schema_id, uint64_t& schema_id) const
{
  int ret = OB_ENTRY_NOT_EXIST;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < schema_id_list_.count(); ++i) {
      if (schema_id_list_.at(i).backup_schema_id_ == backup_schema_id) {
        ret = OB_SUCCESS;
        schema_id = schema_id_list_.at(i).schema_id_;
      }
    }
  }
  return ret;
}
