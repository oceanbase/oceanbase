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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_objpriv_mysql_schema_history_recycler.h"

namespace oceanbase
{
namespace rootserver
{
ObObjectPrivMysqlSchemaKey::ObObjectPrivMysqlSchemaKey()
  : user_id_(OB_INVALID_ID),
    obj_type_(OB_INVALID_ID),
    all_priv_(0),
    obj_name_len_(0),
    grantor_len_(0),
    grantor_host_len_(0)
{
}

ObObjectPrivMysqlSchemaKey::ObObjectPrivMysqlSchemaKey(const ObObjectPrivMysqlSchemaKey &other)
  : user_id_(other.user_id_),
    obj_type_(other.obj_type_),
    all_priv_(other.all_priv_),
    obj_name_len_(other.obj_name_len_),
    grantor_len_(other.grantor_len_),
    grantor_host_len_(other.grantor_host_len_)
{
  MEMCPY(obj_name_, other.obj_name_, other.obj_name_len_);
  MEMCPY(grantor_, other.grantor_, other.grantor_len_);
  MEMCPY(grantor_host_, other.grantor_host_, other.grantor_host_len_);
}

ObObjectPrivMysqlSchemaKey::~ObObjectPrivMysqlSchemaKey()
{
}

bool ObObjectPrivMysqlSchemaKey::operator==(const ObObjectPrivMysqlSchemaKey &other) const
{
  ObString tmp_obj_name(obj_name_len_, obj_name_);
  ObString tmp_grantor(grantor_len_, grantor_);
  ObString tmp_grantor_host(grantor_host_len_, grantor_host_);
  ObString tmp_other_obj_name(other.obj_name_len_, other.obj_name_);
  ObString tmp_other_grantor(other.grantor_len_, other.grantor_);
  ObString tmp_other_grantor_host(other.grantor_host_len_, other.grantor_host_);
  return user_id_ == other.user_id_
         && tmp_obj_name == tmp_other_obj_name
         && obj_type_ == other.obj_type_
         && all_priv_ == other.all_priv_
         && tmp_grantor == tmp_other_grantor
         && tmp_grantor_host == tmp_other_grantor_host;
}

bool ObObjectPrivMysqlSchemaKey::operator!=(const ObObjectPrivMysqlSchemaKey &other) const
{
  return !(*this==other);
}

bool ObObjectPrivMysqlSchemaKey::operator<(const ObObjectPrivMysqlSchemaKey &other) const
{
  bool bret = false;
  ObString tmp_obj_name(obj_name_len_, obj_name_);
  ObString tmp_grantor(grantor_len_, grantor_);
  ObString tmp_grantor_host(grantor_host_len_, grantor_host_);
  ObString tmp_other_obj_name(other.obj_name_len_, other.obj_name_);
  ObString tmp_other_grantor(other.grantor_len_, other.grantor_);
  ObString tmp_other_grantor_host(other.grantor_host_len_, other.grantor_host_);
  if (user_id_ != other.user_id_) {
    bret = user_id_ < other.user_id_;
  } else if (tmp_obj_name != tmp_other_obj_name) {
    bret = tmp_obj_name < tmp_other_obj_name;
  } else if (obj_type_ != other.obj_type_) {
    bret = obj_type_ < other.obj_type_;
  } else if (all_priv_ != other.all_priv_) {
    bret = all_priv_ < other.all_priv_;
  } else if (tmp_grantor != tmp_other_grantor) {
    bret = tmp_grantor < tmp_other_grantor;
  } else if (tmp_grantor_host != tmp_other_grantor_host) {
    bret = tmp_grantor_host < tmp_other_grantor_host;
  } else {
    bret = false;
  }
  return bret;
}

ObObjectPrivMysqlSchemaKey &ObObjectPrivMysqlSchemaKey::operator=(const ObObjectPrivMysqlSchemaKey &other)
{
  assign(other);
  return *this;
}

int ObObjectPrivMysqlSchemaKey::assign(const ObObjectPrivMysqlSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    user_id_ = other.user_id_;
    obj_type_ = other.obj_type_;
    all_priv_ = other.all_priv_;
    obj_name_len_ = other.obj_name_len_;
    grantor_len_ = other.grantor_len_;
    grantor_host_len_ = other.grantor_host_len_;
    MEMCPY(obj_name_, other.obj_name_, other.obj_name_len_);
    MEMCPY(grantor_, other.grantor_, other.grantor_len_);
    MEMCPY(grantor_host_, other.grantor_host_, other.grantor_host_len_);
  }
  return ret;
}

void ObObjectPrivMysqlSchemaKey::reset()
{
  user_id_ = OB_INVALID_ID;
  obj_type_ = OB_INVALID_ID;
  all_priv_ = 0;
  obj_name_len_ = 0;
  grantor_len_ = 0;
  grantor_host_len_ = 0;
}

bool ObObjectPrivMysqlSchemaKey::is_valid() const
{
  return user_id_ != OB_INVALID_ID
         && obj_name_len_ != 0
         && obj_type_ != OB_INVALID_ID;
}

uint64_t ObObjectPrivMysqlSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&user_id_, sizeof(user_id_), hash_val);
  hash_val = murmurhash(obj_name_, obj_name_len_, hash_val);
  hash_val = murmurhash(&obj_type_, sizeof(obj_type_), hash_val);
  hash_val = murmurhash(&all_priv_, sizeof(all_priv_), hash_val);
  hash_val = murmurhash(grantor_, grantor_len_, hash_val);
  hash_val = murmurhash(grantor_host_, grantor_host_len_, hash_val);
  return hash_val;
}

ObObjectPrivMysqlRecycleSchemaExecutor::ObObjectPrivMysqlRecycleSchemaExecutor(
  const uint64_t tenant_id,
  const int64_t schema_version,
  const char* table_name,
  common::ObMySQLProxy *sql_proxy,
  ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_history_map_()
{
}

ObObjectPrivMysqlRecycleSchemaExecutor::~ObObjectPrivMysqlRecycleSchemaExecutor()
{
}

bool ObObjectPrivMysqlRecycleSchemaExecutor::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_SYS_TENANT_ID == tenant_id_
      || !ObSchemaService::is_formal_version(schema_version_)
      || OB_ISNULL(table_name_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(recycler_)) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", K(bret), K_(tenant_id), K_(schema_version),
             KP_(table_name), KP_(sql_proxy), KP_(recycler));
  }
  return bret;
}

int ObObjectPrivMysqlRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
    "select user_id, obj_name, obj_type, all_priv, grantor, grantor_host, schema_version, is_deleted "
    "from %s where tenant_id = 0 and schema_version <= %ld "
    "order by user_id, obj_name, obj_type, all_priv, grantor, grantor_host, schema_version "
    "limit %ld, %ld",
    table_name_,  schema_version_,
    start_idx, SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObObjectPrivMysqlRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObObjectPrivMysqlSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else {
    ObString obj_name;
    ObString grantor;
    ObString grantor_host;
    EXTRACT_INT_FIELD_MYSQL(result, "user_id", cur_key.user_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "obj_type", cur_key.obj_type_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "all_priv", cur_key.all_priv_, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "obj_name", obj_name);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "grantor", grantor);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "grantor_host", grantor_host);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
    if (OB_SUCC(ret)) {
      MEMCPY(cur_key.obj_name_, obj_name.ptr(), obj_name.length());
      cur_key.obj_name_[obj_name.length()] = 0;
      cur_key.obj_name_len_ = obj_name.length() + 1;
      MEMCPY(cur_key.grantor_, grantor.ptr(), grantor.length());
      cur_key.grantor_[grantor.length()] = 0;
      cur_key.grantor_len_ = grantor.length() + 1;
      MEMCPY(cur_key.grantor_host_, grantor_host.ptr(), grantor_host.length());
      cur_key.grantor_host_[grantor_host.length()] = 0;
      cur_key.grantor_host_len_ = grantor_host.length() + 1;
    }
  }
  return ret;
}

int ObObjectPrivMysqlRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const common::ObIArray<ObObjectPrivMysqlSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (dropped_schema_keys.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt(
        " delete from %s where schema_version <= %ld"
        " and (tenant_id, user_id, obj_name, obj_type, all_priv, grantor, grantor_host) in ( ",
         table_name_, schema_version_))) {
      LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); i++) {
      const ObObjectPrivMysqlSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (0, %ld, '%.*s', %ld, %ld, '%.*s', '%.*s')", 0 == i ? "" : ",",
                                        key.user_id_,
                                        key.obj_name_len_, key.obj_name_,
                                        key.obj_type_,
                                        key.all_priv_,
                                        key.grantor_len_, key.grantor_,
                                        key.grantor_host_len_, key.grantor_host_))) {
        LOG_WARN("fail to append fmt", KR(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", KR(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

int ObObjectPrivMysqlRecycleSchemaExecutor::gen_batch_compress_schema_history_sql(
    const ObIArray<ObObjectPrivMysqlCompressSchemaInfo> &compress_schema_infos,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (compress_schema_infos.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and ( ",
                                table_name_))) {
      LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < compress_schema_infos.count(); i++) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (user_id  = %ld "
                                        "and obj_name = '%.*s' "
                                        "and obj_type  = %ld "
                                        "and all_priv = %ld "
                                        "and grantor = '%.*s' "
                                        "and grantor_host = '%.*s' "
                                        "and schema_version < %ld)",
                                        0 == i ? "" : "or",
                                        compress_schema_infos.at(i).key_.user_id_,
                                        compress_schema_infos.at(i).key_.obj_name_len_, compress_schema_infos.at(i).key_.obj_name_,
                                        compress_schema_infos.at(i).key_.obj_type_,
                                        compress_schema_infos.at(i).key_.all_priv_,
                                        compress_schema_infos.at(i).key_.grantor_len_, compress_schema_infos.at(i).key_.grantor_,
                                        compress_schema_infos.at(i).key_.grantor_host_len_, compress_schema_infos.at(i).key_.grantor_host_,
                                        compress_schema_infos.at(i).max_schema_version_))) {
        LOG_WARN("fail to append fmt", KR(ret), "schema_info", compress_schema_infos.at(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", KR(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}
}
}