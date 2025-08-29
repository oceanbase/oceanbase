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

#ifndef OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_SCHEMA_HISTORY_RECYCLER_H_
#define OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_SCHEMA_HISTORY_RECYCLER_H_

#include "rootserver/ob_schema_history_recycler.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace rootserver
{
  /*
* #############################################
* #   tenant object mysql priviledge          #
* #############################################
*/

struct ObObjectPrivMysqlSchemaKey
{
public:
  ObObjectPrivMysqlSchemaKey();
  ObObjectPrivMysqlSchemaKey(const ObObjectPrivMysqlSchemaKey &other);
  ~ObObjectPrivMysqlSchemaKey();
  bool operator==(const ObObjectPrivMysqlSchemaKey &other) const;
  bool operator!=(const ObObjectPrivMysqlSchemaKey &other) const;
  bool operator<(const ObObjectPrivMysqlSchemaKey &other) const;
  ObObjectPrivMysqlSchemaKey &operator=(const ObObjectPrivMysqlSchemaKey &other);
  int assign(const ObObjectPrivMysqlSchemaKey &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  TO_STRING_KV(K_(user_id), K_(obj_name), K_(obj_type), K_(all_priv), K_(grantor), K_(grantor_host));
public:
  int64_t user_id_;
  int64_t obj_type_;
  int64_t all_priv_;
  char obj_name_ptr_[OB_MAX_CORE_TALBE_NAME_LENGTH];
  ObString obj_name_;
  char grantor_ptr_[OB_MAX_USER_NAME_LENGTH_STORE];
  ObString grantor_;
  char grantor_host_ptr_[OB_MAX_HOST_NAME_LENGTH];
  ObString grantor_host_;
};

struct ObObjectPrivMysqlCompressSchemaInfo
{
public:
  ObObjectPrivMysqlCompressSchemaInfo()
    : key_(), max_schema_version_(common::OB_INVALID_VERSION) {}
  ~ObObjectPrivMysqlCompressSchemaInfo() {}
  TO_STRING_KV(K_(key), K_(max_schema_version));
public:
  ObObjectPrivMysqlSchemaKey key_;
  int64_t max_schema_version_;
};

// RECYCLE AND COMPRESS
class ObObjectPrivMysqlRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObObjectPrivMysqlRecycleSchemaExecutor() = delete;
  ObObjectPrivMysqlRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char* table_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler);
  virtual ~ObObjectPrivMysqlRecycleSchemaExecutor();
private:
  virtual bool is_valid() const;
  virtual int fill_schema_history_map();
  virtual int recycle_schema_history();
  virtual int compress_schema_history();

  virtual int gen_fill_schema_history_sql(
              int64_t start_idx,
              common::ObSqlString &sql);
  virtual int retrieve_schema_history(
      common::sqlclient::ObMySQLResult &result,
      ObObjectPrivMysqlSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObObjectPrivMysqlSchemaKey &cur_key,
      ObObjectPrivMysqlSchemaKey &key);
  virtual int fill_schema_history(
      const ObObjectPrivMysqlSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
    const common::ObIArray<ObObjectPrivMysqlSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql);
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObObjectPrivMysqlSchemaKey> &dropped_schema_keys);

  virtual int gen_batch_compress_schema_history_sql(
      const ObIArray<ObObjectPrivMysqlCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObObjectPrivMysqlCompressSchemaInfo> &compress_schema_infos);
private:
  common::hash::ObHashMap<ObObjectPrivMysqlSchemaKey, ObRecycleSchemaValue, common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObObjectPrivMysqlRecycleSchemaExecutor);
};
}
}
#endif // OCEANBASE_ROOTSERVER_OB_SCHEMA_HISTORY_RECYCLER_H_