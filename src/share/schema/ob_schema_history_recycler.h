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
#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_HISTORY_RECYCLER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_HISTORY_RECYCLER_H_

#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

struct ObFirstSchemaKey
{
public:
  ObFirstSchemaKey();
  ~ObFirstSchemaKey();
  bool operator==(const ObFirstSchemaKey &other) const;
  bool operator!=(const ObFirstSchemaKey &other) const;
  bool operator<(const ObFirstSchemaKey &other) const;
  ObFirstSchemaKey &operator=(const ObFirstSchemaKey &other);
  int assign(const ObFirstSchemaKey &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(first_schema_id));
public:
  union{
    uint64_t first_schema_id_; // without tenant_id
    uint64_t tenant_id_;
    uint64_t table_id_;
    uint64_t tablegroup_id_;
    uint64_t database_id_;
    uint64_t user_id_;
    uint64_t synoym_id_;
    uint64_t outline_id_;
    uint64_t routine_id_;
    uint64_t package_id_;
    uint64_t sequence_id_;
    uint64_t keystore_id_;
    uint64_t label_se_policy_id_;
    uint64_t label_se_component_id_;
    uint64_t label_se_label_id_;
    uint64_t label_se_user_level_id_;
    uint64_t tablespace_id_;
    uint64_t audit_id_;
    uint64_t trigger_id_;
    uint64_t profile_id_;
    uint64_t type_id_;
    uint64_t coll_type_id_;
  };
};

struct ObSecondSchemaKey : public ObFirstSchemaKey
{
public:
  ObSecondSchemaKey();
  ~ObSecondSchemaKey();
  int assign(const ObSecondSchemaKey &other);
  void reset();
  bool is_valid() const;
  bool operator==(const ObSecondSchemaKey &other) const;
  bool operator!=(const ObSecondSchemaKey &other) const;
  bool operator<(const ObSecondSchemaKey &other) const;
  ObSecondSchemaKey &operator=(const ObSecondSchemaKey &other);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(first_schema_id), K_(second_schema_id));
public:
  union {
    uint64_t second_schema_id_;
    uint64_t column_id_;
    uint64_t part_id_;
    uint64_t constraint_id_;
    uint64_t attribute_;
    uint64_t child_column_id_;
  };
};

struct ObThirdSchemaKey : public ObSecondSchemaKey
{
public:
  ObThirdSchemaKey();
  ~ObThirdSchemaKey();
  int assign(const ObThirdSchemaKey &other);
  void reset();
  bool is_valid() const;
  bool operator==(const ObThirdSchemaKey &other) const;
  bool operator!=(const ObThirdSchemaKey &other) const;
  bool operator<(const ObThirdSchemaKey &other) const;
  ObThirdSchemaKey &operator=(const ObThirdSchemaKey &other);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(first_schema_id), K_(second_schema_id), K_(third_schema_id));
public:
  union {
    uint64_t third_schema_id_;
    uint64_t sub_part_id_;
    uint64_t parent_column_id_;
  };
};

struct ObRecycleSchemaValue
{
public:
  ObRecycleSchemaValue();
  ~ObRecycleSchemaValue();
  ObRecycleSchemaValue &operator=(const ObRecycleSchemaValue &other);
  int assign(const ObRecycleSchemaValue &other);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(is_deleted), K_(max_schema_version), K_(record_cnt));
public:
  bool is_deleted_;
  int64_t max_schema_version_;
  int64_t record_cnt_;
};

struct ObFirstCompressSchemaInfo
{
public:
  ObFirstCompressSchemaInfo()
    : key_(), max_schema_version_(common::OB_INVALID_VERSION) {}
  ~ObFirstCompressSchemaInfo() {}
  TO_STRING_KV(K_(key), K_(max_schema_version));
public:
  ObFirstSchemaKey key_;
  int64_t max_schema_version_;
};

typedef ObFirstCompressSchemaInfo ObFirstArchiveSchemaInfo;

class ObSchemaHistoryRecycler
{
public:
  ObSchemaHistoryRecycler();
  ~ObSchemaHistoryRecycler();
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy &sql_proxy,
           obrpc::ObSrvRpcProxy &srv_rpc_proxy,
           const uint64_t tenant_id);
  void reset();
  int calc_recycle_schema_version(int64_t &recycle_schema_version, const int64_t last_completed_version);
  int execute(const int64_t recycle_schema_version);
  int check_stop() const;
  void set_recycle_not_completed() { is_recycle_completed_ = false; }
  bool is_recycle_completed() const { return is_recycle_completed_; }
  int get_schema_version_by_timestamp(
      const uint64_t tenant_id,
      const int64_t timestamp,
      int64_t &schema_version) const;
private:
  RPC_F(obrpc::OB_GET_MIN_SSTABLE_SCHEMA_VERSION,
        obrpc::ObGetMinSSTableSchemaVersionArg,
        obrpc::ObGetMinSSTableSchemaVersionRes,
        ObGetMinSSTableSchemaVersionProxy);

  int check_inner_stat_() const;
  bool is_valid_recycle_schema_version_(const int64_t recycle_schema_version) const;
  bool can_shortcut_(const int64_t last_completed_version, const int64_t recycle_schema_version) const;
  int get_recycle_schema_version_by_server_(int64_t &recycle_schema_version);
  int get_recycle_schema_version_for_ddl_(int64_t &recycle_schema_version);
  int get_recycle_schema_version_by_expire_time_(int64_t &recycle_schema_version);
  int get_recycle_schema_version_by_major_version_(int64_t &recycle_schema_version);

private:
  bool inited_;
  uint64_t tenant_id_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  bool is_recycle_completed_;
};

class ObIRecycleSchemaExecutor
{
public:
  enum RecycleMode {
    NONE = 0,
    RECYCLE_ONLY = 1,
    COMPRESS_ONLY = 2,
    RECYCLE_AND_COMPRESS = 3,
    RECYCLE_AND_ARCHIVE = 4,
  };
  static const int64_t SCHEMA_HISTORY_BATCH_FETCH_NUM = 10000;
  static const int64_t BATCH_RECYCLE_SCHEMA_CNT = 100;
  static const int64_t BATCH_RECYCLE_RECORD_CNT = 10000;
  static const int64_t TOTAL_RECYCLE_RECORD_CNT = 100000;
  static const int64_t BATCH_COMPRESS_SCHEMA_CNT = 100;
  static const int64_t BATCH_COMPRESS_RECORD_CNT = 1000;
  static const int64_t BATCH_ARCHIVE_SCHEMA_CNT = 100;
  static const int64_t BATCH_ARCHIVE_RECORD_CNT = 1000;
  static const int64_t COMPRESS_RECORD_THREHOLD = 10;
  static const int64_t TOTAL_GC_ARCHIVE_HISTORY_RECORD_CNT = 100000;
  static const int64_t BATCH_DELETE_ROW_LIMIT = 10000;
  static const int64_t BUCKET_NUM = 10000;
public:
  ObIRecycleSchemaExecutor() = delete;
  ObIRecycleSchemaExecutor(const uint64_t tenant_id,
                          const int64_t schema_version,
                          const char* table_name,
                          common::ObMySQLProxy *sql_proxy,
                          ObSchemaHistoryRecycler *recycler);
  virtual ~ObIRecycleSchemaExecutor();
  virtual int execute();

  static bool need_recycle(RecycleMode mode);
  static bool need_compress(RecycleMode mode);
  static bool need_archive(RecycleMode mode);
protected:
  virtual bool is_valid() const = 0;
  int check_stop();

  virtual int fill_schema_history_map() = 0;
  virtual int recycle_schema_history() = 0;
  virtual int compress_schema_history() = 0;
  virtual int archive_schema_history() { return OB_NOT_SUPPORTED; }

  // ===== keyset pagination scan anchor helpers start =====
  // Default implementation advances offset cursor.
  // - Executors that use limit-offset pagination should do nothing.
  // - Executors that use keyset pagination should overload update_scan_cursor_().

  template <typename KEY>
  void update_scan_cursor_(
      const KEY &last_key,
      const int64_t last_schema_version,
      const bool has_row,
      int64_t &start_idx)
  {
    UNUSED(last_key);
    UNUSED(last_schema_version);
    UNUSED(has_row);
    start_idx += SCHEMA_HISTORY_BATCH_FETCH_NUM;
  }

  void reset_scan_anchor_()
  {
    has_anchor_ = false;
    last_schema_version_ = OB_INVALID_VERSION;
  }
  // ===== keyset pagination scan anchor helpers end =====
protected:
  uint64_t tenant_id_;
  int64_t schema_version_;
  const char* table_name_;
  common::ObMySQLProxy *sql_proxy_;
  ObSchemaHistoryRecycler* recycler_;

  // keyset pagination anchor
  bool has_anchor_;
  int64_t last_schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObIRecycleSchemaExecutor);
};

#define DEFINE_KEYSET_UPDATE_SCAN_CURSOR_FUNC(KEY_TYPE) \
  void update_scan_cursor_( \
      const KEY_TYPE &last_key, \
      const int64_t last_schema_version, \
      const bool has_row, \
      int64_t &start_idx) \
  { \
    UNUSED(start_idx); \
    if (has_row) { \
      last_key_ = last_key; \
      has_anchor_ = true; \
      last_schema_version_ = last_schema_version; \
    } else { \
      reset_scan_anchor_(); \
    } \
  }

class ObRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObRecycleSchemaExecutor() = delete;
  ObRecycleSchemaExecutor(const uint64_t tenant_id,
                          const int64_t schema_version,
                          const char* table_name,
                          const char* schema_key_name,
                          const RecycleMode mode,
                          common::ObMySQLProxy *sql_proxy,
                          ObSchemaHistoryRecycler *recycler);
  virtual ~ObRecycleSchemaExecutor();
  virtual int execute();
private:
  int gen_archive_history_table_name_(const char *history_table_name,
                                      common::ObSqlString &archive_history_table_name);
  int gc_archive_history();
  virtual bool is_valid() const;
  virtual int fill_schema_history_map();
  virtual int recycle_schema_history();
  virtual int compress_schema_history();
  virtual int archive_schema_history();

  virtual int gen_fill_schema_history_sql(
              int64_t start_idx,
              common::ObSqlString &sql);
  virtual int retrieve_schema_history(
      common::sqlclient::ObMySQLResult &result,
      ObFirstSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObFirstSchemaKey &cur_key,
      ObFirstSchemaKey &key);
  virtual int fill_schema_history(
      const ObFirstSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
    const common::ObIArray<ObFirstSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql);
  int gen_batch_recycle_archive_schema_history_sql(
    const common::ObIArray<ObFirstSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql);
  int gen_batch_recycle_schema_history_sql_(
    const char *table_name,
    const common::ObIArray<ObFirstSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql);
  int check_aux_version_exceeds_data_();
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObFirstSchemaKey> &dropped_schema_keys);
  int batch_recycle_archive_schema_history(
      const common::ObIArray<ObFirstSchemaKey> &dropped_schema_keys);
  virtual int gen_batch_compress_schema_history_sql(
      const ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int gen_batch_archive_schema_history_sql(
      const ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &insert_sql,
      common::ObSqlString &delete_sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos);
  virtual int batch_archive_schema_history(
      const common::ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos);
private:
  const char* schema_key_name_;
  const RecycleMode mode_;
  common::hash::ObHashMap<ObFirstSchemaKey,
                          ObRecycleSchemaValue,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<ObFirstSchemaKey>,
                          common::hash::equal_to<ObFirstSchemaKey>,
                          common::hash::SimpleAllocer<common::hash::HashMapTypes<ObFirstSchemaKey, ObRecycleSchemaValue>::AllocType>,
                          common::hash::NormalPointer,
                          oceanbase::common::ObMalloc,
                          2 /* EXTEND_RATIO */> schema_history_map_;
  ObFirstSchemaKey last_key_;
  DEFINE_KEYSET_UPDATE_SCAN_CURSOR_FUNC(ObFirstSchemaKey);
  DISALLOW_COPY_AND_ASSIGN(ObRecycleSchemaExecutor);
};

class ObSecondRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObSecondRecycleSchemaExecutor() = delete;
  ObSecondRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char* table_name,
    const char* schema_key_name,
    const char* second_schema_key_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler);
  virtual ~ObSecondRecycleSchemaExecutor();
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
      ObSecondSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObSecondSchemaKey &cur_key,
      ObSecondSchemaKey &key);
  virtual int fill_schema_history(
      const ObSecondSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
      const common::ObIArray<ObSecondSchemaKey> &dropped_schema_keys,
      common::ObSqlString &sql);
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObSecondSchemaKey> &dropped_schema_ids);
private:
  const char* schema_key_name_;
  const char* second_schema_key_name_;
  common::hash::ObHashMap<ObSecondSchemaKey,
                          ObRecycleSchemaValue,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<ObSecondSchemaKey>,
                          common::hash::equal_to<ObSecondSchemaKey>,
                          common::hash::SimpleAllocer<common::hash::HashMapTypes<ObSecondSchemaKey, ObRecycleSchemaValue>::AllocType>,
                          common::hash::NormalPointer,
                          oceanbase::common::ObMalloc,
                          2 /* EXTEND_RATIO */> schema_history_map_;
  ObSecondSchemaKey last_key_;
  DEFINE_KEYSET_UPDATE_SCAN_CURSOR_FUNC(ObSecondSchemaKey);
  DISALLOW_COPY_AND_ASSIGN(ObSecondRecycleSchemaExecutor);
};

class ObThirdRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObThirdRecycleSchemaExecutor() = delete;
  ObThirdRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char* table_name,
    const char* schema_key_name,
    const char* second_schema_key_name,
    const char* third_schema_key_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler);
  virtual ~ObThirdRecycleSchemaExecutor();
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
      ObThirdSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObThirdSchemaKey &cur_key,
      ObThirdSchemaKey &key);
  virtual int fill_schema_history(
      const ObThirdSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
      const common::ObIArray<ObThirdSchemaKey> &dropped_schema_keys,
      common::ObSqlString &sql);
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObThirdSchemaKey> &dropped_schema_ids);
private:
  const char* schema_key_name_;
  const char* second_schema_key_name_;
  const char* third_schema_key_name_;
  common::hash::ObHashMap<ObThirdSchemaKey,
                          ObRecycleSchemaValue,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<ObThirdSchemaKey>,
                          common::hash::equal_to<ObThirdSchemaKey>,
                          common::hash::SimpleAllocer<common::hash::HashMapTypes<ObThirdSchemaKey, ObRecycleSchemaValue>::AllocType>,
                          common::hash::NormalPointer,
                          oceanbase::common::ObMalloc,
                          2 /* EXTEND_RATIO */> schema_history_map_;
  ObThirdSchemaKey last_key_;
  DEFINE_KEYSET_UPDATE_SCAN_CURSOR_FUNC(ObThirdSchemaKey);
  DISALLOW_COPY_AND_ASSIGN(ObThirdRecycleSchemaExecutor);
};

#undef DEFINE_KEYSET_UPDATE_SCAN_CURSOR_FUNC

// system variable
struct ObSystemVariableSchemaKey
{
public:
  ObSystemVariableSchemaKey();
  ~ObSystemVariableSchemaKey();
  bool operator==(const ObSystemVariableSchemaKey &other) const;
  bool operator!=(const ObSystemVariableSchemaKey &other) const;
  bool operator<(const ObSystemVariableSchemaKey &other) const;
  ObSystemVariableSchemaKey &operator=(const ObSystemVariableSchemaKey &other);
  int assign(const ObSystemVariableSchemaKey &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(zone), K_(name));
public:
  common::ObString zone_;
  common::ObString name_;
};

struct ObSystemVariableCompressSchemaInfo
{
public:
  ObSystemVariableCompressSchemaInfo()
    : key_(), max_schema_version_(common::OB_INVALID_VERSION) {}
  ~ObSystemVariableCompressSchemaInfo() {}
  TO_STRING_KV(K_(key), K_(max_schema_version));
public:
  ObSystemVariableSchemaKey key_;
  int64_t max_schema_version_;
};

class ObSystemVariableRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObSystemVariableRecycleSchemaExecutor() = delete;
  ObSystemVariableRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char* table_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler);
  virtual ~ObSystemVariableRecycleSchemaExecutor();
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
      ObSystemVariableSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObSystemVariableSchemaKey &cur_key,
      ObSystemVariableSchemaKey &key);
  virtual int fill_schema_history(
      const ObSystemVariableSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_compress_schema_history_sql(
      const ObIArray<ObSystemVariableCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObSystemVariableCompressSchemaInfo> &compress_schema_infos);
private:
  common::hash::ObHashMap<ObSystemVariableSchemaKey, ObRecycleSchemaValue, common::hash::NoPthreadDefendMode> schema_history_map_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObSystemVariableRecycleSchemaExecutor);
};

// tenant object privilege
struct ObObjectPrivSchemaKey
{
public:
  ObObjectPrivSchemaKey();
  ~ObObjectPrivSchemaKey();
  bool operator==(const ObObjectPrivSchemaKey &other) const;
  bool operator!=(const ObObjectPrivSchemaKey &other) const;
  bool operator<(const ObObjectPrivSchemaKey &other) const;
  ObObjectPrivSchemaKey &operator=(const ObObjectPrivSchemaKey &other);
  int assign(const ObObjectPrivSchemaKey &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(obj_id), K_(obj_type), K_(col_id), K_(grantor_id), K_(grantee_id), K_(priv_id));
public:
  int64_t obj_id_;
  int64_t obj_type_;
  int64_t col_id_;
  int64_t grantor_id_;
  int64_t grantee_id_;
  int64_t priv_id_;
};

struct ObObjectPrivCompressSchemaInfo
{
public:
  ObObjectPrivCompressSchemaInfo()
    : key_(), max_schema_version_(common::OB_INVALID_VERSION) {}
  ~ObObjectPrivCompressSchemaInfo() {}
  TO_STRING_KV(K_(key), K_(max_schema_version));
public:
  ObObjectPrivSchemaKey key_;
  int64_t max_schema_version_;
};

class ObObjectPrivRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObObjectPrivRecycleSchemaExecutor() = delete;
  ObObjectPrivRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char* table_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler);
  virtual ~ObObjectPrivRecycleSchemaExecutor();
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
      ObObjectPrivSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObObjectPrivSchemaKey &cur_key,
      ObObjectPrivSchemaKey &key);
  virtual int fill_schema_history(
      const ObObjectPrivSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
    const common::ObIArray<ObObjectPrivSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql);
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObObjectPrivSchemaKey> &dropped_schema_keys);

  virtual int gen_batch_compress_schema_history_sql(
      const ObIArray<ObObjectPrivCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObObjectPrivCompressSchemaInfo> &compress_schema_infos);
private:
  common::hash::ObHashMap<ObObjectPrivSchemaKey, ObRecycleSchemaValue, common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObObjectPrivRecycleSchemaExecutor);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_HISTORY_RECYCLER_H_
