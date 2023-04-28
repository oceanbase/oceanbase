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

#ifndef OCEANBASE_ROOTSERVER_OB_SCHEMA_HISTORY_RECYCLER_H_
#define OCEANBASE_ROOTSERVER_OB_SCHEMA_HISTORY_RECYCLER_H_

#include "observer/ob_server_struct.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_thread_idling.h"
//#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_zone_manager.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace rootserver
{
class ObSchemaHistoryRecyclerIdling : public ObThreadIdling
{
public:
  explicit ObSchemaHistoryRecyclerIdling(volatile bool &stop)
    : ObThreadIdling(stop) {}
  virtual int64_t get_idle_interval_us();
public:
  const static int64_t DEFAULT_SCHEMA_HISTORY_RECYCLE_INTERVAL = 60 * 60 * 1000 * 1000L; //1h
};

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
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
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
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
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
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
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

// Running in a single thread.
class ObSchemaHistoryRecycler : public ObRsReentrantThread
{
public:
  ObSchemaHistoryRecycler();
  virtual ~ObSchemaHistoryRecycler();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           //ObFreezeInfoManager &freeze_info_manager,
           ObZoneManager &zone_manager,
           common::ObMySQLProxy &sql_proxy);
  virtual void run3() override;
  void wakeup();
  void stop();
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  int check_stop();
  int get_recycle_schema_versions(
      const obrpc::ObGetRecycleSchemaVersionsArg &arg,
      obrpc::ObGetRecycleSchemaVersionsResult &result);
private:
  int check_inner_stat();
  bool is_valid_recycle_schema_version(const int64_t recycle_schema_version);
  int idle();
  int try_recycle_schema_history();
  int try_recycle_schema_history(const common::ObIArray<uint64_t> &tenant_ids);
  int try_recycle_schema_history(
      const uint64_t tenant_id,
      const int64_t recycle_schema_version);
  int check_can_skip_tenant(
      const uint64_t tenant_id,
      bool &skip);
  int calc_recycle_schema_versions(
      const common::ObIArray<uint64_t> &tenant_ids);
  int get_recycle_schema_version_by_server(
      const common::ObIArray<uint64_t> &tenant_ids,
      common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions);
  int get_recycle_schema_version_for_ddl(
      const common::ObIArray<uint64_t> &tenant_ids,
      common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions);
  int get_recycle_schema_version_by_global_stat(
      const common::ObIArray<uint64_t> &tenant_ids,
      common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions);
  int fill_recycle_schema_versions(
      const uint64_t tenant_id,
      const int64_t schema_version,
      common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions);
  int update_recycle_schema_versions(
      const common::ObIArray<uint64_t> &tenant_ids,
      common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions);
public:
  static const int64_t BUCKET_NUM = 10;
private:
  bool inited_;
  mutable ObSchemaHistoryRecyclerIdling idling_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  //ObFreezeInfoManager *freeze_info_mgr_;
  ObZoneManager *zone_mgr_;
  common::ObMySQLProxy *sql_proxy_;
  common::hash::ObHashMap<uint64_t, int64_t, common::hash::ReadWriteDefendMode> recycle_schema_versions_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaHistoryRecycler);
};

class ObIRecycleSchemaExecutor
{
public:
  enum RecycleMode {
    NONE = 0,
    RECYCLE_ONLY = 1,
    COMPRESS_ONLY = 2,
    RECYCLE_AND_COMPRESS = 3,
  };
  static const int64_t SCHEMA_HISTORY_BATCH_FETCH_NUM = 10000;
  static const int64_t BATCH_SCHEMA_RECYCLE_CNT = 100;
  static const int64_t BATCH_RECYCLE_RECORD_CNT = 10000;
  static const int64_t TOTAL_RECYCLE_RECORD_CNT = 100000;
  static const int64_t BATCH_COMPRESS_SCHEMA_CNT = 100;
  static const int64_t BATCH_COMPRESS_RECORD_CNT = 1000;
  static const int64_t COMPRESS_RECORD_THREHOLD = 10;
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
protected:
  virtual bool is_valid() const = 0;
  int check_stop();

  virtual int fill_schema_history_map() = 0;
  virtual int recycle_schema_history() = 0;
  virtual int compress_schema_history() = 0;
protected:
  uint64_t tenant_id_;
  int64_t schema_version_;
  const char* table_name_;
  common::ObMySQLProxy *sql_proxy_;
  ObSchemaHistoryRecycler* recycler_;
  DISALLOW_COPY_AND_ASSIGN(ObIRecycleSchemaExecutor);
};

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
  virtual bool is_valid() const;
  virtual int fill_schema_history_map();
  virtual int recycle_schema_history();
  virtual int compress_schema_history();

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
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObFirstSchemaKey> &dropped_schema_keys);

  virtual int gen_batch_compress_schema_history_sql(
      const ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos);
private:
  const char* schema_key_name_;
  const RecycleMode mode_;
  common::hash::ObHashMap<ObFirstSchemaKey,
                          ObRecycleSchemaValue,
                          common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObRecycleSchemaExecutor);
};

// RECYCLE ONLY
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
                          common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSecondRecycleSchemaExecutor);
};

// RECYCLE ONLY
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
                          common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObThirdRecycleSchemaExecutor);
};

/*
 * ##############################
 * #       system variable      #
 * ##############################
 */
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
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
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

// COMPRESS ONLY
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

/*
 * #######################################
 * #   tenant object priviledge          #
 * #######################################
 */


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
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
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

// RECYCLE AND COMPRESS
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

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_SCHEMA_HISTORY_RECYCLER_H_
