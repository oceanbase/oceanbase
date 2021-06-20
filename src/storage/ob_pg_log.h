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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_GROUP_LOG_
#define OCEANBASE_STORAGE_OB_PARTITION_GROUP_LOG_

#include "clog/ob_i_submit_log_cb.h"
#include "share/ob_rpc_struct.h"
#include "storage/ob_clog_cb_async_worker.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_non_trans_log.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
}

namespace storage {

class ObOfflinePartitionLog : public ObNonTransLog {
  OB_UNIS_VERSION(1);

public:
  ObOfflinePartitionLog()
  {
    reset();
  }
  virtual ~ObOfflinePartitionLog()
  {
    reset();
  }
  int init(const int64_t log_type, const bool is_physical_drop);
  void reset();

public:
  int64_t get_log_type() const
  {
    return log_type_;
  }
  bool is_physical_drop() const
  {
    return is_physical_drop_;
  }
  int64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  virtual bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t tenant_id) override
  {
    UNUSED(tenant_id);
    return common::OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(is_physical_drop), K_(cluster_id));

private:
  DISALLOW_COPY_AND_ASSIGN(ObOfflinePartitionLog);

protected:
  bool is_inited_;
  int64_t log_type_;
  bool is_physical_drop_;
  int64_t cluster_id_;
};

class ObAddPartitionToPGLog : public ObNonTransLog {
  OB_UNIS_VERSION(1);

public:
  ObAddPartitionToPGLog()
  {
    reset();
  }
  virtual ~ObAddPartitionToPGLog()
  {}
  int init(const int64_t log_type, const obrpc::ObCreatePartitionArg& arg);
  void reset();

public:
  int64_t get_log_type() const
  {
    return log_type_;
  }
  const obrpc::ObCreatePartitionArg& get_create_pg_partition_arg()
  {
    return arg_;
  }
  const obrpc::ObCreatePartitionArg& get_arg() const
  {
    return arg_;
  }
  virtual bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override
  {
    UNUSED(new_tenant_id);
    return common::OB_NOT_SUPPORTED;
  }
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(arg));

private:
  DISALLOW_COPY_AND_ASSIGN(ObAddPartitionToPGLog);

protected:
  bool is_inited_;
  int64_t log_type_;
  obrpc::ObCreatePartitionArg arg_;
};

class ObRemovePartitionFromPGLog : public ObNonTransLog {
  OB_UNIS_VERSION(1);

public:
  ObRemovePartitionFromPGLog()
  {
    reset();
  }
  virtual ~ObRemovePartitionFromPGLog()
  {}
  int init(const int64_t log_type, const ObPGKey& pg_key, const ObPartitionKey& pkey);
  void reset();

public:
  int64_t get_log_type() const
  {
    return log_type_;
  }
  const common::ObPGKey& get_pg_key() const
  {
    return pg_key_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  virtual bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(pg_key), K_(partition_key));

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemovePartitionFromPGLog);

protected:
  bool is_inited_;
  int64_t log_type_;
  common::ObPGKey pg_key_;
  common::ObPartitionKey partition_key_;
};

class ObPGSchemaChangeLog : public ObNonTransLog {
  OB_UNIS_VERSION(1);

public:
  ObPGSchemaChangeLog()
  {
    reset();
  }
  virtual ~ObPGSchemaChangeLog()
  {}
  int init(const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey,
      const int64_t schema_version, const uint64_t index_id);
  void reset();

public:
  int64_t get_log_type() const
  {
    return log_type_;
  }
  const common::ObPGKey get_pg_key() const
  {
    return pg_key_;
  }
  const common::ObPartitionKey get_pkey() const
  {
    return pkey_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  uint64_t get_index_id() const
  {
    return index_id_;
  }
  virtual bool is_valid() const;
  virtual int replace_tenant_id(const uint64_t new_tenant_id) override;
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(pg_key), K_(pkey), K_(schema_version), K_(index_id));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGSchemaChangeLog);

protected:
  bool is_inited_;
  int64_t log_type_;
  common::ObPGKey pg_key_;
  // the partition key of schema version changed
  common::ObPartitionKey pkey_;
  int64_t schema_version_;
  uint64_t index_id_;
};

enum ObPGWriteLogState {
  CB_INIT = 0,
  CB_SUCCESS = 1,
  CB_FAIL = 2,
  CB_END = 3,
};

class ObAddPartitionToPGLogCb : public clog::ObISubmitLogCb {
public:
  ObAddPartitionToPGLogCb()
  {
    reset();
  }
  virtual ~ObAddPartitionToPGLogCb()
  {}
  int init(const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey);
  void reset();

  // clog callback
  int on_success(const common::ObPGKey& pg_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed);
  int on_finished(const common::ObPGKey& pg_key, const uint64_t log_id);

  int64_t get_log_type() const
  {
    return log_type_;
  }
  const common::ObPGKey& get_pg_key() const
  {
    return pg_key_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  ObPGWriteLogState get_write_clog_state() const
  {
    return ATOMIC_LOAD(&write_clog_state_);
  }
  int check_can_release(bool& can_release);
  virtual bool is_valid() const;
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(pg_key), K_(partition_key), K_(write_clog_state));

private:
  bool is_inited_;
  int64_t log_type_;
  common::ObPGKey pg_key_;
  common::ObPartitionKey partition_key_;
  ObIPartitionGroupGuard guard_;
  ObPGWriteLogState write_clog_state_;
  // concurrency scenario
  // rs check check_can_release
  // clog callback
  bool is_locking_;
};

class ObRemovePartitionFromPGLogCb : public clog::ObISubmitLogCb {
public:
  ObRemovePartitionFromPGLogCb()
  {
    reset();
  }
  virtual ~ObRemovePartitionFromPGLogCb()
  {}
  int init(const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey,
      ObCLogCallbackAsyncWorker* cb_async_worker);
  void reset();

  // clog callback
  int on_success(const common::ObPGKey& pg_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed);
  int on_finished(const common::ObPGKey& pg_key, const uint64_t log_id);

  int64_t get_log_type() const
  {
    return log_type_;
  }
  const common::ObPGKey& get_pg_key() const
  {
    return pg_key_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  virtual bool is_valid() const;
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(pg_key), K_(partition_key));

private:
  bool is_inited_;
  int64_t log_type_;
  common::ObPGKey pg_key_;
  common::ObPartitionKey partition_key_;
  ObIPartitionGroupGuard guard_;
  ObCLogCallbackAsyncWorker* cb_async_worker_;
};

class ObSchemaChangeClogCb : public clog::ObISubmitLogCb {
public:
  ObSchemaChangeClogCb()
  {
    reset();
  }
  virtual ~ObSchemaChangeClogCb()
  {}
  int init(const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey);
  void reset();

  // clog callback
  int on_success(const common::ObPGKey& pg_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed);
  int on_finished(const common::ObPGKey& pg_key, const uint64_t log_id);

  int64_t get_log_type() const
  {
    return log_type_;
  }
  const common::ObPGKey& get_pg_key() const
  {
    return pg_key_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  ObPGWriteLogState get_write_clog_state() const
  {
    return ATOMIC_LOAD(&write_clog_state_);
  }
  int check_can_release(bool& can_release);
  virtual bool is_valid() const;
  VIRTUAL_TO_STRING_KV(K_(log_type), K_(pg_key), K_(partition_key), K_(write_clog_state));

private:
  bool is_inited_;
  int64_t log_type_;
  common::ObPGKey pg_key_;
  common::ObPartitionKey partition_key_;
  ObIPartitionGroupGuard guard_;
  ObPGWriteLogState write_clog_state_;
  // concurrency scenario
  // rs check check_can_release
  // clog callback
  bool is_locking_;
};

class ObOfflinePartitionCb : public clog::ObISubmitLogCb {
public:
  ObOfflinePartitionCb() : is_inited_(false), is_physical_drop_(false), cb_async_worker_(nullptr)
  {}
  virtual ~ObOfflinePartitionCb()
  {
    destroy();
  }

  int init(ObCLogCallbackAsyncWorker* cb_async_worker, const bool is_physical_drop);
  void reset();
  void destroy();
  virtual int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
      const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed);
  int on_finished(const common::ObPGKey& pg_key, const uint64_t log_id);

private:
  bool is_inited_;
  bool is_physical_drop_;
  ObCLogCallbackAsyncWorker* cb_async_worker_;

  DISALLOW_COPY_AND_ASSIGN(ObOfflinePartitionCb);
};
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PARTITION_GROUP_LOG_
