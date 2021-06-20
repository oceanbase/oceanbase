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

#ifndef OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_
#define OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_

#include "storage/ob_i_partition_group.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/ob_i_partition_storage.h"
#include "common/ob_partition_key.h"
#include "storage/ob_base_storage_info.h"
#include "common/ob_role.h"
#include "storage/ob_replay_status.h"

namespace oceanbase {
namespace clog {
class ObIPartitionLogService;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
using namespace common;
namespace storage {
class ObIPartitionComponentFactory;
class ObIPartitionStorage;
class ObReplayStatus;
class MockObPartition : public ObIPartition {
public:
  int init(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, ObBaseStorage* base_storage,
      clog::ObIPartitionLogService* pls, transaction::ObTransService* txs)
  {
    UNUSED(key);
    UNUSED(cp_fty);
    UNUSED(schema_service);
    UNUSED(base_storage);
    UNUSED(pls);
    UNUSED(txs);
    return OB_NOT_SUPPORTED;
  }
  void destroy()
  {
    ;
  }
  int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos)
  {
    UNUSED(log_seq_num);
    UNUSED(subcmd);
    UNUSED(buf);
    UNUSED(len);
    UNUSED(pos);
    return OB_NOT_SUPPORTED;
  }
  // get partition log service
  const clog::ObIPartitionLogService* get_log_service() const
  {
    return NULL;
  }
  clog::ObIPartitionLogService* get_log_service()
  {
    return NULL;
  }

  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }

  int get_pg_partition(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard)
  {
    UNUSED(pkey);
    UNUSED(guard);
    return OB_SUCCESS;
  }
  int retire_warmup_store(const bool is_disk_full)
  {}
  int enable_write_log()
  {}

  int table_scan(const ObStoreCtx& ctx, ObTableScanParam& param, common::ObNewRowIterator*& result)
  {
    UNUSED(ctx);
    UNUSED(param);
    UNUSED(result);
    return OB_NOT_SUPPORTED;
  }
  int table_scan(const ObStoreCtx& ctx, ObTableScanParam& param, common::ObNewIterIterator*& result)
  {
    UNUSED(ctx);
    UNUSED(param);
    UNUSED(result);
    return OB_NOT_SUPPORTED;
  }
  int revert_scan_iter(common::ObNewRowIterator* iter)
  {
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }
  int revert_scan_iter(common::ObNewIterIterator* iter)
  {
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }
  int delete_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_NOT_SUPPORTED;
  }
  int insert_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_NOT_SUPPORTED;
  }
  int insert_row(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
      int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(duplicated_column_ids);
    UNUSED(row);
    UNUSED(flag);
    UNUSED(affected_rows);
    UNUSED(duplicated_rows);
    return OB_NOT_SUPPORTED;
  }
  int revert_insert_iter(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter)
  {
    UNUSED(pkey);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }
  int update_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(column_ids);
    UNUSED(updated_column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_NOT_SUPPORTED;
  }
  int lock_rows(const ObStoreCtx& ctx, const int64_t timeout, common::ObNewRowIterator* row_iter,
      const ObLockFlag lock_flag, int64_t& affected_rows)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(row_iter);
    UNUSED(lock_flag);
    UNUSED(affected_rows);
    return OB_NOT_SUPPORTED;
  }
  int lock_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObNewRow& row, ObLockFlag lock_flag)
  {
    UNUSED(ctx);
    UNUSED(timeout);
    UNUSED(row);
    UNUSED(lock_flag);
    return OB_NOT_SUPPORTED;
  }

  void set_valid(bool valid)
  {
    UNUSED(valid);
    return;
  }
  bool is_valid() const
  {
    return true;
  }
  int rdlock()
  {
    return OB_NOT_SUPPORTED;
  }
  int wrlock()
  {
    return OB_NOT_SUPPORTED;
  }

  int try_rdlock()
  {
    return OB_NOT_SUPPORTED;
  }
  int try_wrlock()
  {
    return OB_NOT_SUPPORTED;
  }
  int unlock()
  {
    return OB_NOT_SUPPORTED;
  }

  int save_base_storage_info(const common::ObBaseStorageInfo& info)
  {
    UNUSED(info);
    return OB_NOT_SUPPORTED;
  }
  int get_base_storage_info(common::ObBaseStorageInfo& info)
  {
    UNUSED(info);
    return OB_NOT_SUPPORTED;
  }
  int get_saved_storage_info(ObSavedStorageInfo& info)
  {
    UNUSED(info);
    return OB_NOT_SUPPORTED;
  }

  // leader or follower
  int get_role(common::ObRole& role) const
  {
    UNUSED(role);
    return OB_NOT_SUPPORTED;
  }
  int get_role_for_partition_table(common::ObRole& role) const
  {
    UNUSED(role);
    return OB_NOT_SUPPORTED;
  }
  int get_leader_curr_member_list(common::ObMemberList& member_list) const
  {
    UNUSED(member_list);
    return OB_NOT_SUPPORTED;
  }
  ObReplayStatus& get_replay_status()
  {
    return relay_ststus_;
  }

  int set_frozen_version(const int64_t frozen_version, const int64_t frozen_timestamp)
  {

    UNUSED(frozen_version);
    UNUSED(frozen_timestamp);
    return OB_NOT_SUPPORTED;
  }
  int set_freeze_status(const int64_t frozen_version, const int64_t frozen_timestamp, const int64_t status)
  {
    UNUSED(frozen_version);
    UNUSED(frozen_timestamp);
    UNUSED(status);
    return OB_NOT_SUPPORTED;
  }
  int adjust_freeze_status()
  {
    return OB_SUCCESS;
  }
  int64_t get_frozen_version() const
  {
    return 0;
  }
  int64_t get_frozen_timestamp() const
  {
    return 0;
  }
  int64_t get_freeze_status() const
  {
    return 0;
  }

  // write ssstore objects @version tree to data file , used by write_check_point
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return OB_NOT_SUPPORTED;
  }
  // read ssstore objects from data file to construct partition storage's version tree.
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return OB_NOT_SUPPORTED;
  }
  int64_t get_serialize_size()
  {
    return 0;
  }
  virtual int64_t get_c2r_timestamp()
  {
    return OB_INVALID_TIMESTAMP;
  }
  virtual void set_c2r_timestamp(int64_t timestamp)
  {
    UNUSED(timestamp);
  }
  virtual int set_replica_type(const ObReplicaType& replica_type, const bool write_redo_log)
  {
    UNUSED(replica_type);
    UNUSED(write_redo_log);
    return OB_SUCCESS;
  }
  virtual ObPartitionLoopWorker* get_partition_loop_worker()
  {
    return NULL;
  }

protected:
  common::ObPartitionKey partition_key_;
  ObReplayStatus relay_ststus_;
  ObPartitionMCState partition_mc_state_;

private:
  // disallow copy
  // DISALLOW_COPY_AND_ASSIGN(ObIPartition);
};
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_
