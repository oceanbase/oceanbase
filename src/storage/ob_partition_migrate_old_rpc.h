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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_MIGRAT_OLD_RPC_H_
#define OCEANBASE_STORAGE_OB_PARTITION_MIGRAT_OLD_RPC_H_

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "common/ob_partition_key.h"
#include "common/ob_member.h"
#include "storage/ob_saved_storage_info.h"
#include "share/ob_rpc_struct.h"
#include "ob_warm_up_request.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {

namespace storage {
class ObStoreInfo {
public:
  ObStoreInfo();
  virtual ~ObStoreInfo();
  void reset()
  {
    saved_storage_info_.reset();
    sstable_count_ = -1;
  }
  int assign(const ObStoreInfo& info);
  int assign(const ObSavedStorageInfo& info, const int64_t sstable_count);
  const ObSavedStorageInfo& get_saved_info() const
  {
    return saved_storage_info_;
  }
  int64_t get_sstable_count() const
  {
    return sstable_count_;
  }
  TO_STRING_KV(K_(saved_storage_info), K_(sstable_count));

  OB_UNIS_VERSION(1);

private:
  ObSavedStorageInfo saved_storage_info_;
  int64_t sstable_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStoreInfo);
};

}  // namespace storage

namespace obrpc {

struct ObMigrateInfoFetchArg {
  common::ObPartitionKey pkey_;

  ObMigrateInfoFetchArg() : pkey_()
  {}
  explicit ObMigrateInfoFetchArg(const common::ObPartitionKey& pkey) : pkey_(pkey)
  {}
  TO_STRING_KV(K_(pkey));
  OB_UNIS_VERSION(1);
};

struct ObMigrateStoreInfo {
  storage::ObStoreInfo store_info_;
  common::ObAddr server_;

  int assign(const ObMigrateStoreInfo& info);
  void reset()
  {
    store_info_.reset();
    server_.reset();
  }
  ObVersion get_version() const
  {
    return store_info_.get_saved_info().get_frozen_version();
  }
  int64_t get_sstable_count() const
  {
    return store_info_.get_sstable_count();
  }
  TO_STRING_KV(K_(store_info), K_(server));
  OB_UNIS_VERSION(1);
};

struct ObMigrateInfoFetchResult {
  ObSArray<ObMigrateStoreInfo> info_list_;
  common::ObReplicaType replica_type_;

  ObMigrateInfoFetchResult();
  void reset();

  const ObMigrateStoreInfo* left_migrate_info() const;
  const ObMigrateStoreInfo* right_migrate_info() const;
  uint64_t get_last_replay_log_id() const;
  int add_store_info(const ObMigrateStoreInfo& info);
  int add_store_info(const storage::ObStoreInfo& info, const common::ObAddr server);
  bool is_continues() const;
  inline void dump_simple_string(const char* msg) const;
  TO_STRING_KV(K_(info_list));
  OB_UNIS_VERSION(2);
};

struct ObFetchBaseDataMetaArg {
  OB_UNIS_VERSION(1);

public:
  ObFetchBaseDataMetaArg() : pkey_(), version_(0), store_type_(storage::INVALID_STORE_TYPE)
  {}
  TO_STRING_KV(K_(pkey), K_(version), K_(store_type));

  common::ObPartitionKey pkey_;
  common::ObVersion version_;
  storage::ObStoreType store_type_;
};

struct ObFetchMacroBlockOldArg {
  OB_UNIS_VERSION(1);

public:
  ObFetchMacroBlockOldArg() : index_id_(0), macro_block_index_(0), data_version_(0), data_seq_(0)
  {}
  void reset();
  bool operator==(const ObFetchMacroBlockOldArg& arg) const;
  int64_t hash() const;
  TO_STRING_KV(K_(index_id), K_(macro_block_index), K_(data_version), K_(data_seq));

  uint64_t index_id_;
  int64_t macro_block_index_;  // the index of the macro block in the sstable macro block array
  common::ObVersion data_version_;
  int64_t data_seq_;
};

struct ObFetchMacroBlockListOldArg {
  OB_UNIS_VERSION(1);

public:
  ObFetchMacroBlockListOldArg();
  TO_STRING_KV(K_(pkey), K_(data_version), K(store_type_), "arg_count", arg_list_.count());
  common::ObPartitionKey pkey_;
  common::ObVersion data_version_;
  common::ObSArray<ObFetchMacroBlockOldArg> arg_list_;
  storage::ObStoreType store_type_;
};

}  // namespace obrpc
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_PARTITION_MIGRAT_OLD_RPC_H_ */
