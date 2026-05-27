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
 *
 * Persistent storage for serialized DELETE payload in update-split-merge flow.
 * Thin wrapper over IObStoreService (RocksDB lob_storage_cf_handle).
 * RocksDB's own memtable already provides in-process buffering for short-lived
 * entries, so we intentionally do NOT add an additional in-memory cache layer
 * here — doing so would double-count memory against the CDC process without
 * reducing pressure on PartTransTask::allocator (the real target of offloading).
 */

#ifndef OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_STORAGER_H_
#define OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_STORAGER_H_

#include "storage/tx/ob_tx_log.h"    // ObTransID

namespace oceanbase
{
namespace libobcdc
{

struct UpdateSplitMergeStatInfo;

struct UpdateSplitMergeKey
{
  UpdateSplitMergeKey() :
    tenant_id_(common::OB_INVALID_TENANT_ID),
    commit_version_(0),
    trans_id_(),
    trace_id_raw_(0)
  {}

  UpdateSplitMergeKey(
      const uint64_t tenant_id,
      const int64_t commit_version,
      const transaction::ObTransID &trans_id,
      const int64_t trace_id_raw) :
    tenant_id_(tenant_id),
    commit_version_(commit_version),
    trans_id_(trans_id),
    trace_id_raw_(trace_id_raw)
  {}

  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && trace_id_raw_ > 0;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&commit_version_, sizeof(commit_version_), hash_val);
    uint64_t trans_id_hash = trans_id_.hash();
    hash_val = common::murmurhash(&trans_id_hash, sizeof(trans_id_hash), hash_val);
    hash_val = common::murmurhash(&trace_id_raw_, sizeof(trace_id_raw_), hash_val);
    return hash_val;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool operator==(const UpdateSplitMergeKey &other) const
  {
    return tenant_id_ == other.tenant_id_
      && commit_version_ == other.commit_version_
      && trans_id_ == other.trans_id_
      && trace_id_raw_ == other.trace_id_raw_;
  }

  int get_key(std::string &key) const;

  TO_STRING_KV(K_(tenant_id), K_(commit_version), K_(trans_id), K_(trace_id_raw));

  uint64_t tenant_id_;
  int64_t commit_version_;
  transaction::ObTransID trans_id_;
  int64_t trace_id_raw_;
};

class IObStoreService;

class ObCDCUpdateSplitMergeStorager
{
public:
  ObCDCUpdateSplitMergeStorager();
  ~ObCDCUpdateSplitMergeStorager();
  int init(IObStoreService *store_service, UpdateSplitMergeStatInfo &stat);
  void destroy();

  int put(const UpdateSplitMergeKey &key, const char *data, const int64_t data_len);
  int get(ObIAllocator &allocator, const UpdateSplitMergeKey &key, const char *&data, int64_t &data_len);
  int del(const UpdateSplitMergeKey &key);

private:
  int get_cf_handle_(const uint64_t tenant_id, void *&cf_handle);

private:
  bool is_inited_;
  IObStoreService *store_service_;
  UpdateSplitMergeStatInfo *stat_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCUpdateSplitMergeStorager);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
