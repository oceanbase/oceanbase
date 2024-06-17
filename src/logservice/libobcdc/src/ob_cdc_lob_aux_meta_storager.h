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

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_META_STORAGER_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_META_STORAGER_H_

#include "common/object/ob_object.h"        // ObLobId
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/allocator/ob_concurrent_fifo_allocator.h"    // ObConcurrentFIFOAllocator
#include "storage/tx/ob_tx_log.h"    // ObTransID
#include "ob_log_utils.h"            // _G_, _M_
#include "ob_cdc_lob_ctx.h"
#include "logservice/libobcdc/src/ob_log_resource_recycle_task.h"

namespace oceanbase
{
namespace libobcdc
{
struct LobAuxMetaKey
{
  LobAuxMetaKey(
      const int64_t commit_version,
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t aux_lob_meta_tid,
      const common::ObLobId &lob_id,
      const transaction::ObTxSEQ &seq_no) :
    commit_version_(commit_version),
    tenant_id_(tenant_id),
    trans_id_(trans_id),
    aux_lob_meta_tid_(aux_lob_meta_tid),
    lob_id_(lob_id),
    seq_no_(seq_no)
  {
  }

  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_
      && common::OB_INVALID_ID != aux_lob_meta_tid_
      && seq_no_.is_valid();
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&commit_version_, sizeof(commit_version_), hash_val);
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&aux_lob_meta_tid_, sizeof(aux_lob_meta_tid_), hash_val);
    hash_val = common::murmurhash(&lob_id_, sizeof(lob_id_), hash_val);
    hash_val = common::murmurhash(&seq_no_, sizeof(seq_no_), hash_val);

    return hash_val;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  int compare(const LobAuxMetaKey &other) const;
  bool operator>(const LobAuxMetaKey &other) const { return 1 == compare(other); }
  bool operator<(const LobAuxMetaKey &other) const { return -1 == compare(other); }
  bool operator==(const LobAuxMetaKey &other) const
  {
    return commit_version_ == other.commit_version_
      && tenant_id_ == other.tenant_id_
      && trans_id_ == other.trans_id_
      && aux_lob_meta_tid_ == other.aux_lob_meta_tid_
      && lob_id_ == other.lob_id_
      && seq_no_ == other.seq_no_;
  }

  int get_key(std::string &key) const;

  TO_STRING_KV(
      K_(commit_version),
      K_(tenant_id),
      K_(trans_id),
      "table_id", aux_lob_meta_tid_,
      K_(lob_id),
      K_(seq_no));

  int64_t commit_version_;
  uint64_t tenant_id_;
  transaction::ObTransID trans_id_;
  uint64_t aux_lob_meta_tid_;
  common::ObLobId lob_id_;
  transaction::ObTxSEQ seq_no_;
};

struct LobAuxMetaValue
{
  LobAuxMetaValue() :
      lob_data_(nullptr),
      lob_data_len_(0)
  {
  }

  LobAuxMetaValue(
      char *lob_data,
      const int64_t lob_data_len) :
    lob_data_(lob_data),
    lob_data_len_(lob_data_len)
  {
  }

  TO_STRING_KV(
      K_(lob_data_len));

  char *lob_data_;
  int64_t lob_data_len_;
};

class ObCDCLobAuxDataCleanTask : public ObLogResourceRecycleTask
{
public:
  ObCDCLobAuxDataCleanTask():
    ObLogResourceRecycleTask(ObLogResourceRecycleTask::LOB_DATA_CLEAN_TASK),
    tenant_id_(OB_INVALID_TENANT_ID),
    commit_version_(OB_INVALID_VERSION),
    is_task_push_(false)
  {}

  void reset();
  TO_STRING_KV(
      K_(tenant_id), K_(commit_version), K_(is_task_push));
public:
  uint64_t tenant_id_;
  int64_t commit_version_;
  bool is_task_push_;
};

struct ObCDCLobAuxMetaDataStatInfo
{
  ObCDCLobAuxMetaDataStatInfo() { reset(); }
  void reset()
  {
    last_stat_time_ = 0;
    get_req_cnt_ = 0;
    last_get_req_cnt_ = 0;
    mem_get_req_cnt_ = 0;
    last_mem_get_req_cnt_ = 0;
    put_req_cnt_ = 0;
    last_put_req_cnt_ = 0;
    mem_put_req_cnt_ = 0;
    last_mem_put_req_cnt_ = 0;
    get_data_total_size_ = 0;
    mem_get_data_total_size_ = 0;
    put_data_total_size_ = 0;
    mem_put_data_total_size_ = 0;
  }

  double calc_rps(const int64_t delta_time, const int64_t req_cnt, const int64_t last_req_cnt);
  double calc_rate(const int64_t delta_time, const int64_t total_size, const int64_t last_total_size);

  int64_t last_stat_time_;

  int64_t get_req_cnt_;
  int64_t last_get_req_cnt_;
  int64_t mem_get_req_cnt_;
  int64_t last_mem_get_req_cnt_;

  int64_t put_req_cnt_;
  int64_t last_put_req_cnt_;
  int64_t mem_put_req_cnt_;
  int64_t last_mem_put_req_cnt_;

  int64_t get_data_total_size_;
  int64_t last_get_data_total_size_;
  int64_t mem_get_data_total_size_;
  int64_t last_mem_get_data_total_size_;
  int64_t put_data_total_size_;
  int64_t last_put_data_total_size_;
  int64_t mem_put_data_total_size_;
  int64_t last_mem_put_data_total_size_;
};

class IObStoreService;
class ObLogConfig;
class ObCDCLobAuxMetaStorager
{
public:
  ObCDCLobAuxMetaStorager();
  ~ObCDCLobAuxMetaStorager();
  int init(IObStoreService *store_service);
  void destroy();

  int put(
      const LobAuxMetaKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);

  int get(
      ObIAllocator &allocator,
      const LobAuxMetaKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);

  int del(
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  int del(const LobAuxMetaKey &key);
  int get_clean_task(const uint64_t tenant_id, ObCDCLobAuxDataCleanTask *&task);
  int clean_unused_data(ObCDCLobAuxDataCleanTask *task);
  int get_cf_handle(const uint64_t tenant_id, void *&cf_handle);

  int64_t get_clean_task_interval() const { return clean_task_interval_; }
  void configure(const ObLogConfig &cfg);
  void print_stat_info();
private:
  int del_lob_col_value_(
      const int64_t commit_version,
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t aux_lob_meta_tid,
      ObLobDataGetCtx &lob_data_get_ctx,
      volatile bool &stop_flag);

  bool is_need_to_disk_(int64_t alloc_size);

  int memory_put_(
      const LobAuxMetaKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);
  int memory_get_(
      const LobAuxMetaKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);
  int memory_del_(const LobAuxMetaKey &key);
  int memory_del_(const uint64_t tenant_id, const int64_t commit_version);

  int disk_put_(
      const LobAuxMetaKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);
  int disk_get_(
      ObIAllocator &allocator,
      const LobAuxMetaKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);
  int disk_del_(const LobAuxMetaKey &key);
  int disk_del_(void* cf_family, const int64_t commit_version);

  int do_clean_unused_data_(ObCDCLobAuxDataCleanTask *task);

  struct LobAuxMetaDataPurger
  {
    LobAuxMetaDataPurger(
        ObCDCLobAuxMetaStorager &host,
        const uint64_t tenant_id,
        const int64_t commit_version) :
      host_(host),
      tenant_id_(tenant_id),
      commit_version_(commit_version),
      purge_count_(0)
    {}

    bool operator()(const LobAuxMetaKey &lob_aux_meta_key, LobAuxMetaValue &lob_aux_meta_value);

    ObCDCLobAuxMetaStorager &host_;
    uint64_t tenant_id_;
    int64_t commit_version_;
    int64_t purge_count_;
  };

  // For debug
  struct LobAuxMetaDataPrinter
  {
    bool operator()(const LobAuxMetaKey &lob_aux_meta_key, LobAuxMetaValue &lob_aux_meta_value);
  };

private:
  typedef common::ObLinearHashMap<LobAuxMetaKey, LobAuxMetaValue> LobAuxMetaMap;
  typedef common::ObConcurrentFIFOAllocator LobAuxMetaAllocator;
  static const int64_t LOB_AUX_META_ALLOCATOR_TOTAL_LIMIT = 1 * _G_;
  static const int64_t LOB_AUX_META_ALLOCATOR_HOLD_LIMIT = 32 * _M_;

  // The page size affects the memory hold by allocator.
  // If page size is OB_MALLOC_BIG_BLOCK_SIZE(2M), the memory hold by allocator is
  // eight times as much memory as it is actually used.
  // If page size is OB_MALLOC_NORMAL_BLOCK_SIZE(8K), the memory hold by allocator is
  // twice as much memory as it is actually used.
  // And the memory hold almost equal to what is actually used
  // when page size is OB_MALLOC_NORMAL_BLOCK_SIZE and one piece lob data is 256K.
  // So we use OB_MALLOC_NORMAL_BLOCK_SIZE as the page size
  static const int64_t LOB_AUX_META_ALLOCATOR_PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

  bool is_inited_;
  LobAuxMetaMap lob_aux_meta_map_;
  LobAuxMetaAllocator lob_aux_meta_allocator_;
  IObStoreService *store_service_;
  int64_t memory_alloced_;
  int64_t memory_limit_;
  bool enable_memory_;
  int64_t clean_task_interval_;

  ObCDCLobAuxMetaDataStatInfo stat_info_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCLobAuxMetaStorager);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
