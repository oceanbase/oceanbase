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

namespace oceanbase
{
namespace libobcdc
{
struct LobAuxMetaKey
{
  LobAuxMetaKey(
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t aux_lob_meta_tid,
      const common::ObLobId &lob_id,
      const int64_t seq_no) :
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
      && seq_no_ > 0;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&aux_lob_meta_tid_, sizeof(aux_lob_meta_tid_), hash_val);
    hash_val = common::murmurhash(&lob_id_, sizeof(lob_id_), hash_val);
    hash_val = common::murmurhash(&seq_no_, sizeof(seq_no_), hash_val);

    return hash_val;
  }

  int compare(const LobAuxMetaKey &other) const;
  bool operator>(const LobAuxMetaKey &other) const { return 1 == compare(other); }
  bool operator<(const LobAuxMetaKey &other) const { return -1 == compare(other); }
  bool operator==(const LobAuxMetaKey &other) const
  {
    return tenant_id_ == other.tenant_id_
      && trans_id_ == other.trans_id_
      && aux_lob_meta_tid_ == other.aux_lob_meta_tid_
      && lob_id_ == other.lob_id_
      && seq_no_ == other.seq_no_;
  }

  TO_STRING_KV(
      K_(tenant_id),
      K_(trans_id),
      "table_id", aux_lob_meta_tid_,
      K_(lob_id),
      K_(seq_no));

  uint64_t tenant_id_;
  transaction::ObTransID trans_id_;
  uint64_t aux_lob_meta_tid_;
  common::ObLobId lob_id_;
  int64_t seq_no_;
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

class ObCDCLobAuxMetaStorager
{
public:
  ObCDCLobAuxMetaStorager();
  ~ObCDCLobAuxMetaStorager();
  int init();
  void destroy();

  int put(
      const LobAuxMetaKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);

  int get(
      const LobAuxMetaKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);

  int del(
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);

  int del(
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id);

private:
  int del_lob_col_value_(
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t aux_lob_meta_tid,
      ObLobDataGetCtx &lob_data_get_ctx,
      volatile bool &stop_flag);

  struct LobAuxMetaDataPurger
  {
    LobAuxMetaDataPurger(
        ObCDCLobAuxMetaStorager &host,
        const uint64_t tenant_id,
        const transaction::ObTransID &trans_id) :
      host_(host),
      tenant_id_(tenant_id),
      trans_id_(trans_id),
      purge_count_(0)
    {}

    bool operator()(const LobAuxMetaKey &lob_aux_meta_key, LobAuxMetaValue &lob_aux_meta_value);

    ObCDCLobAuxMetaStorager &host_;
    uint64_t tenant_id_;
    transaction::ObTransID trans_id_;
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
  static const int64_t LOB_AUX_META_ALLOCATOR_PAGE_SIZE = 2 * _M_;

  bool is_inited_;
  LobAuxMetaMap lob_aux_meta_map_;
  LobAuxMetaAllocator lob_aux_meta_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCLobAuxMetaStorager);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
