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

#ifndef OCEANBASE_TRANSACTION_OB_CLOG_ENCRYPT_INFO_
#define OCEANBASE_TRANSACTION_OB_CLOG_ENCRYPT_INFO_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_trans_define.h"
#include "share/ob_encryption_util.h"
#include "share/ob_encryption_struct.h"


namespace oceanbase
{
using namespace common;

namespace transaction
{

struct ObPGPartitionEncryptMeta
{
  share::ObEncryptMeta meta_;
  TO_STRING_KV(K_(meta));
  bool is_valid() const
  {
    return meta_.is_valid();
  }
};

typedef hash::ObHashMap<uint64_t, share::ObEncryptMeta> ObEncryptMap;

class ObCLogEncryptInfo
{
public:
  ObCLogEncryptInfo() : cached_encrypt_meta_(), encrypt_meta_(nullptr),
                        cached_table_id_(OB_INVALID_ID), is_inited_(false) {}
  ~ObCLogEncryptInfo() { destroy(); }
  int init();
  bool is_inited() const { return is_inited_; }
  virtual void destroy();
  bool is_valid() const;
  void reset();
  int get_encrypt_info(const uint64_t table_id, bool &need_encrypt, share::ObEncryptMeta &meta) const;
  int store(const ObPGPartitionEncryptMeta &meta);
  int store(uint64_t table_id, const share::ObEncryptMeta &meta);
  int add_clog_encrypt_info(const ObCLogEncryptInfo &rhs);
  ObEncryptMap* get_info() { return encrypt_meta_; }
  bool has_encrypt_meta() const;
  int replace_tenant_id(const uint64_t real_tenant_id);
  int decrypt_table_key();
  OB_UNIS_VERSION(1);
public:
private:
  int init_encrypt_meta_();
  int get_(const uint64_t table_id, share::ObEncryptMeta &meta) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCLogEncryptInfo);
private:
  mutable share::ObEncryptMeta cached_encrypt_meta_;
  ObEncryptMap *encrypt_meta_;
  mutable uint64_t cached_table_id_;
  bool is_inited_;
};

typedef ObSEArray<ObPGPartitionEncryptMeta, 4, TransModulePageAllocator> ObPGPartitionEncryptMetaArray;

struct ObSerializeEncryptMeta : public share::ObEncryptMeta
{
  ObSerializeEncryptMeta() : share::ObEncryptMeta() {}
  OB_UNIS_VERSION(1);
};
struct ObEncryptMetaCache
{
  int64_t table_id_;
  int64_t local_index_id_;
  ObSerializeEncryptMeta meta_;
  ObEncryptMetaCache() : table_id_(OB_INVALID_ID), local_index_id_(OB_INVALID_ID), meta_() {}
  inline int64_t real_table_id() const { return local_index_id_ != OB_INVALID_ID ? local_index_id_ : table_id_; }
  TO_STRING_KV(K_(meta), K_(table_id), K_(local_index_id));
  OB_UNIS_VERSION(1);
};

}//transaction
}//oceanbase

#endif
