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

#ifndef OCEANBASE_SHARE_OB_TABLET_AUTOINCREMENT_PARAM_H_
#define OCEANBASE_SHARE_OB_TABLET_AUTOINCREMENT_PARAM_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/hash_func/murmur_hash.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_tablet_id.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"

namespace oceanbase
{
namespace share
{

static const uint64_t DEFAULT_HANDLE_CACHE_SIZE = 10;
static const uint64_t DEFAULT_TABLET_INCREMENT_CACHE_SIZE = 10000;       // 1w

struct ObTabletAutoincKey final
{
public:
  ObTabletAutoincKey() : tenant_id_(0), tablet_id_(0) {}
  void reset()
  {
    tenant_id_ = 0;
    tablet_id_.reset();
  }
  bool operator==(const ObTabletAutoincKey &other) const
  {
    return other.tenant_id_    == tenant_id_
           && other.tablet_id_  == tablet_id_;
  }

  int compare(const ObTabletAutoincKey &other) {
    int ret = OB_SUCCESS;
    if (tenant_id_ < other.tenant_id_) {
      ret = -1;
    } else if (tenant_id_ > other.tenant_id_) {
      ret = 1;
    } else {
      ret = tablet_id_.compare(other.tablet_id_);
    }
    return ret;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = tablet_id_.hash();
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    return hash_val;
  }

  inline bool is_valid() const { return tenant_id_ != 0 && tablet_id_.is_valid(); }

  TO_STRING_KV(K_(tenant_id), K_(tablet_id));
public:
  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;
};

struct ObTabletAutoincInterval final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletAutoincInterval()
    : tablet_id_(), start_(0), end_(0) {}
  bool is_valid() const { return tablet_id_.is_valid() && end_ >= start_; }
  void reset()
  {
    tablet_id_.reset();
    start_ = 0;
    end_ = 0;
  }
  TO_STRING_KV(K_(tablet_id), K_(start), K_(end));
public:
  common::ObTabletID tablet_id_;
  // interval range is [start, end]
  uint64_t start_;
  uint64_t end_;
};

struct ObTabletCacheInterval final
{
public:
  ObTabletCacheInterval()
  : tablet_id_(OB_INVALID_ID), cache_size_(0), task_id_(-1), next_value_(0), start_(0), end_(0)
  {}
  ObTabletCacheInterval(common::ObTabletID tablet_id, uint64_t cache_size)
  : tablet_id_(tablet_id), cache_size_(cache_size), task_id_(-1), next_value_(0), start_(0), end_(0)
  {}
  ~ObTabletCacheInterval() {}

  TO_STRING_KV(K_(tablet_id), K_(start), K_(end), K_(cache_size), K_(next_value), K_(task_id));
  void set(uint64_t start, uint64_t end);
  int next_value(uint64_t &next_value);
  int fetch(uint64_t count, ObTabletCacheInterval &dest);
  uint64_t count() const { return end_ - start_ + 1; }
  bool operator <(const ObTabletCacheInterval &other) { return tablet_id_ < other.tablet_id_; }
public:
  common::ObTabletID tablet_id_;
  uint64_t cache_size_;
  int64_t task_id_;
private:
  uint64_t next_value_;
  uint64_t start_;
  uint64_t end_;
};

struct ObTabletAutoincParam final
{
public:
  ObTabletAutoincParam()
    : tenant_id_(OB_INVALID_ID),
      auto_increment_cache_size_(DEFAULT_TABLET_INCREMENT_CACHE_SIZE)
  {}
  bool is_valid() const
  {
    return OB_INVALID_ID != tenant_id_ && auto_increment_cache_size_ > 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(auto_increment_cache_size));
public:
  uint64_t tenant_id_;
  int64_t auto_increment_cache_size_; // how many tablet seqs to cache on one tablet node
  OB_UNIS_VERSION(1);
};

struct ObMigrateTabletAutoincSeqParam final
{
  OB_UNIS_VERSION(1);
public:
  ObMigrateTabletAutoincSeqParam()
    : src_tablet_id_(), dest_tablet_id_(), ret_code_(OB_SUCCESS), autoinc_seq_(0)
  {}
  bool is_valid() const { return src_tablet_id_.is_valid(); }
  TO_STRING_KV(K_(src_tablet_id), K_(dest_tablet_id), K_(ret_code), K_(autoinc_seq));
public:
  common::ObTabletID src_tablet_id_;
  common::ObTabletID dest_tablet_id_;
  int ret_code_;
  uint64_t autoinc_seq_;
};

class ObTabletAutoincSeq : public memtable::ObIMultiSourceDataUnit, public storage::ObIStorageMetaObj
{
public:
  //friend class ObTabletAutoincSeqMdsUserData;
  const int32_t AUTOINC_SEQ_VERSION = 1;
public:
  ObTabletAutoincSeq();
  ~ObTabletAutoincSeq();

  int assign(common::ObIAllocator &allocator, const ObTabletAutoincSeq &other);
  // Only for old multi_data_source
  // Free origin data with allocator and alloc new.
  // TODO yq: remove later
  virtual int deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator) override;
  virtual int deep_copy(
      char *dst_buf,
      const int64_t buf_size,
      storage::ObIStorageMetaObj *&value) const;
  virtual int64_t get_deep_copy_size() const { return sizeof(ObTabletAutoincSeq) + sizeof(ObTabletAutoincInterval) * intervals_count_; }
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual inline int64_t get_data_size() const override { return get_deep_copy_size(); }
  virtual inline memtable::MultiSourceDataUnitType type() const override
  {
    return memtable::MultiSourceDataUnitType::TABLET_SEQ;
  }
  int get_autoinc_seq_value(uint64_t &autoinc_seq);
  int set_autoinc_seq_value(
    common::ObArenaAllocator &allocator,
    const uint64_t autoinc_seq);
  const share::ObTabletAutoincInterval* get_intervals() const { return intervals_; }
  int64_t get_intervals_count() const { return intervals_count_; }

  int serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const;
  // multi source deserialize with ObIAllocator, will set allocator_
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(version), K_(intervals_count), KPC_(intervals));
private:
  int deserialize_(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int serialize_(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const;
  int64_t get_serialize_size_(void) const;
private:
  int64_t version_;
  ObIAllocator *allocator_;
  share::ObTabletAutoincInterval *intervals_;
  int64_t intervals_count_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletAutoincSeq);
};

}//end namespace share
}//end namespace oceanbase
#endif
