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

#ifndef OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H
#define OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H

#include "lib/container/ob_array.h"           // ObArray
#include "lib/allocator/ob_allocator.h"       // ObIAllocator

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
}
}

namespace rootserver
{

class ObTransferPartGroup;
struct ObBalanceGroupID;
class ObRRPartGroupContainer;
class ObSimplePartGroupContainer;

class ObIPartGroupInfo
{
public:
  friend class ObSimplePartGroupContainer;
  friend class ObRRPartGroupContainer;

  virtual ~ObIPartGroupInfo() { part_group_ = nullptr; is_inited_ = false; }
  ObTransferPartGroup *part_group() const { return part_group_; }
  virtual void reset() { part_group_ = nullptr; is_inited_ = false; }
  virtual bool is_valid() const { return is_inited_; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(part_group));
protected:
  ObIPartGroupInfo() : is_inited_(false), part_group_(nullptr) {}
  virtual int append_(ObRRPartGroupContainer &pg_container) const { return OB_NOT_IMPLEMENT; }
  virtual int append_(ObSimplePartGroupContainer &pg_container) const { return OB_NOT_IMPLEMENT; }
  virtual int select_(const ObRRPartGroupContainer &src_pgs,
                      ObRRPartGroupContainer &dst_pgs) { return OB_NOT_IMPLEMENT; }
  virtual int select_(const ObSimplePartGroupContainer &src_pgs,
                      const ObSimplePartGroupContainer &dst_pgs) { return OB_NOT_IMPLEMENT; }
  virtual int remove_(ObRRPartGroupContainer &pg_container) const { return OB_NOT_IMPLEMENT; }
  virtual int remove_(ObSimplePartGroupContainer &pg_container) const { return OB_NOT_IMPLEMENT; }
  virtual int get_largest_(const ObRRPartGroupContainer &pg_container) { return OB_NOT_IMPLEMENT; }
  virtual int get_largest_(const ObSimplePartGroupContainer &pg_container) { return OB_NOT_IMPLEMENT; }
  virtual int get_smallest_(const ObRRPartGroupContainer &pg_container) { return OB_NOT_IMPLEMENT; }
  virtual int get_smallest_(const ObSimplePartGroupContainer &pg_container) { return OB_NOT_IMPLEMENT; }

  bool is_inited_;
  ObTransferPartGroup *part_group_;
};

class ObIPartGroupContainer
{
public:
  explicit ObIPartGroupContainer(ObIAllocator &alloc) : is_inited_(false), alloc_(alloc) {}
  virtual ~ObIPartGroupContainer() { is_inited_ = false; };
  virtual int append(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const uint64_t part_group_uid,
      ObTransferPartGroup *const part_group) = 0;
  virtual int append(const ObIPartGroupInfo &pg_info) = 0;
  virtual int remove(const ObIPartGroupInfo &pg_info) = 0;
  virtual int select(ObIPartGroupContainer &dst_pg, ObIPartGroupInfo *&pg_info) const = 0;
  virtual int get_largest(ObIPartGroupInfo *&pg_info) const = 0;
  virtual int get_smallest(ObIPartGroupInfo *&pg_info) const = 0;
  virtual int64_t count() const = 0;
  virtual bool is_valid() const { return is_inited_; }
  VIRTUAL_TO_STRING_KV(K_(is_inited));
protected:
  bool is_inited_;
  ObIAllocator &alloc_;
};

class ObSimplePartGroupInfo final: public ObIPartGroupInfo
{
public:
  friend class ObSimplePartGroupContainer;

  ~ObSimplePartGroupInfo() override { pg_idx_ = OB_INVALID_INDEX; };
  void reset() override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(part_group), K_(pg_idx));
private:
  ObSimplePartGroupInfo() : pg_idx_(OB_INVALID_INDEX) {}
  int init_(const int64_t pg_idx, ObTransferPartGroup *const part_group);
  int remove_(ObSimplePartGroupContainer &pg_container) const override;
  int append_(ObSimplePartGroupContainer &pg_container) const override;
  int select_(const ObSimplePartGroupContainer &src_pgs,
              const ObSimplePartGroupContainer &dst_pgs) override;
  int get_largest_(const ObSimplePartGroupContainer &pg_container) override;
  int get_smallest_(const ObSimplePartGroupContainer &pg_container) override;

  int64_t pg_idx_;
};

class ObSimplePartGroupContainer final: public ObIPartGroupContainer
{
public:
  friend class ObSimplePartGroupInfo;

  ObSimplePartGroupContainer(ObIAllocator &alloc) :
    ObIPartGroupContainer(alloc),
    part_groups_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "PartGroups")) {}
  ~ObSimplePartGroupContainer() override;
  int init() { is_inited_ = true; return OB_SUCCESS; }
  int append(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const uint64_t part_group_uid,
      ObTransferPartGroup *const part_group) override;
  int append(const ObIPartGroupInfo &pg_info) override;
  int select(ObIPartGroupContainer &dst_pg, ObIPartGroupInfo *&pg_info) const override;
  int remove(const ObIPartGroupInfo &pg_info) override;
  int get_largest(ObIPartGroupInfo *&pg_info) const override;
  int get_smallest(ObIPartGroupInfo *&pg_info) const override;
  int64_t count() const override { return part_groups_.count(); }
  TO_STRING_KV(K_(is_inited), K_(part_groups));
private:
  ObArray<ObTransferPartGroup*> part_groups_;
};

typedef common::ObArray<ObTransferPartGroup *> ObPartGroupBucket;

class ObBalanceGroupUnit
{
public:
  explicit ObBalanceGroupUnit(common::ObIAllocator &alloc) :
      inited_(false),
      alloc_(alloc),
      part_group_buckets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "PGBuckets")),
      part_group_cnt_(0) {}
  ~ObBalanceGroupUnit();
  int init(const int64_t bucket_num);
  bool is_valid() const { return inited_; }
  int64_t get_part_group_count() const { return part_group_cnt_; }
  int append_part_group(const uint64_t part_group_uid, ObTransferPartGroup *const part_group);
  int append_part_group_into_bucket(
      const int64_t bucket_idx,
      ObTransferPartGroup *const part_group);
  int remove_part_group(const int64_t bucket_idx, const int64_t pg_idx);
  int get_transfer_out_part_group(
      const ObBalanceGroupUnit &dst_unit,
      int64_t &bucket_idx,
      int64_t &pg_idx,
      ObTransferPartGroup *&part_group) const;
  int get_largest_part_group(
      int64_t &bucket_idx,
      int64_t &pg_idx,
      ObTransferPartGroup *&part_group) const;
  int get_smallest_part_group(
      int64_t &bucket_idx,
      int64_t &pg_idx,
      ObTransferPartGroup *&part_group) const;

  TO_STRING_KV("part_group_count", part_group_cnt_, K_(part_group_buckets));

private:
  int get_transfer_out_bucket_(const ObBalanceGroupUnit &dst_unit, int64_t &bucket_idx) const;

private:
  bool inited_;
  ObIAllocator &alloc_;
  common::ObArray<ObPartGroupBucket> part_group_buckets_;
  int64_t part_group_cnt_;
};

class ObRRPartGroupInfo final: public ObIPartGroupInfo
{
public:
  friend class ObRRPartGroupContainer;

  ~ObRRPartGroupInfo() override;
  void reset() override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(part_group), K_(bg_unit_id), K_(bucket_idx), K_(pg_idx));
private:
  ObRRPartGroupInfo() :
      bg_unit_id_(OB_INVALID_ID),
      bucket_idx_(OB_INVALID_INDEX),
      pg_idx_(OB_INVALID_INDEX) {}
  int init_(const ObObjectID &bg_unit_id, const int64_t bucket_idx, const int64_t pg_idx,
            ObTransferPartGroup *const part_group);
  int remove_(ObRRPartGroupContainer &pg_container) const override;
  int append_(ObRRPartGroupContainer &pg_container) const override;
  int select_(const ObRRPartGroupContainer &src_pgs,
              ObRRPartGroupContainer &dst_pgs) override;
  int get_largest_(const ObRRPartGroupContainer &pg_container) override;
  int get_smallest_(const ObRRPartGroupContainer &pg_container) override;

  ObObjectID bg_unit_id_;
  int64_t bucket_idx_;
  int64_t pg_idx_;
};

class ObRRPartGroupContainer final: public ObIPartGroupContainer
{
public:
  friend class ObRRPartGroupInfo;

  ObRRPartGroupContainer(ObIAllocator &alloc) :
      ObIPartGroupContainer(alloc),
      bg_units_(),
      part_group_cnt_(0),
      bucket_num_(0) {}
  ~ObRRPartGroupContainer() override;
  int init(const ObBalanceGroupID &bg_id, const int64_t ls_num);
  int append(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const uint64_t part_group_uid,
      ObTransferPartGroup *const part_group) override;
  int append(const ObIPartGroupInfo &pg_info) override;
  int select(ObIPartGroupContainer &dst_pg, ObIPartGroupInfo *&pg_info) const override;
  int remove(const ObIPartGroupInfo &pg_info) override;
  int get_largest(ObIPartGroupInfo *&pg_info) const override;
  int get_smallest(ObIPartGroupInfo *&pg_info) const override;
  int64_t count() const override { return part_group_cnt_; }
  TO_STRING_KV(K_(is_inited), K_(part_group_cnt), K_(bucket_num));
private:
  int get_or_create_bg_unit_(const ObObjectID &bg_unit_id, ObBalanceGroupUnit *&bg_unit);
  int get_transfer_out_unit_(const ObRRPartGroupContainer &dst_pgs, ObObjectID &bg_unit_id) const;

  const int64_t MAP_BUCKET_NUM = 4096;

private:
  hash::ObHashMap<ObObjectID, ObBalanceGroupUnit*> bg_units_;
  int64_t part_group_cnt_;
  int64_t bucket_num_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H */
