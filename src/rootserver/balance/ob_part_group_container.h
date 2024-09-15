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

#include "lib/container/ob_array.h"   // ObArray
#include "lib/hash/ob_hashmap.h"      // ObHashmap

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

// the partititon distrbution mode for LS balance and partition balance
// continuous: partitions will be distributed continuously with best effort
// round_robin: partitions will be distributed round robin with best effort
enum class ObPartDistributionMode {
  INVALID,
  CONTINUOUS,
  ROUND_ROBIN,
  // add new distribution mode here
  MAX
};

// interface of part group info
class ObIPartGroupInfo
{
public:
  virtual ~ObIPartGroupInfo() { part_group_ = nullptr; is_inited_ = false; }
  const ObPartDistributionMode &get_part_distribution_mode() const { return part_distribution_mode_; }
  ObTransferPartGroup *part_group() const { return part_group_; }
  virtual void reset() { part_group_ = nullptr; is_inited_ = false; }
  virtual bool is_valid() const { return is_inited_; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(part_group));
protected:
  ObIPartGroupInfo(const ObPartDistributionMode &distr_mode) :
      is_inited_(false),
      part_distribution_mode_(distr_mode),
      part_group_(nullptr) {}

  bool is_inited_;
  ObPartDistributionMode part_distribution_mode_;
  ObTransferPartGroup *part_group_;
};

// interface of part group container
class ObIPartGroupContainer
{
public:
  explicit ObIPartGroupContainer(
      ObIAllocator &alloc,
      const ObPartDistributionMode part_distribution_mode) :
        is_inited_(false),
        part_distribution_mode_(part_distribution_mode),
        alloc_(alloc) {}
  virtual ~ObIPartGroupContainer() { is_inited_ = false; };
  const ObPartDistributionMode &get_part_distribution_mode() const
      { return part_distribution_mode_; }
  virtual int append_new_part_group(
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
  ObPartDistributionMode part_distribution_mode_;
  ObIAllocator &alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIPartGroupContainer);
};

// the part group info corresponding to ObContinuousPartGroupContainer
class ObContinuousPartGroupInfo final: public ObIPartGroupInfo
{
public:
  friend class ObContinuousPartGroupContainer;

  ~ObContinuousPartGroupInfo() override { pg_idx_ = OB_INVALID_INDEX; };
  void reset() override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(part_group), K_(pg_idx));
private:
  ObContinuousPartGroupInfo() :
      ObIPartGroupInfo(ObPartDistributionMode::CONTINUOUS),
      pg_idx_(OB_INVALID_INDEX) {}
  int init_(const int64_t pg_idx, ObTransferPartGroup *const part_group);
  DISALLOW_COPY_AND_ASSIGN(ObContinuousPartGroupInfo);

  int64_t pg_idx_;
};

// the part group container for continuous partition distribution
class ObContinuousPartGroupContainer final: public ObIPartGroupContainer
{
public:
  ObContinuousPartGroupContainer(ObIAllocator &alloc) :
    ObIPartGroupContainer(alloc, ObPartDistributionMode::CONTINUOUS),
    part_groups_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "PartGroups")) {}
  ~ObContinuousPartGroupContainer() override;
  int init() { is_inited_ = true; return OB_SUCCESS; }
  int append_new_part_group(
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
  int create_part_group_info_if_needed_(
      ObIPartGroupInfo *&pg_info,
      ObContinuousPartGroupInfo *&pg_info_continuous) const;
  DISALLOW_COPY_AND_ASSIGN(ObContinuousPartGroupContainer);

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

// the part group info corresponding to ObRRPartGroupContainer
class ObRRPartGroupInfo final: public ObIPartGroupInfo
{
public:
  friend class ObRRPartGroupContainer;

  ~ObRRPartGroupInfo() override;
  void reset() override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(part_group), K_(bg_unit_id), K_(bucket_idx), K_(pg_idx));
private:
  ObRRPartGroupInfo() :
      ObIPartGroupInfo(ObPartDistributionMode::ROUND_ROBIN),
      bg_unit_id_(OB_INVALID_ID),
      bucket_idx_(OB_INVALID_INDEX),
      pg_idx_(OB_INVALID_INDEX) {}
  int init_(const ObObjectID &bg_unit_id, const int64_t bucket_idx, const int64_t pg_idx,
            ObTransferPartGroup *const part_group);
  DISALLOW_COPY_AND_ASSIGN(ObRRPartGroupInfo);

  ObObjectID bg_unit_id_;
  int64_t bucket_idx_;
  int64_t pg_idx_;
};

// the part group container for round robin partitition distribution
class ObRRPartGroupContainer final: public ObIPartGroupContainer
{
public:
  ObRRPartGroupContainer(ObIAllocator &alloc) :
      ObIPartGroupContainer(alloc, ObPartDistributionMode::ROUND_ROBIN),
      bg_units_(),
      part_group_cnt_(0),
      bucket_num_(0) {}
  ~ObRRPartGroupContainer() override;
  int init(const ObBalanceGroupID &bg_id, const int64_t ls_num);
  int append_new_part_group(
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
  int create_part_group_info_if_needed_(
      ObIPartGroupInfo *&pg_info,
      ObRRPartGroupInfo *&pg_info_rr) const;
  DISALLOW_COPY_AND_ASSIGN(ObRRPartGroupContainer);

  const int64_t MAP_BUCKET_NUM = 4096;

private:
  hash::ObHashMap<ObObjectID, ObBalanceGroupUnit *> bg_units_;
  int64_t part_group_cnt_;
  int64_t bucket_num_;
};

// When a new partition distribution mode is needed, declare the corresponding
// ObModePartGroupInfo and ObModePartGroupContainer, which inherit from
// ObIPartGroupInfo and ObIPartGroupContainer, respectively.

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H */
