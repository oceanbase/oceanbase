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

#ifndef OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_DEFINE_H_
#define OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_DEFINE_H_
#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/transfer/ob_transfer_info.h"  // ObTransferPartInfo, ObTransferPartList

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObSimpleTablegroupSchema;
class ObSimpleTableSchemaV2;
class ObPartition;
}
}

namespace rootserver
{
typedef common::ObFixedLengthString<
        common::OB_MAX_BALANCE_GROUP_NAME_LENGTH>
        ObBalanceGroupName;

struct ObBalanceGroupID
{
public:
  ObBalanceGroupID() : id_high_(common::OB_INVALID_ID),
                     id_low_(common::OB_INVALID_ID) {}
  ObBalanceGroupID(const uint64_t id_high,
                 const uint64_t id_low)
    : id_high_(id_high),
      id_low_(id_low) {}

  int hash(uint64_t &res) const {
    res = 0;
    res = murmurhash(&id_high_, sizeof(id_high_), res);
    res = murmurhash(&id_low_, sizeof(id_low_), res);
    return common::OB_SUCCESS;
  }
  bool operator ==(const ObBalanceGroupID &other) const {
    return id_high_ == other.id_high_ &&
      id_low_ == other.id_low_;
  }
  bool operator !=(const ObBalanceGroupID &other) const {
    return !(*this == other);
  }

  bool is_non_part_table_bg() const { return NON_PART_BG_ID == *this; }
  bool is_sharding_none_tablegroup_bg() const { return SHARDING_NONE_TABLEGROUP_BG_ID == *this; }

  bool is_valid() const {
    return OB_INVALID_ID != id_high_
           && OB_INVALID_ID != id_low_;
  }

  void reset() {
    id_high_ = common::OB_INVALID_ID;
    id_low_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(K_(id_high), K_(id_low));
public:
  static const ObBalanceGroupID NON_PART_BG_ID;
  static const ObBalanceGroupID DUP_TABLE_BG_ID;
  static const ObBalanceGroupID SHARDING_NONE_TABLEGROUP_BG_ID;

  uint64_t id_high_;
  uint64_t id_low_;
};

class ObBalanceGroup
{
public:
  ObBalanceGroup() : id_(), name_() {}
  ~ObBalanceGroup() {}

  const ObBalanceGroupID &id() const { return id_; }
  const ObBalanceGroupName &name() const { return name_; }

  int init_by_tablegroup(const share::schema::ObSimpleTablegroupSchema &tg,
      const int64_t max_part_level,
      const int64_t part_group_index = 0);
  int init_by_table(const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const share::schema::ObPartition *partition = NULL);

  int hash(uint64_t &res) const;
  bool operator ==(const ObBalanceGroup &other) const {
    return id_ == other.id_;
  }
  bool operator !=(const ObBalanceGroup &other) const {
    return !(*this == other);
  }
  bool is_non_part_table_bg() const { return id_.is_non_part_table_bg(); }
  bool is_valid() const { return id_.is_valid() && !name_.is_empty(); }

  TO_STRING_KV(K_(id), K_(name));
public:
  const static char* NON_PART_BG_NAME;
  const static char* DUP_TABLE_BG_NAME;
  const static char* SHARDING_NONE_TABLEGROUP_BG_NAME;
private:
  ObBalanceGroupID id_;
  ObBalanceGroupName name_;
  // TODO: add type
};


// A group of partitions that should be distributed on the same LS and transfered together
class ObPartGroupInfo
{
public:
  ObPartGroupInfo() :
      part_group_uid_(OB_INVALID_ID),
      bg_unit_id_(OB_INVALID_ID),
      data_size_(0),
      weight_(0),
      part_list_("PartGroup") {}

  ObPartGroupInfo(common::ObIAllocator &alloc) :
      part_group_uid_(OB_INVALID_ID),
      bg_unit_id_(OB_INVALID_ID),
      data_size_(0),
      weight_(0),
      part_list_(alloc, "PartGroup") {}

  ~ObPartGroupInfo() {
    part_group_uid_ = OB_INVALID_ID;
    bg_unit_id_ = OB_INVALID_ID;
    data_size_ = 0;
    weight_ = 0;
    part_list_.reset();
  }
  bool is_valid()
  {
    return is_valid_id(part_group_uid_)
        && is_valid_id(bg_unit_id_);
  }
  bool is_same_pg(const uint64_t part_group_uid) { return part_group_uid == part_group_uid_; }
  uint64_t get_part_group_uid() const { return part_group_uid_; }
  uint64_t get_bg_unit_id() const { return bg_unit_id_; }
  int64_t get_data_size() const { return data_size_; }
  int64_t get_weight() const { return weight_; }
  const share::ObTransferPartList &get_part_list() const { return part_list_; }
  int64_t count() const { return part_list_.count(); }
  int assign(const ObPartGroupInfo &other);
  int init(const uint64_t part_group_uid, const uint64_t bg_unit_id);
  // add new partition into partition group
  int add_part(
      const share::ObTransferPartInfo &part,
      const int64_t data_size,
      const int64_t balance_weight);
  // less by weight
  static bool weight_cmp(const ObPartGroupInfo *left, const ObPartGroupInfo *right)
  {
    bool bret = false;
    if (OB_NOT_NULL(left) && OB_NOT_NULL(right)) {
      if (left->get_weight() < right->get_weight()) {
        bret = true;
      }
    }
    return bret;
  }
  TO_STRING_KV(K_(part_group_uid), K_(bg_unit_id), K_(data_size), K_(weight), K_(part_list));
private:
  uint64_t part_group_uid_;
  uint64_t bg_unit_id_; // database id or tablegroup id
  int64_t data_size_;
  int64_t weight_;
  share::ObTransferPartList part_list_;
};

}//end namespace rootserver
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_DEFINE_H_
