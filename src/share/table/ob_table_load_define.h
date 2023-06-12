// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "common/ob_tablet_id.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ls_id.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace table
{

static const int64_t TABLE_LOAD_CTX_ID = common::ObCtxIds::WORK_AREA;

struct ObTableLoadFlag
{
  OB_UNIS_VERSION(1);
public:
  static const uint64_t BIT_IS_NEED_SORT = 1;
  static const uint64_t BIT_DATA_TYPE = 2;
  static const uint64_t BIT_DUP_ACTION_TYPE = 2;
  static const uint64_t BIT_RESERVED = 59;

  union {
    uint64_t flag_;
    struct {
      uint64_t is_need_sort_   : BIT_IS_NEED_SORT;
      uint64_t data_type_      : BIT_DATA_TYPE;
      uint64_t dup_action_     : BIT_DUP_ACTION_TYPE;
      uint64_t reserved_       : BIT_RESERVED;
    };
  };

  ObTableLoadFlag() : flag_(0) {}
  void reset() { flag_ = 0; }
  TO_STRING_KV(K_(is_need_sort), K_(data_type), K_(dup_action));
};

struct ObTableLoadConfig final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadConfig() : session_count_(0), batch_size_(0), max_error_row_count_(0) {}
  int32_t session_count_;
  int32_t batch_size_;
  uint64_t max_error_row_count_;
  ObTableLoadFlag flag_;

  TO_STRING_KV(K_(session_count), K_(batch_size), K_(max_error_row_count), K_(flag));
};

struct ObTableLoadPartitionId
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadPartitionId() : partition_id_(common::OB_INVALID_ID) {}
  ObTableLoadPartitionId(uint64_t partition_id, const common::ObTabletID &tablet_id)
    : partition_id_(partition_id), tablet_id_(tablet_id) {}
  ObTableLoadPartitionId(const ObTableLoadPartitionId &other)
    : partition_id_(other.partition_id_), tablet_id_(other.tablet_id_) {}
  uint64_t partition_id_;
  common::ObTabletID tablet_id_;
  bool is_valid() const
  {
    return common::OB_INVALID_ID != partition_id_ && tablet_id_.is_valid();
  }
  ObTableLoadPartitionId &operator=(const ObTableLoadPartitionId &other)
  {
    partition_id_ = other.partition_id_;
    tablet_id_ = other.tablet_id_;
    return *this;
  }
  bool operator==(const ObTableLoadPartitionId &other) const
  {
    return (partition_id_ == other.partition_id_ && tablet_id_ == other.tablet_id_);
  }
  bool operator!=(const ObTableLoadPartitionId &other) const
  {
    return !(*this == other);
  }
  bool operator<(const ObTableLoadPartitionId &other) const
  {
    return (partition_id_ != other.partition_id_ ? partition_id_ < other.partition_id_
                                                 : tablet_id_ < other.tablet_id_);
  }
  bool operator>(const ObTableLoadPartitionId &other) const
  {
    return (partition_id_ != other.partition_id_ ? partition_id_ > other.partition_id_
                                                 : tablet_id_ > other.tablet_id_);
  }
  bool operator<=(const ObTableLoadPartitionId &other) const
  {
    return !(*this > other);
  }
  bool operator>=(const ObTableLoadPartitionId &other) const
  {
    return !(*this < other);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = common::murmurhash(&partition_id_, sizeof(partition_id_), 0);
    hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
    return hash_val;
  }
  int compare(const ObTableLoadPartitionId &other) const
  {
    return (partition_id_ != other.partition_id_ ? static_cast<int32_t>(partition_id_ - other.partition_id_)//TODO(suzhi.yt): fix convert int64 to int32
                                                 : tablet_id_.compare(other.tablet_id_));
  }
  TO_STRING_KV(K_(partition_id), K_(tablet_id));
};

struct ObTableLoadLSIdAndPartitionId
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadLSIdAndPartitionId() {}
  ObTableLoadLSIdAndPartitionId(const share::ObLSID &ls_id, const ObTableLoadPartitionId &partition_id)
    : ls_id_(ls_id), part_tablet_id_(partition_id)
  {
  }
  share::ObLSID ls_id_;
  ObTableLoadPartitionId part_tablet_id_;

  ObTableLoadLSIdAndPartitionId &operator=(const ObTableLoadLSIdAndPartitionId &other)
  {
    ls_id_ = other.ls_id_;
    part_tablet_id_ = other.part_tablet_id_;
    return *this;
  }

  bool is_valid() const
  {
    return ls_id_.is_valid() && part_tablet_id_.is_valid();
  }

  TO_STRING_KV(K_(ls_id), K_(part_tablet_id));
};

struct ObTableLoadLSTabletID
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadLSTabletID() {}
  ObTableLoadLSTabletID(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
    : ls_id_(ls_id), tablet_id_(tablet_id)
  {
  }
  ObTableLoadLSTabletID &operator=(const ObTableLoadLSTabletID &other)
  {
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    return *this;
  }
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid();
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

enum class ObTableLoadStatusType : int64_t
{
  NONE = 0,
  INITED, // 初始化
  LOADING, // 只有LOADING状态能创建trans
  FROZEN, // 冻结, 不再创建trans
  MERGING, // 合并中
  MERGED, // 合并完成
  COMMIT, // 完成
  ERROR,
  ABORT,
};

struct ObTableLoadSegmentID final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadSegmentID() : id_(common::OB_INVALID_ID) {}
  explicit ObTableLoadSegmentID(uint64_t id) : id_(id) {}
  void reset() { id_ = common::OB_INVALID_ID; }
  bool is_valid() const { return id_ != common::OB_INVALID_ID; }
  ObTableLoadSegmentID &operator=(const uint64_t id)
  {
    id_ = id;
    return *this;
  }
  ObTableLoadSegmentID &operator=(const ObTableLoadSegmentID &other)
  {
    id_ = other.id_;
    return *this;
  }
  bool operator==(const ObTableLoadSegmentID &other) const
  {
    return (id_ == other.id_);
  }
  bool operator!=(const ObTableLoadSegmentID &other) const
  {
    return !(*this == other);
  }
  bool operator<(const ObTableLoadSegmentID &other) const
  {
    return (id_ < other.id_);
  }
  bool operator>(const ObTableLoadSegmentID &other) const
  {
    return (id_ > other.id_);
  }
  bool operator>=(const ObTableLoadSegmentID &other) const
  {
    return !(*this < other);
  }
  bool operator<=(const ObTableLoadSegmentID &other) const
  {
    return !(*this > other);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = common::murmurhash(&id_, sizeof(id_), 0);
    return hash_val;
  }
  int compare(const ObTableLoadSegmentID &other) const
  {
    return static_cast<int32_t>(id_ - other.id_);//TODO(suzhi.yt): fix convert int64 to int32
  }
  TO_STRING_KV(K_(id));
public:
  uint64_t id_;
};

struct ObTableLoadTransId final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadTransId() : trans_gid_(common::OB_INVALID_ID) {}
  ObTableLoadTransId(const ObTableLoadSegmentID &segment_id, uint64_t trans_gid)
    : segment_id_(segment_id), trans_gid_(trans_gid)
  {
  }
  void reset()
  {
    segment_id_.reset();
    trans_gid_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return segment_id_.is_valid() && common::OB_INVALID_ID != trans_gid_;
  }
  bool operator==(const ObTableLoadTransId &other) const
  {
    return (segment_id_ == other.segment_id_ && trans_gid_ == other.trans_gid_);
  }
  bool operator!=(const ObTableLoadTransId &other) const
  {
    return !(*this == other);
  }
  bool operator<(const ObTableLoadTransId &other) const
  {
    return (segment_id_ != other.segment_id_ ? segment_id_ < other.segment_id_
                                             : trans_gid_ < other.trans_gid_);
  }
  bool operator>(const ObTableLoadTransId &other) const
  {
    return (segment_id_ != other.segment_id_ ? segment_id_ > other.segment_id_
                                             : trans_gid_ > other.trans_gid_);
  }
  bool operator>=(const ObTableLoadTransId &other) const
  {
    return !(*this < other);
  }
  bool operator<=(const ObTableLoadTransId &other) const
  {
    return !(*this > other);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = segment_id_.hash();
    hash_val = common::murmurhash(&trans_gid_, sizeof(trans_gid_), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const ObTableLoadTransId &other) const
  {
    return (segment_id_ != other.segment_id_ ? segment_id_.compare(other.segment_id_)
                                             : static_cast<int32_t>(trans_gid_ - other.trans_gid_));//TODO(suzhi.yt): fix convert int64 to int32
  }
  TO_STRING_KV(K_(segment_id), K_(trans_gid));
public:
  ObTableLoadSegmentID segment_id_;
  uint64_t trans_gid_;
};

enum class ObTableLoadTransStatusType : int64_t
{
  NONE = 0,
  INITED,
  RUNNING,
  FROZEN,
  COMMIT,
  ERROR,
  ABORT,
};

static int table_load_status_to_string(ObTableLoadStatusType status,
    common::ObString &status_str)
{
  int ret = OB_SUCCESS;

  switch (status) {
    case ObTableLoadStatusType::NONE:
      status_str = "none";
      break;
    case ObTableLoadStatusType::INITED:
      status_str = "inited";
      break;
    case ObTableLoadStatusType::LOADING:
      status_str = "loading";
      break;
    case ObTableLoadStatusType::FROZEN:
      status_str = "frozen";
      break;
    case ObTableLoadStatusType::MERGING:
      status_str = "merging";
      break;
    case ObTableLoadStatusType::MERGED:
      status_str = "merged";
      break;
    case ObTableLoadStatusType::COMMIT:
      status_str = "commit";
      break;
    case ObTableLoadStatusType::ABORT:
      status_str = "abort";
      break;
    case ObTableLoadStatusType::ERROR:
      status_str = "error";
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
  }

  return ret;
}

static int table_load_trans_status_to_string(ObTableLoadTransStatusType trans_status,
    common::ObString &status_str)
{
  int ret = OB_SUCCESS;

  switch (trans_status) {
    case ObTableLoadTransStatusType::NONE:
      status_str = "none";
      break;
    case ObTableLoadTransStatusType::INITED:
      status_str = "inited";
      break;
    case ObTableLoadTransStatusType::RUNNING:
      status_str = "running";
      break;
    case ObTableLoadTransStatusType::FROZEN:
      status_str = "frozen";
      break;
    case ObTableLoadTransStatusType::COMMIT:
      status_str = "commit";
      break;
    case ObTableLoadTransStatusType::ABORT:
      status_str = "abort";
      break;
    case ObTableLoadTransStatusType::ERROR:
      status_str = "error";
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
  }

  return ret;
}

struct ObTableLoadResultInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadResultInfo() : rows_affected_(0), records_(0), deleted_(0), skipped_(0), warnings_(0) {}
  ~ObTableLoadResultInfo() {}
  TO_STRING_KV(K_(rows_affected), K_(records), K_(deleted), K_(skipped), K_(warnings));
public:
  uint64_t rows_affected_ CACHE_ALIGNED;
  uint64_t records_ CACHE_ALIGNED;
  uint64_t deleted_ CACHE_ALIGNED;
  uint64_t skipped_ CACHE_ALIGNED;
  uint64_t warnings_ CACHE_ALIGNED;
};

} // namespace table
} // namespace oceanbase
