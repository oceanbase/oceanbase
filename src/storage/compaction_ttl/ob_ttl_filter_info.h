// Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_
#define OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_

#include "lib/utility/ob_print_utils.h"
#include "object/ob_obj_type.h"

namespace oceanbase
{

namespace share
{
class SCN;
struct ObTTLFlag
{
public:
  static constexpr uint8_t TTL_FLAG_VERSION_V1 = 1;

  ObTTLFlag();
  bool is_valid(const uint64_t tenant_data_version = UINT64_MAX) const;
  void reset();
  uint64_t get_ttl_type() const { return ttl_type_;}
  void fuse(const ObTTLFlag &other)
  {
    if (other.had_rowscn_as_ttl_ || had_rowscn_as_ttl_) {
      had_rowscn_as_ttl_ = 1;
    } else {
      had_rowscn_as_ttl_ = 0;
    }
    if (other.ttl_type_ != ttl_type_) {
      ttl_type_ = other.ttl_type_;
    }
  }
  OB_INLINE int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    LST_DO_CODE(OB_UNIS_ENCODE, flag_);
    return ret;
  }

  OB_INLINE int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    LST_DO_CODE(OB_UNIS_DECODE, flag_);
    return ret;
  }

  OB_INLINE int64_t get_serialize_size() const
  {
    int64_t len = 0;
    LST_DO_CODE(OB_UNIS_ADD_LEN, flag_);
    return len;
  }

  OB_INLINE int write_string(ObIAllocator &allocator, ObString &str) const
  {
    int ret = OB_SUCCESS;

    int64_t len = get_serialize_size();
    int64_t pos = 0;
    char *ptr = nullptr;

    if (OB_ISNULL(ptr = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate memory failed", K(ret), K(len));
    } else if (OB_FAIL(serialize(ptr, len, pos))) {
      STORAGE_LOG(WARN, "serialize failed", K(ret));
    } else {
      str.assign_ptr(ptr, len);
    }

    return ret;
  }

  TO_STRING_KV(K_(version), K_(had_rowscn_as_ttl), K_(ttl_type));

  union {
    uint64_t flag_;
    struct { // FARM COMPAT WHITELIST
      uint64_t version_ : 8;
      uint64_t had_rowscn_as_ttl_ : 1;
      uint64_t ttl_type_ : 4; // NONE means no ttl_definition on schema NOW or compat for old KV_TTL mode table
      uint64_t reserved_ : 51;
    };
  };
};
}

namespace storage
{
class ObTTLFilterInfoKey final
{
  OB_UNIS_VERSION(1);

public:
  static constexpr uint8_t MAGIC_NUMBER = 0xFE;

  ObTTLFilterInfoKey() : tx_id_(0) {}

  OB_INLINE void reset() { tx_id_ = 0; }
  OB_INLINE bool is_valid() const { return tx_id_ > 0; }

  /**
   * compare
   */
  bool operator!=(const ObTTLFilterInfoKey &other) const { return tx_id_ != other.tx_id_; }
  bool operator==(const ObTTLFilterInfoKey &other) const { return tx_id_ == other.tx_id_; }
  bool operator<(const ObTTLFilterInfoKey &other) const { return tx_id_ < other.tx_id_; }

  /**
   * serialize and deserialize
   */
  int mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int64_t mds_get_serialize_size() const;

  /**
   * to string
   */
  void gene_info(char *buf, const int64_t buf_len, int64_t &pos) const;

  TO_STRING_KV(K_(tx_id));

public:
  int64_t tx_id_;
};

class ObTTLFilterInfo final
{
public:
  static constexpr int64_t TTL_FILTER_INFO_VERSION_V1 = 1;
  static constexpr int64_t TTL_FILTER_INFO_VERSION_LATEST = TTL_FILTER_INFO_VERSION_V1;

  enum class ObTTLFilterColType : uint8_t
  {
    ROWSCN = 0, // rowscn col is int64_t, in the units of ns
    INT64 = 1, // int64
    DATE = 2,
    TIMESTAMP = 3, // timestamp col
    TIMESTAMP_TZ = 4, // timestamp with time zone for oracle
    TIMESTAMP_LTZ = 5, // timestamp with local time zone for oracle
    TIMESTAMP_NANO = 6, // timestamp nanosecond for oracle
    MYSQL_DATETIME = 7, // datetime for mysql
    MAX = 8,
  };

public:
  ObTTLFilterInfo()
      : version_(TTL_FILTER_INFO_VERSION_LATEST), ttl_filter_col_type_(ObTTLFilterColType::MAX),
        reserved_(0), key_(), commit_version_(0), ttl_filter_col_idx_(0), ttl_filter_value_(0)
  {
  }

  OB_INLINE bool is_valid() const
  {
    return key_.is_valid() && ttl_filter_col_type_ < ObTTLFilterColType::MAX
           && ttl_filter_col_idx_ >= 0 && ttl_filter_value_ > 0;
  }

  OB_INLINE void reset()
  {
    version_ = TTL_FILTER_INFO_VERSION_LATEST;
    reserved_ = 0;
    key_.reset();
    commit_version_ = -1;
    ttl_filter_col_idx_ = -1;
    ttl_filter_col_type_ = ObTTLFilterColType::MAX;
    ttl_filter_value_ = 0;
  }

  OB_INLINE void destroy() { reset(); }

  /**
   * copy related functions
   */
  OB_INLINE int assign(ObIAllocator &, const ObTTLFilterInfo &other)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", KR(ret), K(other));
    } else if (OB_FAIL(other.shallow_copy(*this))) {
      STORAGE_LOG(WARN, "Fail to shallow copy", KR(ret));
    }

    return ret;
  }

  OB_INLINE int deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObTTLFilterInfo &dest) const
  {
    return shallow_copy(dest);
  }

  OB_INLINE int shallow_copy(ObTTLFilterInfo &dest) const
  {
    int ret = OB_SUCCESS;

    dest.info_ = info_;
    dest.key_ = key_;
    dest.commit_version_ = commit_version_;
    dest.ttl_filter_col_idx_ = ttl_filter_col_idx_;
    dest.ttl_filter_value_ = ttl_filter_value_;

    return ret;
  }

  OB_INLINE int64_t get_deep_copy_size() const { return get_serialize_size(); }
  OB_INLINE ObTTLFilterColType get_ttl_filter_col_type() const { return static_cast<ObTTLFilterColType>(ttl_filter_col_type_); }
  OB_INLINE bool is_rowscn_filter() const { return get_ttl_filter_col_type() == ObTTLFilterColType::ROWSCN; }

  /**
   * serialize and deserialize
   */
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int deserialize(ObIAllocator &unused_allocator, const char *buf, const int64_t buf_len, int64_t &pos); // for template interface
  OB_INLINE int64_t get_serialize_size() const
  {
    int64_t len = 0;
    LST_DO_CODE(OB_UNIS_ADD_LEN, info_, key_, commit_version_, ttl_filter_col_idx_, ttl_filter_value_);
    return len;
  }

  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);

  static int to_filter_col_type(const ObObjType &obj_type, ObTTLFilterColType &filter_col_type);

  /**
   * to string
   */
  TO_STRING_KV(K_(key),
               K_(version),
               K_(reserved),
               K_(commit_version),
               K_(ttl_filter_col_idx),
               K_(ttl_filter_col_type),
               K_(ttl_filter_value));

public:
  union {
    uint64_t info_;
    struct
    {
      uint64_t version_ : 8;
      ObTTLFilterColType ttl_filter_col_type_ : 8; // for alignment, don't change position
      uint64_t reserved_ : 48;
    };
  };

  ObTTLFilterInfoKey key_;
  int64_t commit_version_;
  int64_t ttl_filter_col_idx_;
  int64_t ttl_filter_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTTLFilterInfo);

  // for unittest
  int compare(const ObTTLFilterInfo &other, bool &equal) const
  {
    equal = info_ == other.info_
            && key_.tx_id_ == other.key_.tx_id_
            && commit_version_ == other.commit_version_
            && ttl_filter_col_idx_ == other.ttl_filter_col_idx_
            && ttl_filter_value_ == other.ttl_filter_value_;
    return OB_SUCCESS;
  }
};

} // namespace strorage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_