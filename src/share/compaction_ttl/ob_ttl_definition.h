// Copyright (c) 2024 OceanBase
//  OceanBase is licensed under Mulan PubL v2.
//  You can use this software according to the terms and conditions of the Mulan PubL v2.
//  You may obtain a copy of Mulan PubL v2 at:
//           http://license.coscl.org.cn/MulanPubL-2.0
//  THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
//  EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
//  MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//  See the Mulan PubL v2 for more details.

#ifndef OB_SHARE_TTL_DEFINITION_H_
#define OB_SHARE_TTL_DEFINITION_H_

#include "lib/utility/ob_print_utils.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace share
{

struct ObTTLDefinition final
{
  enum ObTTLType : uint8_t
  {
    NONE = 0, // no ttl definition on schema NOW or compat for old DELETING mode table
    DELETING = 1,
    COMPACTION = 2,
    INVALID,
  };

  static const char *ttl_type_to_string(const ObTTLType ttl_type)
  { // only for print schema
    const char *ret_str = "INVALID";
    switch (ttl_type) {
    case DELETING:
    case NONE:
      ret_str = "DELETING";
      break;
    case COMPACTION:
      ret_str = "COMPACTION";
      break;
    default:
      break;
    }
    return ret_str;
  }

  ObTTLDefinition() : ttl_definition_(), ttl_type_(INVALID) {}

  ObTTLDefinition(const ObString &ttl_definition, const uint64_t ttl_type)
      : ttl_definition_(ttl_definition), ttl_type_(static_cast<ObTTLType>(ttl_type))
  {
  }

  OB_INLINE bool is_valid() const { return ttl_type_ != INVALID && !ttl_definition_.empty(); }

  OB_INLINE void reset()
  {
    ttl_definition_.reset();
    ttl_type_ = INVALID;
  }

  OB_INLINE void gene_info(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || buf_len <= 0 || pos >= buf_len) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TTL = %.*s BY %s ", ttl_definition_.length(), ttl_definition_.ptr(), ttl_type_to_string(ttl_type_)))) {
      COMMON_LOG(WARN, "fail to print ttl definition", K(ret), K(ttl_definition_), K(ttl_type_to_string(ttl_type_)));
    }
  }

  TO_STRING_KV("ttl", ttl_definition_, "type", ttl_type_to_string(ttl_type_));

  ObString ttl_definition_;
  ObTTLType ttl_type_;
};


struct ObTTLFlag
{
public:
  static constexpr uint8_t TTL_FLAG_VERSION_V1 = 1;
  static constexpr uint8_t TTL_FLAG_VERSION_V2 = 2;

  enum class TTLColumnType: uint8_t {
    NONE = 0,
    ROWSCN = 1,
    ROWKEY = 2,
    HBASE = 3
  };

  ObTTLFlag()
      : version_(TTL_FLAG_VERSION_V1), ttl_column_type_(TTLColumnType::NONE),
        ttl_type_(ObTTLDefinition::NONE), was_compaction_ttl_(0), reserved_(0),
        being_scn_ttl_time_us_(0)
  {
  }

  OB_INLINE bool is_valid(const uint64_t tenant_data_version = UINT64_MAX) const
  {
    bool bool_ret = true;

    if (tenant_data_version < DATA_VERSION_4_5_1_0 || version_ == TTL_FLAG_VERSION_V1) {
      bool_ret = version_ == TTL_FLAG_VERSION_V1
                 && ttl_column_type_ == TTLColumnType::NONE
                 && ttl_type_ == ObTTLDefinition::NONE
                 && was_compaction_ttl_ == 0
                 && reserved_ == 0
                 && being_scn_ttl_time_us_ == 0;
    } else {
      bool_ret = version_ == TTL_FLAG_VERSION_V2 && reserved_ == 0;
    }

    return bool_ret;
  }

  OB_INLINE void reset()
  {
    version_ = TTL_FLAG_VERSION_V1;
    ttl_column_type_ = TTLColumnType::NONE;
    ttl_type_ = ObTTLDefinition::NONE;
    was_compaction_ttl_ = 0;
    reserved_ = 0;
    being_scn_ttl_time_us_ = 0;
  }

  OB_INLINE uint64_t get_ttl_type() const { return ttl_type_;}

  OB_INLINE int64_t get_being_scn_ttl_time_ns() const { return being_scn_ttl_time_us_ * 1000L; }

  OB_INLINE void update_being_scn_ttl_time(const int64_t being_scn_ttl_time)
  {
    version_ = MAX(version_, TTL_FLAG_VERSION_V2);
    being_scn_ttl_time_us_ = MAX(being_scn_ttl_time_us_, being_scn_ttl_time);
  }

  OB_INLINE void fuse(const ObTTLFlag &other)
  {
    version_ = MAX(version_, other.version_);
    was_compaction_ttl_ |= other.was_compaction_ttl_;
    ttl_column_type_ = other.ttl_column_type_;
    ttl_type_ = other.ttl_type_;
    update_being_scn_ttl_time(other.being_scn_ttl_time_us_);
  }

  OB_INLINE int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;

    LST_DO_CODE(OB_UNIS_ENCODE, flag_);
    if (OB_SUCC(ret) && version_ >= TTL_FLAG_VERSION_V2) {
      LST_DO_CODE(OB_UNIS_ENCODE, being_scn_ttl_time_us_);
    }

    return ret;
  }

  OB_INLINE int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;

    LST_DO_CODE(OB_UNIS_DECODE, flag_);
    if (OB_SUCC(ret) && version_ >= TTL_FLAG_VERSION_V2) {
      LST_DO_CODE(OB_UNIS_DECODE, being_scn_ttl_time_us_);
    }

    return ret;
  }

  OB_INLINE int64_t get_serialize_size() const
  {
    int64_t len = 0;

    LST_DO_CODE(OB_UNIS_ADD_LEN, flag_);
    if (version_ >= TTL_FLAG_VERSION_V2) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, being_scn_ttl_time_us_);
    }

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

    if (OB_FAIL(ret)) {
      if (ptr != nullptr) {
        allocator.free(ptr);
      }
    }

    return ret;
  }

  TO_STRING_KV(K_(version), K_(ttl_column_type), K_(ttl_type), K_(was_compaction_ttl), K_(being_scn_ttl_time_us));

  union {
    uint64_t flag_;
    struct { // FARM COMPAT WHITELIST
      uint64_t version_ : 8;
      TTLColumnType ttl_column_type_ : 8; // indicate NOW the ttl column type is ROWSCN, ROWKEY or HBASE
      uint64_t ttl_type_ : 4; // NONE means no ttl_definition on schema NOW or compat for old KV_TTL mode table
      uint64_t was_compaction_ttl_ : 1; // once this table become compaction ttl table, this flag will always be true
      uint64_t reserved_ : 43;
    };
  };
  int64_t being_scn_ttl_time_us_;
};

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_TTL_DEFINITION_H_
