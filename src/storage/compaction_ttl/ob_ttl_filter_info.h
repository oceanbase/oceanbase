//Copyright (c) 2025 OceanBase
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
struct ObTTLFlag
{
public:
  static constexpr uint8_t TTL_FLAG_VERSION_V1 = 1;

  ObTTLFlag() : version_(TTL_FLAG_VERSION_V1), had_rowscn_as_ttl_(0), reserved_(0) {}

  void reset()
  {
    version_ = TTL_FLAG_VERSION_V1;
    had_rowscn_as_ttl_ = 0;
    reserved_ = 0;
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

  TO_STRING_KV(K_(version), K_(had_rowscn_as_ttl));

  union {
    uint64_t flag_;
    struct
    {
      uint64_t version_ : 8;
      uint64_t had_rowscn_as_ttl_ : 1;
      uint64_t reserved_ : 55;
    };
  };
};
}

namespace storage
{
struct ObTTLFilterInfoKey final
{
public:
  ObTTLFilterInfoKey() = default;
  ~ObTTLFilterInfoKey() = default;
  int64_t to_string(char *, const int64_t) const { return 0; }
  int serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int deserialize(const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int mds_serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int mds_deserialize(const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t mds_get_serialize_size() const { return 0; }
  int assign(const ObTTLFilterInfoKey &other) { return OB_SUCCESS; }
};

struct ObTTLFilterInfo final
{
public:
  ObTTLFilterInfo() = default;
  ~ObTTLFilterInfo() = default;
  int64_t to_string(char *, const int64_t) const { return 0; }
  int serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int deserialize(common::ObIAllocator &allocator, const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int assign(ObIAllocator &allocator, const ObTTLFilterInfo &other) { return OB_SUCCESS; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTTLFilterInfo);
};

} // namespace strorage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_