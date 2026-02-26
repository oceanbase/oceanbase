//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_MDS_FILTER_INFO_H_
#define OB_STORAGE_COMPACTION_MDS_FILTER_INFO_H_
#include "lib/utility/ob_macro_utils.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <class T>
class ObIArray;
}
namespace compaction
{

struct ObMdsFilterInfo final
{
public:
  enum MdsFilterInfoType : uint8_t
  {
    TRUNCATE_INFO = 0,
    TTL_FILTER_INFO = 1,
    // add new mds filter type here
    MDS_FILTER_TYPE_MAX
  };
  static bool is_valid_filter_info_type(const MdsFilterInfoType type)
  { return type >= TRUNCATE_INFO && type < MDS_FILTER_TYPE_MAX; }
  static const char *filter_info_type_to_str(const MdsFilterInfoType &type);
public:
  template <typename T, MdsFilterInfoType type>
  struct ObFilterInfoArray
  {
    ObFilterInfoArray()
      : cnt_(0),
        array_(nullptr)
    {}
    ~ObFilterInfoArray();
    void destroy(common::ObIAllocator &allocator);
    int assign(common::ObIAllocator &allocator, const ObFilterInfoArray &other);
    int init(common::ObIAllocator &allocator, const common::ObIArray<T> &other)
    { return inner_init(allocator, other.get_data(), other.count()); }
    const T &at(const int64_t idx) const
    {
      OB_ASSERT(nullptr != array_ && idx >= 0 && idx < cnt_);
      return array_[idx];
    }
    bool is_empty() const { return cnt_ == 0; }
    int64_t count() const { return cnt_; }
    bool is_valid() const { return is_valid_filter_info_type(type) && (is_empty() || nullptr != array_); }
    MdsFilterInfoType get_filter_type() const { return type; }
    int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
    int deserialize(
        common::ObIAllocator &allocator,
        const char *buf,
        const int64_t data_len,
        int64_t &pos);
    int64_t get_serialize_size() const;
    void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
    int64_t to_string(char* buf, const int64_t buf_len) const;
    bool operator ==(const ObFilterInfoArray &other) const = delete;
  private:
    int inner_init(
      common::ObIAllocator &allocator,
      const T *other,
      const int64_t cnt);
  private:
    int64_t cnt_;
    T *array_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObFilterInfoArray);
  };
  typedef ObFilterInfoArray<storage::ObTruncateInfoKey, TRUNCATE_INFO> ObTruncateInfoKeyArray;
  typedef ObFilterInfoArray<storage::ObTTLFilterInfoKey, TTL_FILTER_INFO> ObTTLFilterInfoKeyArray;

public:
  ObMdsFilterInfo()
    : version_(MDS_FILTER_INFO_VERSION_LATEST),
      reserved_(0),
      truncate_info_keys_(),
      ttl_filter_info_keys_(),
      mlog_purge_scn_(0)
  {}
  ~ObMdsFilterInfo() {}
  void destroy(common::ObIAllocator &allocator);
  bool is_empty() const { return !has_truncate_info(); }
  bool has_truncate_info() const { return truncate_info_keys_.count() > 0; }
  bool is_valid() const { return truncate_info_keys_.is_valid(); }
  int init_truncate_keys(common::ObIAllocator &allocator, common::ObIArray<storage::ObTruncateInfoKey> &input_array)
  { return truncate_info_keys_.init(allocator, input_array); }
  const ObTruncateInfoKeyArray &get_truncate_keys() const { return truncate_info_keys_; }
  int assign(common::ObIAllocator &allocator, const ObMdsFilterInfo &medium_info);
  bool operator ==(const ObMdsFilterInfo &other) const = delete;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
  {
    truncate_info_keys_.gene_info(buf, buf_len, pos);
  }
  TO_STRING_KV(K_(version), K_(truncate_info_keys), K_(ttl_filter_info_keys), K_(mlog_purge_scn));
private:
  static const int64_t MDS_FILTER_INFO_VERSION_V1 = 1;
  static const int64_t MDS_FILTER_INFO_VERSION_V2 = 2; // TTL Filter Info & Mlog Purge SCN
  static const int64_t MDS_FILTER_INFO_VERSION_LATEST = MDS_FILTER_INFO_VERSION_V2;
private:
  static const int32_t MFI_ONE_BYTE = 8;
  static const int32_t MFI_RESERVED_BITS = 56;
  union {
    uint64_t info_;
    struct
    {
      uint64_t version_                  : MFI_ONE_BYTE;
      uint64_t reserved_                 : MFI_RESERVED_BITS;
    };
  };
  ObTruncateInfoKeyArray truncate_info_keys_;
  ObTTLFilterInfoKeyArray ttl_filter_info_keys_;
  int64_t mlog_purge_scn_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMdsFilterInfo);
};

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
ObMdsFilterInfo::ObFilterInfoArray<T, type>::~ObFilterInfoArray()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(array_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "exist unfree buf", "filter_info_type", filter_info_type_to_str(type), K_(cnt), KP_(array));
  }
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
void ObMdsFilterInfo::ObFilterInfoArray<T, type>::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(array_)) {
    allocator.free(array_);
    array_ = nullptr;
  }
  cnt_ = 0;
}

template <typename T>
int alloc_array(ObIAllocator &allocator, const int64_t cnt, T *&array) {
  int ret = OB_SUCCESS;
  void *alloc_buf = nullptr;
  if (OB_ISNULL(alloc_buf = allocator.alloc(sizeof(T) * cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_COMPACTION_LOG(WARN, "failed to alloc memory", KR(ret), K(cnt));
  } else {
    array = new(alloc_buf) T[cnt];
  }
  return ret;
}

template <typename T>
int loop_to_assign(const int64_t cnt, const T *src, T *dst) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == src || nullptr == dst)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_COMPACTION_LOG(WARN, "invalid argument", KR(ret), KP(src), KP(dst));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt; ++idx) {
    if (OB_UNLIKELY(!src[idx].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_COMPACTION_LOG(WARN, "item in array is not valid", KR(ret), K(idx), K(src[idx]));
    } else {
      dst[idx] = src[idx];
    }
  }
  return ret;
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
int ObMdsFilterInfo::ObFilterInfoArray<T, type>::assign(
  ObIAllocator &allocator,
  const ObFilterInfoArray &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid() || other.get_filter_type() != type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_COMPACTION_LOG(WARN, "invalid argument", KR(ret), K(other), "cur_filter_type", filter_info_type_to_str(type));
  } else {
    ret = inner_init(allocator, other.array_, other.cnt_);
  }
  return ret;
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
int ObMdsFilterInfo::ObFilterInfoArray<T, type>::inner_init(
    common::ObIAllocator &allocator,
    const T *other,
    const int64_t cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == other || cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_COMPACTION_LOG(WARN, "invalid argument", KR(ret), K(other), "cur_filter_type", filter_info_type_to_str(type));
  } else if (OB_UNLIKELY(!is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_COMPACTION_LOG(WARN, "current mds filter info is not empty", KR(ret), KPC(this));
  } else if (0 == cnt) {
    cnt_ = 0;
  } else if (OB_FAIL(alloc_array(allocator, cnt, array_))) {
    STORAGE_COMPACTION_LOG(WARN, "failed to alloc array", KR(ret), K(other));
  } else if (OB_FAIL(loop_to_assign(cnt, other/*src*/, array_/*dst*/))) {
    STORAGE_COMPACTION_LOG(WARN, "failed to assign", KR(ret), K(other));
  } else {
    cnt_ = cnt;
  }
  if (OB_FAIL(ret)) {
    destroy(allocator);
  }
  return ret;
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
int ObMdsFilterInfo::ObFilterInfoArray<T, type>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_COMPACTION_LOG(WARN, "invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_COMPACTION_LOG(WARN, "invalid MdsFilterInfo to serialize", KR(ret), KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, cnt_);
    if (OB_FAIL(ret) || is_empty()) {
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt_; ++idx) {
        if (OB_UNLIKELY(!array_[idx].is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_COMPACTION_LOG(WARN, "item in array is not valid", KR(ret), K(idx), K(array_[idx]));
        } else if (OB_FAIL(array_[idx].serialize(buf, buf_len, pos))) {
          STORAGE_COMPACTION_LOG(WARN, "failed to serialize item", KR(ret), K(idx), K(array_[idx]));
        }
      } // for
    }
  }
  return ret;
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
int ObMdsFilterInfo::ObFilterInfoArray<T, type>::deserialize(
      ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_COMPACTION_LOG(WARN, "invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, cnt_);
    if (OB_UNLIKELY(cnt_ < 0)) {
      ret = OB_ERR_SYS;
      STORAGE_COMPACTION_LOG(WARN, "deserialized cnt is invalid", KR(ret), K_(cnt));
    } else if (cnt_ > 0) {
      if (OB_FAIL(alloc_array(allocator, cnt_, array_))) {
        STORAGE_COMPACTION_LOG(WARN, "failed to alloc array", KR(ret), K_(cnt));
      }
      for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt_; ++idx) {
        if (OB_FAIL(array_[idx].deserialize(buf, data_len, pos))) {
          STORAGE_COMPACTION_LOG(WARN, "failed to deserialize item", KR(ret), K(idx));
        }
      } // for
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_DATA;
      STORAGE_COMPACTION_LOG(WARN, "invalid mds filter info", K(ret), KPC(this));
    }
    if (OB_FAIL(ret)) {
      destroy(allocator);
    }
  }
  return ret;
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
int64_t ObMdsFilterInfo::ObFilterInfoArray<T, type>::get_serialize_size() const
{
  int64_t len = 0;
  if (is_valid()) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, cnt_);
    for (int64_t idx = 0; idx < cnt_; ++idx) {
      len += array_[idx].get_serialize_size();
    }
  }
  return len;
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
void ObMdsFilterInfo::ObFilterInfoArray<T, type>::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || pos >= buf_len) {
  } else {
    J_OBJ_START();
    J_KV("filter_info_type", filter_info_type_to_str(type), K_(cnt));
    if (!is_valid() || is_empty()) {
    } else {
      J_COMMA();
      for (int64_t idx = 0; idx < cnt_; ++idx) {
        BUF_PRINTF("[%ld]:", idx);
        J_OBJ_START();
        array_[idx].gene_info(buf, buf_len, pos);
        J_OBJ_END();
        if (idx != cnt_ - 1) {
          BUF_PRINTF(";");
        }
      } // for
    }
    J_OBJ_END();
  }
}

template<typename T, ObMdsFilterInfo::MdsFilterInfoType type>
int64_t ObMdsFilterInfo::ObFilterInfoArray<T, type>::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  gene_info(buf, buf_len, pos);
  return pos;
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MDS_FILTER_INFO_H_
