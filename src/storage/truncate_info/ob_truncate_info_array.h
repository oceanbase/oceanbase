// Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OB_STORAGE_TRUNCATE_INFO_ARRAY_H_
#define OB_STORAGE_TRUNCATE_INFO_ARRAY_H_

#include "storage/truncate_info/ob_truncate_info.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
struct MdsDumpKey;
struct MdsDumpNode;
} // namespace mds

enum class ObMDSInfoSrc : uint8_t
{
  SRC_KV_CACHE = 0,
  SRC_MDS = 1,
  SRC_MAX
};

template <typename T>
class ObMDSInfoArray
{
public:
  static constexpr int64_t STACK_CACHED_ARRAY_SIZE = 1;

  ObMDSInfoArray()
      : allocator_(nullptr), stack_cached_array_idx_(0), src_(ObMDSInfoSrc::SRC_MAX),
        is_inited_(false)
  {
  }

  virtual ~ObMDSInfoArray() { reset(); }

  int init_for_first_creation(ObIAllocator &allocator);
  int init_with_kv_cache_array(ObIAllocator &allocator, const ObArrayWrap<T> &info_array);

  void reset();

  /**
   * some getter
   */
  OB_INLINE ObMDSInfoSrc get_src() const { return src_; }
  OB_INLINE const ObIArray<T *> &get_array() const { return mds_info_array_; }
  OB_INLINE ObIArray<T *> &get_array() { return mds_info_array_; }

  /**
   * array interface
   */
  OB_INLINE int64_t count() const { return mds_info_array_.count(); }
  OB_INLINE bool empty() const { return mds_info_array_.empty(); }
  OB_INLINE bool is_valid() const { return is_inited_ && inner_is_valid(); }
  OB_INLINE const T *at(const int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count());
    return mds_info_array_.at(idx);
  }
  OB_INLINE T *at(const int64_t idx)
  {
    OB_ASSERT(idx >= 0 && idx < count());
    return mds_info_array_.at(idx);
  }

  int append_with_deep_copy(const T &info);
  int append_ptr(T &info);
  int assign(ObIAllocator &allocator, const ObMDSInfoArray &other);

  /**
   * serialize & deserialize
   */
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  /**
   * virtual interface
   */
  virtual int sort_array() = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  bool inner_is_valid() const { return 0 == count() || (count() > 0 && allocator_ != nullptr); }

  int inner_allocate_and_append(T *&info);
  int inner_append_and_deep_copy(const T &info);

protected:
  ObIAllocator *allocator_;
  T stack_cached_array_[STACK_CACHED_ARRAY_SIZE];
  int64_t stack_cached_array_idx_;
  ObSEArray<T *, STACK_CACHED_ARRAY_SIZE> mds_info_array_;
  ObMDSInfoSrc src_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMDSInfoArray);
};

class ObTruncateInfoArray : public ObMDSInfoArray<ObTruncateInfo>
{
public:
  static bool compare(const ObTruncateInfo *lhs, const ObTruncateInfo *rhs);
  int sort_array() override;

  VIRTUAL_TO_STRING_KV(K_(is_inited), "array_cnt", count(), K_(src), K_(mds_info_array));
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_ARRAY_H_
