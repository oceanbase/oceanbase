//Copyright (c) 2024 OceanBase
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
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
struct MdsDumpKey;
struct MdsDumpNode;
}
struct ObTruncateInfo;

enum ObTruncateInfoSrc : uint8_t
{
  TRUN_SRC_KV_CACHE = 0,
  TRUN_SRC_MDS = 1,
  TRUN_SRC_MAX
};

struct ObTruncateInfoArray
{
public:
  ObTruncateInfoArray();
  ~ObTruncateInfoArray();
  int init_for_first_creation(common::ObIAllocator &allocator);
  int init_with_kv_cache_array(common::ObIAllocator &allocator, const ObArrayWrap<ObTruncateInfo> &truncate_info_array);
  void reset();
  OB_INLINE int64_t count() const { return truncate_info_array_.count(); }
  OB_INLINE bool empty() const { return truncate_info_array_.empty(); }
  OB_INLINE bool is_valid() const { return is_inited_ && inner_is_valid(); }
  const ObTruncateInfo *at(const int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count());
    return truncate_info_array_.at(idx);
  }
  ObTruncateInfo *at(const int64_t idx)
  {
    OB_ASSERT(idx >= 0 && idx < count());
    return truncate_info_array_.at(idx);
  }
  OB_INLINE ObIArray<ObTruncateInfo *> &get_array() { return truncate_info_array_; }
  OB_INLINE const ObIArray<ObTruncateInfo *> &get_array() const { return truncate_info_array_; }
  OB_INLINE ObTruncateInfoSrc get_src() const { return src_; }
  int append_with_deep_copy(const ObTruncateInfo &truncate_info); // will deep copy with allocator
  int append_ptr(ObTruncateInfo &truncate_info); // will append ptr only
  int append(
      const mds::MdsDumpKey &key,
      const mds::MdsDumpNode &node);
  void sort_array();
  int assign(common::ObIAllocator &allocator, const ObTruncateInfoArray &other);
  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  static bool compare(const ObTruncateInfo *lhs, const ObTruncateInfo *rhs);
  TO_STRING_KV(K_(is_inited), "array_cnt", count(), K_(src), K_(truncate_info_array));
private:
  bool inner_is_valid() const { return 0 == count() || (count() >= 0 && allocator_ != nullptr); }
  void reset_list();
  int inner_append_and_sort(ObTruncateInfo &info);
  ObSEArray<ObTruncateInfo *, 1> truncate_info_array_;
  common::ObIAllocator *allocator_;
  ObTruncateInfoSrc src_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTruncateInfoArray);
};

} // namespace strorage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_ARRAY_H_
