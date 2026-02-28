//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_TTL_TTL_FILTER_VAL_H_
#define OB_STORAGE_COMPACTION_TTL_TTL_FILTER_VAL_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace storage
{
class ObTTLFilterInfoArray;

class ObTTLFilterVal final
{
public:
  struct TTLFilterPair {
  public:
    TTLFilterPair() : col_idx_(0), filter_val_(0) {}
    TTLFilterPair(const int64_t col_idx, const int64_t filter_val)
      : col_idx_(col_idx), filter_val_(filter_val) {}
    TTLFilterPair &operator=(const TTLFilterPair &other) {
      col_idx_ = other.col_idx_;
      filter_val_ = other.filter_val_;
      return *this;
    }
    TO_STRING_KV(K(col_idx_), K(filter_val_));
    int64_t col_idx_;
    int64_t filter_val_;
  };
  ObTTLFilterVal()
    : filter_pairs_()
  {
  }
  ~ObTTLFilterVal() = default;
  OB_INLINE bool is_valid() const { return count() > 0; }
  OB_INLINE void reset() { filter_pairs_.reset(); }
  int init(
    const ObTTLFilterInfoArray &ttl_filter_info_array);
  int64_t count() const { return filter_pairs_.count(); }
  const TTLFilterPair &at(const int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count());
    return filter_pairs_[idx];
  }
  TO_STRING_KV(K_(filter_pairs));
public:
  common::ObSEArray<TTLFilterPair, 2> filter_pairs_; // in col_idx ascending order
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TTL_TTL_FILTER_VAL_H_
