//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_SCHEMA_OB_LIST_ROW_VALUES_H_
#define OB_SHARE_SCHEMA_OB_LIST_ROW_VALUES_H_
#include "common/row/ob_row.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
namespace schema
{

struct ObListRowValues
{
public:
  ObListRowValues();
  explicit ObListRowValues(common::ObIAllocator &allocator);
  ~ObListRowValues();
  void reset() { values_.reset(); }
  int64_t count() const { return values_.count(); }
  int push_back(const common::ObNewRow &row) { return values_.push_back(row); }
  common::ObNewRow &at(const int64_t idx) { return values_.at(idx); }
  const common::ObNewRow &at(const int64_t idx) const { return values_.at(idx); }
  const common::ObIArray<common::ObNewRow>& get_values() const { return values_; }
  int assign(ObIAllocator &allocator, const ObListRowValues &other);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const;
  bool operator ==(const ObListRowValues &other) const = delete;
  int sort_array();
  TO_STRING_KV(K_(values));
protected:
  int push_back_with_deep_copy(ObIAllocator &allocator, const common::ObNewRow &row);

  // CAREFUL! this struct cannot add more member in serialization
  common::ObSEArray<common::ObNewRow, 2> values_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObListRowValues);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_SCHEMA_OB_LIST_ROW_VALUES_H_
