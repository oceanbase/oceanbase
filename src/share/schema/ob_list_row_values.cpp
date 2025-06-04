//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_list_row_values.h"
#include "share/schema/ob_schema_struct.h"
namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

/*
* ObListRowValues
*/
ObListRowValues::ObListRowValues()
  : values_()
{}

ObListRowValues::~ObListRowValues()
{
  reset();
}

ObListRowValues::ObListRowValues(ObIAllocator &allocator)
  : values_(SCHEMA_MALLOC_BLOCK_SIZE, ModulePageAllocator(allocator))
{}

int ObListRowValues::assign(ObIAllocator &allocator, const ObListRowValues &other)
{
  int ret = OB_SUCCESS;
  const int64_t count = other.values_.count();
  if (OB_FAIL(values_.reserve(count))) {
    LOG_WARN("fail to reserve se array", K(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      const ObNewRow &row = other.values_.at(i);
      if (OB_FAIL(push_back_with_deep_copy(allocator, row))) {
        LOG_WARN("Fail to push row", K(ret), K(row));
      }
    } // for
  }
  return ret;
}

int ObListRowValues::push_back_with_deep_copy(ObIAllocator &allocator, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObNewRow tmp_row;
  if (OB_FAIL(ob_write_row(allocator, row, tmp_row))) {
    LOG_WARN("Fail to write row", K(ret), K(row));
  } else if (OB_FAIL(values_.push_back(tmp_row))) {
    LOG_WARN("Fail to push back row", K(ret));
  }
  return ret;
}

// CAREFUL! this struct cannot add more member to serialization
int ObListRowValues::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, values_.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < values_.count(); i ++) {
    if (OB_FAIL(values_.at(i).serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to encode row", K(ret));
    }
  }
  return ret;
}

int64_t ObListRowValues::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(values_.count());
  for (int64_t i = 0; i < values_.count(); i ++) {
    len += values_.at(i).get_serialize_size();
  }
  return len;
}

int64_t ObListRowValues::get_deep_copy_size() const
{
  int64_t list_row_size = values_.get_data_size();
  for (int64_t i = 0; i < values_.count(); i ++) {
    list_row_size += values_.at(i).get_deep_copy_size();
  }
  return list_row_size * 2 - 1;
}

int ObListRowValues::deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  ObArenaAllocator tmp_allocator("ListRowVal");
  ObObj *obj_array = NULL;
  const int obj_capacity = 1024;
  void *tmp_buf = NULL;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &size))) {
    LOG_WARN("fail to decode vi64", K(ret));
  } else if (OB_ISNULL(tmp_buf = tmp_allocator.alloc(sizeof(ObObj) * obj_capacity))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret));
  } else {
    obj_array = new (tmp_buf) ObObj[obj_capacity];
    ObNewRow row;
    row.assign(obj_array, obj_capacity);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      row.count_ = obj_capacity;
      if (OB_FAIL(row.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize row", K(ret));
      } else if (OB_FAIL(push_back_with_deep_copy(allocator, row))) {
        LOG_WARN("fail to push back row", K(ret), K(row));
      }
    } // for
  }
  return ret;
}

struct InnerPartListVectorCmp
{
public:
  InnerPartListVectorCmp() : ret_(OB_SUCCESS) {}
public:
  bool operator()(const ObNewRow &left, const ObNewRow &right)
  {
    bool bool_ret = false;
    int cmp = 0;
    if (OB_SUCCESS != ret_) {
      // failed before
    } else if (OB_SUCCESS != (ret_ = ObRowUtil::compare_row(left, right, cmp))) {
      SHARE_SCHEMA_LOG_RET(ERROR, ret_, "l or r is invalid", K(ret_));
    } else {
      bool_ret = (cmp < 0);
    }
    return bool_ret;
  }
  int get_ret() const { return ret_; }
private:
  int ret_;
};

int ObListRowValues::sort_array()
{
  int ret = OB_SUCCESS;
  InnerPartListVectorCmp part_list_vector_op;
  lib::ob_sort(values_.begin(), values_.end(), part_list_vector_op);
  if (OB_FAIL(part_list_vector_op.get_ret())) {
    LOG_WARN("fail to sort list row values", K(ret));
  }
  return ret;
}


} // namespace schema
} // namespace share
} // namespace oceanbase
