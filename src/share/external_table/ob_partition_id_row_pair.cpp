  /**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL
#include "share/external_table/ob_partition_id_row_pair.h"

namespace oceanbase
{

namespace share
{

int ObPartitionIdRowPairArray::set_part_pair_by_idx(const int64_t idx, ObPartitionIdRowPair &pair)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= pairs_.count())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("array index out of range", K(ret), K(idx), K(pairs_.count()));
  } else {
    pairs_.at(idx) = pair;
  }
  return ret;
}
int ObPartitionIdRowPairArray::reserve(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (OB_FAIL(pairs_.allocate_array(allocator_, capacity))) {
    LOG_WARN("fail to reserve array", K(ret), K(capacity));
  }
  return ret;
}

int ObPartitionIdRowPairArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // 序列化数组大小
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, pairs_.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  // 序列化每个pair
  for (int64_t i = 0; OB_SUCC(ret) && i < pairs_.count(); i++) {
    const ObPartitionIdRowPair &pair = pairs_.at(i);
    LST_DO_CODE(OB_UNIS_ENCODE, pair.part_id_);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pair.list_row_value_.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize row", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionIdRowPairArray::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // 反序列化数组大小
  int64_t count = 0;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("fail to decode count", K(ret));
  } else if (count < 0) {
    LOG_WARN("invalid count", K(ret), K(count));
    ret = OB_INVALID_ARGUMENT;
  } else if (count == 0) {
    // 如果count为0，则不进行反序列化
    return ret;
  } else if (OB_FAIL(pairs_.allocate_array(allocator_, count))) {
    LOG_WARN("fail to alloc pairs array", K(ret), K(count));
  } else {
    // 分配临时内存用于ObObj数组
    ObArenaAllocator tmp_allocator("PartIdRowPair");
    void *tmp_buf = NULL;
    ObObj *obj_array = NULL;
    const int obj_capacity = 1024;

    if (OB_ISNULL(tmp_buf = tmp_allocator.alloc(sizeof(ObObj) * obj_capacity))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret));
    } else if (OB_ISNULL(obj_array = new (tmp_buf) ObObj[obj_capacity])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new obj array", KR(ret));
    } else {
      // 反序列化每个pair
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        ObPartitionIdRowPair pair;
        ObNewRow row;
        ObNewRow tmp_row;
        row.assign(obj_array, obj_capacity);

        LST_DO_CODE(OB_UNIS_DECODE, pair.part_id_);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to decode part_id", K(ret));
        } else if (OB_FAIL(row.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize row", K(ret));
        } else if (OB_FAIL(ob_write_row(allocator_, row, tmp_row))) {
          LOG_WARN("fail to write row", K(ret));
        } else {
          pair.list_row_value_ = tmp_row;
          pairs_.at(i) = pair;
        }
      }
    }
  }
  return ret;
}

int64_t ObPartitionIdRowPairArray::get_serialize_size() const
{
  int64_t len = serialization::encoded_length_vi64(pairs_.count());
  for (int64_t i = 0; i < pairs_.count(); i++) {
    const ObPartitionIdRowPair &pair = pairs_.at(i);
    len += serialization::encoded_length_vi64(pair.part_id_);
    len += pair.list_row_value_.get_serialize_size();
  }
  return len;
}

}  // namespace share
}  // namespace oceanbase
