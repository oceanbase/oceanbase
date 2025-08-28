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
#include "share/external_table/ob_external_table_part_info.h"

namespace oceanbase
{

namespace share
{

int ObExternalTablePartInfoArray::set_part_pair_by_idx(const int64_t idx, ObExternalTablePartInfo &part_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= part_infos_.count())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("array index out of range", K(ret), K(idx), K(part_infos_.count()));
  } else {
    part_infos_.at(idx) = part_info;
  }
  return ret;
}
int ObExternalTablePartInfoArray::reserve(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (OB_FAIL(part_infos_.allocate_array(allocator_, capacity))) {
    LOG_WARN("fail to reserve array", K(ret), K(capacity));
  }
  return ret;
}

int ObExternalTablePartInfoArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const 
{
  int ret = OB_SUCCESS;
  // 序列化数组大小
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, part_infos_.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  // 序列化每个pair
  for (int64_t i = 0; OB_SUCC(ret) && i < part_infos_.count(); i++) {
    const ObExternalTablePartInfo &part_info = part_infos_.at(i);
    LST_DO_CODE(OB_UNIS_ENCODE, part_info.part_id_);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_info.list_row_value_.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize row", K(ret));
      } else {
        LST_DO_CODE(OB_UNIS_ENCODE, part_info.partition_spec_);
      }
    }
  }
  return ret;
}

int ObExternalTablePartInfoArray::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
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
  } else if (OB_FAIL(part_infos_.allocate_array(allocator_, count))) {
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
        ObExternalTablePartInfo part_info;
        ObNewRow row;
        ObNewRow tmp_row;
        row.assign(obj_array, obj_capacity);
        
        LST_DO_CODE(OB_UNIS_DECODE, part_info.part_id_);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to decode part_id", K(ret));
        } else if (OB_FAIL(row.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize row", K(ret));
        } else if (OB_FAIL(ob_write_row(allocator_, row, tmp_row))) {
          LOG_WARN("fail to write row", K(ret));
        } else {
          part_info.list_row_value_ = tmp_row;
        }

        if (OB_SUCC(ret)) {
          ObString tmp_str;
          LST_DO_CODE(OB_UNIS_DECODE, tmp_str);
          if (OB_FAIL(ob_write_string(allocator_, tmp_str, part_info.partition_spec_))) {
            LOG_WARN("failed to copy string", KR(ret), K(tmp_str));
          }
        }
        if (OB_SUCC(ret)) {
          part_infos_.at(i) = part_info;
        }
      }
    }
  }
  return ret;
}

int64_t ObExternalTablePartInfoArray::get_serialize_size() const
{
  int64_t len = serialization::encoded_length_vi64(part_infos_.count());
  for (int64_t i = 0; i < part_infos_.count(); i++) {
    const ObExternalTablePartInfo &part_info = part_infos_.at(i);
    len += serialization::encoded_length_vi64(part_info.part_id_);
    len += part_info.list_row_value_.get_serialize_size();
    len += part_info.partition_spec_.get_serialize_size();
  }
  return len;
}

}  // namespace share
}  // namespace oceanbase
