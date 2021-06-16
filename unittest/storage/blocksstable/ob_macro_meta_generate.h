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

#ifndef OB_MACRO_META_GENERATE_H_
#define OB_MACRO_META_GENERATE_H_

#include "lib/number/ob_number_v2.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {
class ObMacroMetaGenerator {
public:
  ObMacroMetaGenerator()
  {}
  virtual ~ObMacroMetaGenerator()
  {}
  static int gen_meta(ObMacroBlockMeta& meta, ObIAllocator& allocator);
};

int ObMacroMetaGenerator::gen_meta(ObMacroBlockMeta& meta, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  int16_t column_num = 7;
  char* buf = NULL;

  meta.attr_ = ObMacroBlockCommonHeader::SSTableData;
  meta.data_version_ = 1;
  meta.column_number_ = column_num;
  meta.rowkey_column_number_ = column_num;
  meta.column_index_scale_ = 1;
  meta.row_store_type_ = 0;
  meta.row_count_ = 10;
  meta.occupy_size_ = 123;
  meta.data_checksum_ = 111;
  meta.micro_block_count_ = 1;
  meta.micro_block_data_offset_ = 0;
  meta.micro_block_index_offset_ = 222;
  meta.micro_block_endkey_offset_ = 333;

  meta.compressor_ = (char*)allocator.alloc(5);
  strncpy(meta.compressor_, "none", 5);

  if (OB_SUCC(ret)) {
    if (NULL == (buf = (char*)allocator.alloc(sizeof(ObObj) * column_num))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      meta.endkey_ = new (buf) ObObj[column_num];
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = (char*)allocator.alloc(sizeof(ObObjMeta) * column_num))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      meta.column_type_array_ = new (buf) ObObjMeta[column_num];
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = (char*)allocator.alloc(sizeof(uint16_t) * column_num))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      meta.column_id_array_ = new (buf) uint16_t[column_num];
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = (char*)allocator.alloc(sizeof(ObOrderType) * column_num))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      meta.column_order_array_ = new (buf) ObOrderType[column_num];
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = (char*)allocator.alloc(sizeof(int64_t) * column_num))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      meta.column_checksum_ = new (buf) int64_t[column_num];
    }
  }

  if (OB_SUCC(ret)) {
    // init endkey
    // ObTinyIntType type
    meta.endkey_[0].set_tinyint(static_cast<int8_t>(0));
    meta.column_type_array_[0].set_type(common::ObTinyIntType);
    // ObUTinyIntType type
    meta.endkey_[1].set_utinyint(1);
    meta.column_type_array_[1].set_type(common::ObUTinyIntType);
    // ObDoubleType type
    meta.endkey_[2].set_double(static_cast<double>(2));
    meta.column_type_array_[2].set_type(common::ObDoubleType);
    // ObNumber type
    common::number::ObNumber number;
    buf = reinterpret_cast<char*>(allocator.alloc(1024));
    int64_t tmp = 3;
    ret = number.from(tmp, allocator);
    meta.endkey_[3].set_number(number);
    meta.column_type_array_[3].set_type(common::ObNumberType);
    // ObYearType type
    meta.endkey_[4].set_year(static_cast<uint8_t>(4));
    meta.column_type_array_[4].set_type(common::ObYearType);
    // ObVarcharType
    sprintf(buf, "%d", 5);
    ObString str;
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
    meta.endkey_[5].set_varchar(str);
    meta.endkey_[5].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    meta.column_type_array_[5].set_type(common::ObVarcharType);
    meta.column_type_array_[5].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    // ObHexStringType
    sprintf(buf, "%d", 5);
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
    meta.endkey_[6].set_hex_string(str);
    meta.column_type_array_[6].set_type(common::ObHexStringType);

    for (int64_t i = 0; i < column_num; ++i) {
      meta.column_id_array_[i] = static_cast<uint16_t>(i);
      meta.column_checksum_[i] = i + 0x1220;
      if (i % 2 == 0) {
        meta.column_order_array_[i] = ObOrderType::ASC;
      } else {
        meta.column_order_array_[i] = ObOrderType::DESC;
      }
    }
  }

  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MACRO_META_GENERATE_H_ */
