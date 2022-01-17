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

#ifndef OCEANBASE_COMMON_OB_SPARSE_CELL_READER_H_
#define OCEANBASE_COMMON_OB_SPARSE_CELL_READER_H_
#include "common/cell/ob_cell_writer.h"
#include "common/object/ob_object.h"
#include "lib/ob_define.h"
namespace oceanbase {
namespace blocksstable {
class ObSparseCellReader {
public:
  ObSparseCellReader();
  virtual ~ObSparseCellReader()
  {
    reset();
  }
  int init(const char* buf, int64_t buf_size, common::ObIAllocator* allocator);
  void reset();
  void set_pos(const int64_t pos)
  {
    pos_ = pos;
  }
  int64_t get_pos() const
  {
    return pos_;
  }
  int read_cell(common::ObObj& obj);

private:
  inline int read_oracle_timestamp(const common::ObObjType obj_type, const uint8_t meta_attr,
      const common::ObOTimestampMetaAttrType otmat, common::ObObj& obj);
  int read_interval_ds(common::ObObj& obj);
  int read_interval_ym(common::ObObj& obj);
  template <class T>
  int read(const T*& ptr);
  int read_text_store(const common::ObCellWriter::CellMeta& cell_meta, common::ObObj& obj);
  template <class T>
  static const T* read(const char* row_buf, int64_t& pos);

private:
  const char* buf_;
  int64_t buf_size_;
  int64_t pos_;
  uint64_t column_id_;
  common::ObObj obj_;
  int64_t row_start_;
  bool is_inited_;
  common::ObIAllocator* allocator_;
};

template <class T>
inline const T* ObSparseCellReader::read(const char* row_buf, int64_t& pos)
{
  const T* ptr = reinterpret_cast<const T*>(row_buf + pos);
  pos += sizeof(T);
  return ptr;
}
}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
