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

#ifndef OCEANBASE_COMMON_OB_SPARSE_COLUMN_WRITER_H_
#define OCEANBASE_COMMON_OB_SPARSE_COLUMN_WRITER_H_
#include "common/object/ob_obj_type.h"
#include "lib/number/ob_number_v2.h"
#include "lib/ob_define.h"
namespace oceanbase {
namespace common {
class ObObj;
class ObNewRow;
class ObCellWriter;
}  // namespace common
namespace blocksstable {

class ObSparseCellWriter {
public:
  ObSparseCellWriter();
  virtual ~ObSparseCellWriter()
  {}
  int init(char* buf, int64_t size, const bool old_text_format = false);
  int append(const common::ObObj& obj);
  void reset();
  inline int64_t size() const
  {
    return pos_;
  }

private:
  template <class T>
  int append(const T& value);
  inline int write_int(const enum common::ObObjType meta_type, const int64_t value);
  inline int write_number(const enum common::ObObjType meta_type, const common::number::ObNumber& number);
  inline int write_char(const common::ObObj& obj, const enum common::ObObjType meta_type, const common::ObString& str);
  inline int write_binary(const enum common::ObObjType meta_type, const common::ObString& str);
  inline int write_oracle_timestamp(const common::ObObj& obj, const common::ObOTimestampMetaAttrType otmat);
  inline int write_interval_ym(const common::ObObj& obj);
  inline int write_interval_ds(const common::ObObj& obj);
  inline int write_ext(const int64_t ext_value);
  inline int get_int_byte(int64_t int_value);
  int write_text_store(const common::ObObj& obj);

private:
  char* buf_;
  int64_t buf_size_;
  int64_t pos_;
  int64_t last_append_pos_;
  int64_t cell_cnt_;
  bool old_text_format_;
  bool is_inited_;
};
}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
