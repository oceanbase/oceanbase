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

#ifndef OCEANBASE_COMMON_OB_CELL_READER_H_
#define OCEANBASE_COMMON_OB_CELL_READER_H_
#include "common/cell/ob_cell_writer.h"
#include "common/object/ob_object.h"
#include "lib/ob_define.h"
namespace oceanbase
{
namespace common
{
class ObCellReader
{
public:
  ObCellReader();
  virtual ~ObCellReader() {}
  int init(const char *buf, int64_t buf_size, ObCompactStoreType store_type);
  void reset();
  int next_cell();
  int get_cell(uint64_t &column_id, ObObj &obj, bool *is_row_finished = NULL, ObString *row = NULL);
  int get_cell(const ObObj *&obj, bool *is_row_finished = NULL, ObString *row = NULL);
  int get_cell(uint64_t &column_id, const ObObj *&obj, bool *is_row_finished = NULL, ObString *row = NULL);
  void set_pos(const int64_t pos) { pos_ = pos; }
  int64_t get_pos() const { return pos_; }
  int read_cell(common::ObObj &obj);
private:
  inline int read_oracle_timestamp(const ObObjType obj_type, const uint8_t meta_attr,
      const ObOTimestampMetaAttrType otmat, ObObj &obj);
  int read_interval_ds(ObObj &obj);
  int read_interval_ym(ObObj &obj);
  int read_urowid();
  int read_decimal_int(ObObj &);
  int parse(uint64_t *column_id);
  template<class T>
  int read(const T *&ptr);
  inline int is_es_end_object(const common::ObObj &obj, bool &is_end_obj);
private:
  const char *buf_;
  int64_t buf_size_;
  int64_t pos_;
  uint64_t column_id_;
  ObObj obj_;
  ObCompactStoreType store_type_;
  int64_t row_start_;
  bool is_inited_;
};
}//end namespace common
}//end namespace oceanbase
#endif
