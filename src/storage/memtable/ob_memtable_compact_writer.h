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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_COMPACT_WRITER_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_COMPACT_WRITER_

#include "share/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "common/cell/ob_cell_writer.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtableCompactWriter : public common::ObCellWriter
{
public:
  static const int64_t RP_LOCAL_NUM = 0;
  static const int64_t RP_TOTAL_NUM = 128;
  static const int64_t SMALL_BUFFER_SIZE = (1 << 12) - sizeof(common::ObCellWriter) - 16/* sizeof(ObMemtableCompactWriter)*/;
  static const int64_t NORMAL_BUFFER_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
  static const int64_t BIG_ROW_BUFFER_SIZE = common::OB_MAX_ROW_LENGTH_IN_MEMTABLE;
public:
  ObMemtableCompactWriter();
  ~ObMemtableCompactWriter();
public:
  int init();
  void reset();
  void set_dense() { common::ObCellWriter::set_store_type(common::DENSE); }
  int append(uint64_t column_id, const common::ObObj &obj, common::ObObj *clone_obj = nullptr);
  int row_finish();
  int64_t get_buf_size() { return buf_size_; }
  virtual bool allow_lob_locator() override { return false; }

private:
  int extend_buf();
private:
  char buf_[SMALL_BUFFER_SIZE];
  char *buffer_;
  int64_t buf_size_;

  DISALLOW_COPY_AND_ASSIGN(ObMemtableCompactWriter);
};
}//namespace memtable
}//namespace memtable
#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_COMPACT_WRITER_
