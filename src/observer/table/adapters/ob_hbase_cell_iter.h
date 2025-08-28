/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_HBASE_CELL_ITER_H
#define _OB_HBASE_CELL_ITER_H

#include "share/table/ob_table.h"
#include "observer/table/ob_table_scan_executor.h"
#include "observer/table/tableapi/ob_table_api_service.h"

namespace oceanbase
{   
namespace table
{

class ObHbaseRescanParam
{
public:
  ObHbaseRescanParam()
    : scan_range_(),
      limit_(-1)
  {}
  ~ObHbaseRescanParam() = default;
  TO_STRING_KV(K_(scan_range), K_(limit))

  void reset()
  {
    scan_range_.reset(); 
    limit_ = -1;
  }
  common::ObNewRange &get_scan_range() { return scan_range_; }
  int32_t get_limit() const { return limit_; }

  void set_limit(int32_t limit) { limit_ = limit; }

private:
  common::ObNewRange scan_range_; 
  int32_t limit_;
};

class ObHbaseICellIter
{
public:
  ObHbaseICellIter() = default;
  virtual ~ObHbaseICellIter() = default;
  virtual int open() = 0;
  virtual int get_next_cell(ObNewRow *&row) = 0;
  virtual int rescan(ObHbaseRescanParam &rescan_param) = 0;
  virtual int close() = 0;
};

class ObHbaseCellIter : public ObHbaseICellIter
{
public:
  ObHbaseCellIter();
  virtual ~ObHbaseCellIter() = default;
  virtual int open() override;
  virtual int get_next_cell(ObNewRow *&row) override;
  virtual int rescan(ObHbaseRescanParam &rescan_param) override;
  virtual int close() override;
  ObTableCtx &get_tb_ctx() { return tb_ctx_; }
  OB_INLINE ObTableApiRowIterator &get_table_api_scan_iter() { return tb_row_iter_; }

protected:
  common::ObArenaAllocator allocator_;
  ObTableApiRowIterator tb_row_iter_;
  ObTableCtx tb_ctx_;
  bool is_opened_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHbaseCellIter);
};

} // end of namespace table
} // end of namespace oceanbase

#endif