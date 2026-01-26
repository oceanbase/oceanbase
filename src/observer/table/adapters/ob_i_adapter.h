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

#ifndef _OB_I_ADAPTER_H
#define _OB_I_ADAPTER_H

#include "observer/table/common/ob_table_common_struct.h"
#include "lib/container/ob_iarray.h"
#include "share/table/ob_table.h"
#include "common/ob_range.h"
#include "ob_hbase_cell_iter.h"

namespace oceanbase
{
namespace table
{

class ObIHbaseAdapter
{
public:
  ObIHbaseAdapter()
      : allocator_("HbaseAdapAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {
    lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
  }

  virtual ~ObIHbaseAdapter() {}
  virtual int put(ObTableExecCtx &ctx, const ObITableEntity &cell) = 0;
  virtual int put(ObTableCtx &ctx, const ObHCfRows &rows) = 0;
  virtual int multi_put(ObTableExecCtx &ctx, const ObIArray<const ObITableEntity *> &cells) = 0;
  virtual int del(ObTableExecCtx &ctx, const ObITableEntity &cell) = 0;
  virtual int scan(ObIAllocator &alloc, ObTableExecCtx &ctx, const ObTableQuery &query, ObHbaseICellIter *&iter) = 0;
  virtual void reuse();

protected:
  int init_table_ctx(ObTableExecCtx &exec_ctx,
                     const ObITableEntity &cell,
                     ObTableOperationType::Type op_type,
                     ObTableCtx &tb_ctx);

  int init_scan(ObTableExecCtx &exec_ctx, const ObTableQuery &query, ObTableCtx &tb_ctx);

protected:
  common::ObArenaAllocator allocator_;
  int64_t lob_inrow_threshold_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObIHbaseAdapter);
};

} // end of namespace table
} // end of namespace oceanbase

#endif