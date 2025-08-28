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

#ifndef _OB_HBASE_NORMAL_ADAPTER_H
#define _OB_HBASE_NORMAL_ADAPTER_H

#include "ob_i_adapter.h"

namespace oceanbase
{
namespace table
{

class ObHNormalAdapter : public ObIHbaseAdapter
{
public:
  ObHNormalAdapter() = default;
  virtual ~ObHNormalAdapter() {}

  virtual int put(ObTableExecCtx &ctx, const ObITableEntity &cell) override;
  virtual int put(ObTableCtx &ctx, const ObHCfRows &rows) override;
  virtual int multi_put(ObTableExecCtx &ctx, const ObIArray<const ObITableEntity *> &cells) override;
  virtual int del(ObTableExecCtx &ctx, const ObITableEntity &cell) override;
  virtual int scan(ObIAllocator &alloc, ObTableExecCtx &ctx, const ObTableQuery &query, ObHbaseICellIter *&iter) override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHNormalAdapter);
};

} // end of namespace table
} // end of namespace oceanbase

#endif
