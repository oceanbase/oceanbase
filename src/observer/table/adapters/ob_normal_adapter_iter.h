/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_NORMAL_ADAPTER_ITER_H
#define _OB_NORMAL_ADAPTER_ITER_H

#include "ob_hbase_cell_iter.h"

namespace oceanbase
{
namespace table
{

class ObHbaseNormalCellIter : public ObHbaseCellIter
{
public:
  ObHbaseNormalCellIter() = default;
  int get_next_cell(ObNewRow *&row) override;

private:

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHbaseNormalCellIter);
};

} // end of namespace table
} // end of namespace oceanbase

#endif