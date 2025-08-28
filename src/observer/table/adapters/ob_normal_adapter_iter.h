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