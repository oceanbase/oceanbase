/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_DAS_SEARCH_INDEX_UTILS_H
#define OCEANBASE_DAS_SEARCH_INDEX_UTILS_H

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashset.h"
#include "share/datum/ob_datum.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "share/search_index/ob_search_index_generator.h"
#include "sql/das/ob_das_domain_utils.h"

namespace oceanbase
{
namespace sql
{

class ObSearchIndexDMLIterator final : public ObDomainDMLIterator
{
public:
  ObSearchIndexDMLIterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef),
      is_inited_(false),
      row_generator_()
  {}
  virtual ~ObSearchIndexDMLIterator() = default;
  int init(
    const ObDASDMLBaseCtDef *das_ctdef,
    const ObDASDMLBaseCtDef *main_ctdef,
    const IntFixedArray *row_projector);
private:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;

private:
    bool is_inited_;
    share::ObSearchIndexRowGenerator row_generator_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_DAS_SEARCH_INDEX_UTILS_H
