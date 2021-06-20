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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_IN_MEMORY_SORT_
#define OCEANBASE_SQL_ENGINE_SORT_OB_IN_MEMORY_SORT_

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/sort/ob_base_sort.h"

namespace oceanbase {
namespace sql {
class ObInMemorySort : public ObBaseSort {
public:
  ObInMemorySort();
  virtual ~ObInMemorySort();
  virtual void reset();
  virtual void reuse();
  virtual void rescan();
  virtual int set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns);
  virtual int add_row(const common::ObNewRow& row);
  virtual int sort_rows();
  // @pre sort_rows()
  virtual int get_next_row(common::ObNewRow& row);
  virtual int get_next_compact_row(common::ObString& compact_row);

  virtual int64_t get_row_count() const override;
  virtual int64_t get_used_mem_size() const override;
  virtual int init_tenant_id(uint64_t tenant_id)
  {
    allocator_.set_tenant_id(tenant_id);
    row_store_.set_tenant_id(tenant_id);
    return common::OB_SUCCESS;
  }
  void set_use_compact(bool opt)
  {
    row_store_.set_use_compact(opt);
  }
  TO_STRING_KV(K_(sort_array_pos));

private:
  // types
  struct StaticComparer;
  struct Comparer;

private:
  // data members
  common::ObArenaAllocator allocator_;
  common::ObRowStore row_store_;
  common::ObArray<const common::ObRowStore::StoredRow*> sort_array_;
  int64_t sort_array_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObInMemorySort);
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_IN_MEMORY_SORT_ */
