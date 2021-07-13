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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_SPECIFIC_COLUMNS_SORT
#define OCEANBASE_SQL_ENGINE_SORT_OB_SPECIFIC_COLUMNS_SORT

#include "common/row/ob_row_iterator.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase {
namespace sql {
class ObSpecificColumnsSort : public ObBaseSort {
public:
  explicit ObSpecificColumnsSort();
  ObSpecificColumnsSort(const char* label, uint64_t malloc_block_size, uint64_t tenant_id,
      oceanbase::common::ObCtxIds::ObCtxIdEnum ctx_id);
  virtual ~ObSpecificColumnsSort(){};
  virtual void reset();
  virtual void reuse();
  virtual void rescan();
  virtual int set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos);
  virtual int add_row(const common::ObNewRow& row, bool& need_sort);
  int add_row_without_copy(common::ObNewRow* row);
  virtual int sort_rows();
  virtual int get_next_row(common::ObNewRow& row);
  int get_sort_result_array(common::ObArray<const common::ObNewRow*>& sort_result);
  virtual int64_t get_row_count() const override
  {
    return row_count_;
  }
  virtual int64_t get_used_mem_size() const override
  {
    return row_alloc_.used();
  }
  virtual int init_tenant_id(uint64_t tenant_id)
  {
    row_alloc_.set_tenant_id(tenant_id);
    return common::OB_SUCCESS;
  }
  virtual int get_next_compact_row(common::ObString& compact_row)
  {
    UNUSED(compact_row);
    return common::OB_SUCCESS;
  }
  inline const common::ObIArray<ObSortColumn>* get_sort_columns()
  {
    return sort_columns_;
  }
  inline bool has_prefix_pos() const
  {
    return prefix_keys_pos_ > 0;
  }
  int check_block_row(const common::ObNewRow& row, const common::ObNewRow* last_row, bool& is_cur_block);
  int deep_copy_new_row(const common::ObNewRow& row, common::ObNewRow*& new_row, common::ObArenaAllocator& alloc);
  TO_STRING_KV(K_(row_count), K_(prefix_keys_pos), K_(row_array_pos));
  // private funs
private:
  int init_specific_columns();
  static int specific_columns_cmp(const void* p1, const void* p2);

protected:
  common::ObArenaAllocator row_alloc_;  // deep copy row
private:
  int64_t row_count_;
  int64_t prefix_keys_pos_;           // prefix columns keys pos
  int64_t row_array_pos_;             // for get next row, cur array pos
  common::ObNewRow* next_block_row_;  // next sort block row
  common::ObArray<const common::ObNewRow*> sort_array_;
  const common::ObIArray<ObSortColumn>* sort_columns_;
  char* obobj_buffer_;
  int err_;
  // common::ObSEArray<ObSortColumn, 8> prefix_columns_;
  DISALLOW_COPY_AND_ASSIGN(ObSpecificColumnsSort);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_SPECIFIC_COLUMNS_SORT */
