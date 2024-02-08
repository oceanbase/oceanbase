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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_STORE_ROW_FACTORY_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_STORE_ROW_FACTORY_H_
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
namespace oceanbase {
namespace sql {
template <typename Store_Row, bool has_addon>
class ObSortVecOpStoreRowFactory {
public:
  ObSortVecOpStoreRowFactory(common::ObIAllocator &allocator,
                  ObSqlMemMgrProcessor &sql_mem_processor,
                  const RowMeta *&sk_row_meta,
                  const RowMeta *&addon_row_meta,
                  int64_t &inmem_row_size,
                  const int64_t &topn_cnt)
      : allocator_(allocator), sql_mem_processor_(sql_mem_processor),
        sk_row_meta_(sk_row_meta), addon_row_meta_(addon_row_meta),
        inmem_row_size_(inmem_row_size), topn_cnt_(topn_cnt) {}

  void free_row_store(const RowMeta &sk_row_meta, Store_Row *&sk_row)
  {
    sql_mem_processor_.alloc(-1 * sk_row->get_max_size(sk_row_meta));
    inmem_row_size_ -= sk_row->get_max_size(sk_row_meta);
    allocator_.free(sk_row);
    sk_row = nullptr;
  }

  void free_row_store(Store_Row *&sk_row)
  {
    if (has_addon) {
      Store_Row *addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
      free_row_store(*addon_row_meta_, addon_row);
    }
    free_row_store(*sk_row_meta_, sk_row);
  }

  int copy_row(const RowMeta *sk_row_meta, const Store_Row *src_row, Store_Row *&reuse_row)
  {
    int ret = OB_SUCCESS;
    int64_t buffer_len = 0;
    int64_t row_size = src_row->get_row_size();
    Store_Row *dst_row = nullptr;
    // check to see whether this old row's space is adequate for new one
    if (nullptr != reuse_row && reuse_row->get_max_size(*sk_row_meta) >= row_size) {
      dst_row = reuse_row;
      buffer_len = reuse_row->get_max_size(*sk_row_meta);
    } else {
      char *buf = nullptr;
      if (topn_cnt_ < 256) {
        buffer_len = row_size > 256 ? row_size * 4 : 1024;
      } else {
        buffer_len = row_size * 2;
      }
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator_.alloc(buffer_len)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc buf failed", K(ret));
      } else {
        // generate new row
        sql_mem_processor_.alloc(buffer_len);
        dst_row = new (buf) Store_Row();
        inmem_row_size_ += buffer_len;
        // release reuse row
        if (OB_NOT_NULL(reuse_row)) {
          sql_mem_processor_.alloc(-1 * (reuse_row->get_max_size(*sk_row_meta)));
          inmem_row_size_ -= reuse_row->get_max_size(*sk_row_meta);
          free_row_store(*sk_row_meta, reuse_row);
        }
        reuse_row = dst_row;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(reuse_row->assign(
            *reinterpret_cast<const ObCompactRow *>(src_row)))) {
      LOG_WARN("stored row assign failed", K(ret));
    } else {
      reuse_row->set_max_size(buffer_len, *sk_row_meta);
    }
    return ret;
  }
  // copy row to row.
  // if row space is enough reuse the space, else use the alloc get new space.
  int copy_to_row(const Store_Row *src_row, Store_Row *&reuse_row)
  {
    int ret = OB_SUCCESS;
    Store_Row *reuse_addon_row = nullptr;
    Store_Row *ori_addon_row = nullptr;
    if (has_addon) {
      if (OB_NOT_NULL(reuse_row)) {
        reuse_addon_row = reuse_row->get_addon_ptr(*sk_row_meta_);
      }
      ori_addon_row = const_cast<Store_Row *>(src_row)->get_addon_ptr(*sk_row_meta_);
    }

    if (OB_FAIL(copy_row(sk_row_meta_, src_row, reuse_row))) {
      LOG_WARN("failed to copy sort key row", K(ret));
    } else if (has_addon) {
      if (OB_FAIL(copy_row(addon_row_meta_, ori_addon_row, reuse_addon_row))) {
        free_row_store(*sk_row_meta_, reuse_row);
        LOG_WARN("failed to copy addon row", K(ret));
      } else {
        reuse_row->set_addon_ptr(reuse_addon_row, *sk_row_meta_);
      }
    }
    return ret;
  }

  // deep copy orign_row use the alloc
  int deep_copy_row(const RowMeta *row_meta, const Store_Row *orign_row, Store_Row *&new_row)
  {
    int ret = OB_SUCCESS;
    new_row = nullptr;
    char *buf = nullptr;
    if (OB_ISNULL(orign_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_ISNULL(buf = reinterpret_cast<char *>(
                             allocator_.alloc(orign_row->get_row_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buf failed", K(ret));
    } else if (OB_ISNULL(new_row = new (buf) Store_Row())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to new row", K(ret));
    } else if (OB_FAIL(new_row->assign(
                   *reinterpret_cast<const ObCompactRow *>(orign_row)))) {
      LOG_WARN("stored row assign failed", K(ret));
    } else {
      int64_t row_size = orign_row->get_row_size();
      sql_mem_processor_.alloc(row_size);
      inmem_row_size_ += row_size;
    }
    return ret;
  }

  int generate_new_row(const Store_Row *orign_row, Store_Row *&new_row)
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = deep_copy_row(sk_row_meta_, orign_row, new_row))) {
      LOG_WARN("failed to copy to row", K(ret));
    } else if (has_addon) {
      Store_Row *new_addon_row = nullptr;
      Store_Row *ori_addon_row =
          const_cast<Store_Row *>(orign_row->get_addon_ptr(*sk_row_meta_));
      if (OB_FAIL(deep_copy_row(addon_row_meta_, ori_addon_row, new_addon_row))) {
        free_row_store(*sk_row_meta_, new_row);
        LOG_WARN("failed to copy to row", K(ret));
      } else {
        new_row->set_addon_ptr(new_addon_row, *sk_row_meta_);
      }
    }
    return ret;
  }

private:
  common::ObIAllocator &allocator_;
  ObSqlMemMgrProcessor &sql_mem_processor_;
  const RowMeta *&sk_row_meta_;
  const RowMeta *&addon_row_meta_;
  int64_t &inmem_row_size_;
  const int64_t &topn_cnt_;
};
} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_STORE_ROW_FACTORY_H_ */