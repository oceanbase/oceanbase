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

#include "storage/ob_index_merge.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/object/ob_obj_compare.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_query_iterator_factory.h"
#include "storage/ob_dml_param.h"
namespace oceanbase {
using namespace common;
using namespace blocksstable;
namespace storage {
ObIndexMerge::ObIndexMerge()
    : index_iter_(NULL),
      main_iter_(NULL),
      rowkey_cnt_(0),
      index_param_(NULL),
      access_ctx_(NULL),
      table_iter_(),
      rowkeys_(),
      rowkey_allocator_(ObModIds::OB_SSTABLE_GET_SCAN),
      rowkey_range_idx_(),
      index_range_array_cursor_(0)
{}

ObIndexMerge::~ObIndexMerge()
{
  reset();
}

void ObIndexMerge::reset()
{
  index_iter_ = NULL;
  main_iter_ = NULL;
  index_param_ = NULL;
  access_ctx_ = NULL;
  table_iter_.reset();
  rowkeys_.reset();
  rowkey_allocator_.reset();
  rowkey_range_idx_.reset();
  index_range_array_cursor_ = 0;
}

void ObIndexMerge::reuse()
{
  table_iter_.reuse();
  index_range_array_cursor_ = 0;
}

int ObIndexMerge::open(ObQueryRowIterator& index_iter)
{
  int ret = OB_SUCCESS;
  main_iter_ = NULL;
  index_iter_ = &index_iter;
  return ret;
}

int ObIndexMerge::init(const ObTableAccessParam& param, const ObTableAccessParam& index_param,
    ObTableAccessContext& context, const ObGetTableParam& get_table_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_iter_.init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "Fail to init table iter, ", K(ret));
  } else {
    index_param_ = &index_param;
    access_ctx_ = &context;
    rowkey_cnt_ = param.iter_param_.rowkey_cnt_;
  }
  return ret;
}

int ObIndexMerge::get_next_row(ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == index_iter_) || OB_UNLIKELY(NULL == access_ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexMerge is not initiated", KP(index_iter_), K(ret));
  } else if (access_ctx_->is_end()) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret)) {
      if (NULL != main_iter_ && OB_SUCC(main_iter_->get_next_row(row))) {
        // find one row
        break;
      } else {
        if (OB_ITER_END == ret) {
          if (!access_ctx_->is_end()) {
            ret = OB_SUCCESS;
          }
          main_iter_ = NULL;
        }

        if (OB_SUCC(ret)) {
          // batch get main table rowkeys from index table
          ObStoreRow* index_row = NULL;
          ObStoreRowkey src_key;
          ObExtStoreRowkey dest_key;
          rowkeys_.reuse();
          rowkey_allocator_.reuse();
          table_iter_.reuse();
          access_ctx_->allocator_->reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < MAX_NUM_PER_BATCH; ++i) {
            if (OB_FAIL(index_iter_->get_next_row(index_row))) {
              if (OB_ARRAY_BINDING_SWITCH_ITERATOR == ret) {
                ++index_range_array_cursor_;
                if (OB_FAIL(index_iter_->switch_iterator(index_range_array_cursor_))) {
                  STORAGE_LOG(WARN, "fail to switch iterator", K(ret));
                }
              } else if (OB_ITER_END != ret) {
                STORAGE_LOG(WARN, "Fail to get next row from index iter, ", K(ret));
              }
            } else {
              src_key.assign(index_row->row_val_.cells_, rowkey_cnt_);
              if (OB_FAIL(src_key.deep_copy(dest_key.get_store_rowkey(), rowkey_allocator_))) {
                STORAGE_LOG(WARN, "Fail to deep copy rowkey, ", K(ret));
              } else {
                dest_key.set_range_array_idx(index_row->range_array_idx_);
                if (OB_FAIL(rowkeys_.push_back(dest_key))) {
                  STORAGE_LOG(WARN, "fail to push back rowkey", K(ret));
                }
              }
            }
          }

          if (OB_ITER_END == ret) {
            if (rowkeys_.count() > 0) {
              ret = OB_SUCCESS;
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(table_iter_.open(rowkeys_))) {
              STORAGE_LOG(WARN, "fail to open iterator", K(ret));
            } else {
              main_iter_ = &table_iter_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexMerge::switch_iterator(const int64_t range_array_idx)
{
  return table_iter_.switch_iterator(range_array_idx);
}

}  // namespace storage
}  // namespace oceanbase
