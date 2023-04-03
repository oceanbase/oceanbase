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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/recursive_cte/ob_fake_cte_table_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObFakeCTETableSpec, ObOpSpec),
                      column_involved_offset_,
                      column_involved_exprs_);

int ObFakeCTETableOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("Failed to check physical plan status", K(ret));
  } else if (!has_valid_data()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(pump_row_)
            && OB_UNLIKELY(MY_SPEC.column_involved_exprs_.count() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump row is null", K(ret));
  } else if (OB_LIKELY(NULL != pump_row_)
            && OB_FAIL(pump_row_->to_expr(MY_SPEC.column_involved_exprs_, eval_ctx_))) {
    LOG_WARN("Stored row to expr failed", K(ret));
  } else {
    empty_ = true;
  }
  return ret;
}

int ObFakeCTETableOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  UNUSED(max_row_cnt);
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (ret == OB_ITER_END) {
      brs_.end_ = true;
      brs_.size_ = 0;
      ret = OB_SUCCESS;
    } else {
      LOG_INFO("Failed to get result from cte table", K(ret));
    }
  } else {
    brs_.size_ = 1;
  }
  return ret;
}


void ObFakeCTETableOp::reuse()
{
  pump_row_ = nullptr;
  empty_ = true;
}

int ObFakeCTETableOp::add_row(ObChunkDatumStore::StoredRow *row)
{
  int ret = OB_SUCCESS;
  /**
   * R union 返回的数据是按照fake cte table的所有基准列来返回的
   * fake cte table的基准列不一定是全部被使用了的，所以要把StoredRow被使用的cell拷贝到new StoredRow中
   */
  const ObChunkDatumStore::StoredRow *new_row = nullptr;
  ObChunkDatumStore::StoredRow *old_row = nullptr;
  if (OB_UNLIKELY(0 == MY_SPEC.column_involved_offset_.count())) {
    empty_ = false;
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fake cte table add row count != output_ count", KPC(row));
  } else if (OB_FAIL(deep_copy_row(row, new_row, MY_SPEC.column_involved_offset_,
                                    ObSearchMethodOp::ROW_EXTRA_SIZE, allocator_))) {
    LOG_WARN("fail to deep copy stored row", K(ret));
  } else {
    old_row = const_cast<ObChunkDatumStore::StoredRow *>(pump_row_);
    pump_row_ = new_row;
    empty_ = false;
    if (nullptr != old_row) {
      allocator_.free(old_row);
    }
  }
  return ret;
}

int ObFakeCTETableOp::inner_rescan()
{
  int ret = ObOperator::inner_rescan();
  if (pump_row_ != nullptr) {
    empty_ = false;
  }
  return ret;
}

int ObFakeCTETableOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.column_involved_exprs_.count() != MY_SPEC.column_involved_offset_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fake cte table spec", K(ret));
  }
  return ret;
}

int ObFakeCTETableOp::inner_close()
{
  int ret = OB_SUCCESS;
  reuse();
  return ret;
}

int ObFakeCTETableOp::copy_datums(ObChunkDatumStore::StoredRow *row, common::ObDatum *datums,
  int64_t cnt, const common::ObIArray<int64_t> &chosen_datums, char *buf, const int64_t size,
  const int64_t row_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row || row->payload_ != buf
                  || size < 0 || nullptr == datums || chosen_datums.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(size), K(datums));
  } else {
    row->cnt_ = static_cast<uint32_t>(chosen_datums.count());
    int64_t pos = sizeof(ObDatum) * row->cnt_ + row_extend_size;
    row->row_size_ = static_cast<int32_t>(row_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < row->cnt_; ++i) {
      int64_t idx = chosen_datums.at(i);
      if (OB_UNLIKELY(idx >= cnt)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(row->payload_), KP(buf),
                  K(size), K(datums), K(idx), K(cnt));
      } else {
        ObDatum *datum = new (&row->cells()[i])ObDatum();
        if (OB_FAIL(datum->deep_copy(datums[idx], buf, size, pos))) {
          LOG_WARN("failed to copy datum", K(ret), K(i), K(pos),
            K(size), K(row_size), K(datums[idx]), K(datums[idx].len_));
        }
      }
    }
  }
  return ret;
}

int ObFakeCTETableOp::deep_copy_row(const ObChunkDatumStore::StoredRow *src_row,
                                    const ObChunkDatumStore::StoredRow *&dst_row,
                                    const ObIArray<int64_t> &chosen_index,
                                    int64_t extra_size,
                                    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row) || OB_ISNULL(src_row->cells())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src row is null", K(ret), K(src_row));
  } else if (chosen_index.empty()) {
    dst_row = nullptr;
  } else {
    char *buf = nullptr;
    int64_t row_size = sizeof(ObDatum) * chosen_index.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < chosen_index.count(); i++) {
      int64_t idx = chosen_index.at(i);
      if (OB_UNLIKELY(idx >= src_row->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("chosen index greater than src_row count", K(ret),
                  K(chosen_index), K(src_row->cnt_));
      } else {
        row_size += src_row->cells()[idx].len_;
      }
    }
    if (OB_SUCC(ret)) {
      int64_t buffer_len = 0;
      int64_t head_size = sizeof(ObChunkDatumStore::StoredRow);
      int64_t pos = head_size;
      ObChunkDatumStore::StoredRow *new_row = nullptr;
      buffer_len = row_size + head_size + extra_size;
      if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buffer_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed", K(ret));
      } else if (OB_ISNULL(new_row = new(buf)ObChunkDatumStore::StoredRow())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to new row", K(ret));
      } else if (OB_FAIL(copy_datums(new_row, const_cast<ObDatum *>(src_row->cells()),
                                    src_row->cnt_, chosen_index, buf + pos,
                                    buffer_len - head_size, row_size, extra_size))) {
          LOG_WARN("failed to deep copy row", K(ret), K(buffer_len), K(row_size));
      } else {
        dst_row = new_row;
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
