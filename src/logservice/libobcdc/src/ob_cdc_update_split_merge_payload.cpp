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

#define USING_LOG_PREFIX OBLOG_SORTER

#include "ob_cdc_update_split_merge_payload.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

OB_SERIALIZE_MEMBER(MergeOldColValue, data_, origin_);

OB_SERIALIZE_MEMBER(MergeOldColsPayload, old_cols_);

int MergeOldColsPayload::init_from_delete(IBinlogRecord &del_data, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  unsigned int old_col_count = 0;
  binlogBuf *old_cols = del_data.oldCols(old_col_count);
  old_cols_.reuse();

  if (OB_UNLIKELY(old_col_count > 0 && OB_ISNULL(old_cols))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("old cols array is null", KR(ret), K(old_col_count));
  }

  for (unsigned int i = 0; OB_SUCC(ret) && i < old_col_count; ++i) {
    ObString old_col;
    const int64_t old_col_len = static_cast<int64_t>(old_cols[i].buf_used_size);
    char *old_col_buf = nullptr;
    if (OB_UNLIKELY(old_col_len < 0)) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("invalid old column length", KR(ret), K(i), K(old_col_len));
    } else if (OB_UNLIKELY(old_col_len > 0 && OB_ISNULL(old_cols[i].buf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("old column buffer is null", KR(ret), K(i), K(old_col_len), K(old_cols[i].m_origin));
    } else if (old_col_len > 0) {
      old_col_buf = static_cast<char *>(allocator.alloc(old_col_len));
      if (OB_ISNULL(old_col_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc old column payload buffer failed", KR(ret), K(i), K(old_col_len));
      } else {
        MEMCPY(old_col_buf, old_cols[i].buf, old_col_len);
      }
    }

    if (OB_SUCC(ret)) {
      old_col.assign_ptr(old_col_buf, old_col_len);
      if (OB_FAIL(old_cols_.push_back(MergeOldColValue(old_col, static_cast<uint8_t>(old_cols[i].m_origin))))) {
        LOG_ERROR("push back old column payload failed", KR(ret), K(i), K(old_col), K(old_cols[i].m_origin));
      }
    }
  }

  return ret;
}

int MergeOldColsPayload::apply_to_insert(IBinlogRecord &ins_data) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < old_cols_.count(); ++i) {
    const MergeOldColValue &old_col = old_cols_.at(i);
    if (OB_FAIL(ins_data.putOld(old_col.data_.ptr(),
            static_cast<int>(old_col.data_.length()),
            static_cast<VALUE_ORIGIN>(old_col.origin_)))) {
      LOG_ERROR("put old column from payload failed", KR(ret), K(i), K(old_col));
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
