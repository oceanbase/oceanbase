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
#include "lib/container/ob_se_array.h"
#ifdef OB_USE_DRCMSG
#include <drcmsg/MD.h>
#include <drcmsg/MsgWrapper.h>
#endif

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

int serialize_merge_delete_br(
    IBinlogRecord &del_data,
    const char *&data,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  data = nullptr;
  data_len = 0;
#ifdef OB_USE_DRCMSG
  static thread_local DrcMsgBuf message_buf;
#else
  static thread_local LogMsgBuf message_buf;
#endif
  size_t serialized_len = 0;
  const char *serialized_data = nullptr;

  if (OB_UNLIKELY(EDELETE != del_data.recordType()) || OB_ISNULL(del_data.getTableMeta())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid delete br for merge serialization", KR(ret), "record_type", del_data.recordType());
  } else if (OB_ISNULL(serialized_data = del_data.toString(&serialized_len, &message_buf))
      || OB_UNLIKELY(0 == serialized_len || serialized_len > INT64_MAX)) {
    ret = OB_SERIALIZE_ERROR;
    LOG_ERROR("serialize merge delete br failed", KR(ret), KP(serialized_data), K(serialized_len));
  } else {
    data = serialized_data;
    data_len = static_cast<int64_t>(serialized_len);
  }

  return ret;
}

int apply_serialized_merge_delete_br(
    const char *data,
    const int64_t data_len,
    ObIAllocator &allocator,
    IBinlogRecord &ins_data)
{
  int ret = OB_SUCCESS;
  int br_ret = 0;
  ObLogSerilizedBR del_br;
  IBinlogRecord *del_data = del_br.get_data();
  IStrArray *old_cols = nullptr;
  const uint8_t *origins = nullptr;
  size_t origin_count = 0;
  unsigned int existing_old_count = 0;
  ins_data.oldCols(existing_old_count);
  ITableMeta *table_meta = ins_data.getTableMeta();
  ObSEArray<ObString, 2> old_values;

  if (OB_ISNULL(data) || OB_UNLIKELY(data_len <= 0) || OB_ISNULL(table_meta)
      || OB_UNLIKELY(EINSERT != ins_data.recordType() || 0 != existing_old_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument for merge delete br", KR(ret), KP(data), K(data_len),
        KP(table_meta), K(existing_old_count), "record_type", ins_data.recordType());
  } else if (OB_ISNULL(del_data)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create parsed merge delete br failed", KR(ret));
  } else if (0 != (br_ret = del_data->parse(data, data_len)) || !del_data->parsedOK()) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_ERROR("parse merge delete br failed", KR(ret), K(br_ret), K(data_len));
  } else if (OB_UNLIKELY(EDELETE != del_data->recordType())
      || OB_ISNULL(old_cols = del_data->parsedOldCols())) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid persisted merge delete br", KR(ret), "record_type", del_data->recordType(), KP(old_cols));
  } else {
    const int64_t column_count = table_meta->getColCount();
    origins = del_data->parsedOldValueOrigins(origin_count);
    if (OB_UNLIKELY(column_count <= 0 || old_cols->size() != column_count || origin_count != column_count)
        || OB_ISNULL(origins)) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("merge delete columns or origins do not match insert", KR(ret), K(column_count),
          "old_col_count", old_cols->size(), K(origin_count), KP(origins));
    }

    // putOld() retains the pointer. Copy values before destroying the parsed BR,
    // and finish all fallible preparation before appending any old columns.
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      const char *value = nullptr;
      size_t value_len = 0;
      ObString old_value;
      if (0 != (br_ret = old_cols->elementAt(static_cast<int>(i), value, value_len))) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("get parsed merge old column failed", KR(ret), K(br_ret), K(i));
      } else if (OB_UNLIKELY(origins[i] > static_cast<uint8_t>(PADDING))) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("invalid merge old column origin", KR(ret), K(i), K(origins[i]));
      } else if (OB_ISNULL(value)) {
        if (OB_UNLIKELY(0 != value_len)) {
          ret = OB_INVALID_DATA;
          LOG_ERROR("invalid null merge old column length", KR(ret), K(i), K(value_len));
        }
      } else if (OB_UNLIKELY(0 == value_len || value_len - 1 > INT32_MAX || '\0' != value[value_len - 1])) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("invalid parsed merge old column length or terminator", KR(ret), K(i), K(value_len));
      } else {
        char *buf = static_cast<char *>(allocator.alloc(value_len));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc merge old column failed", KR(ret), K(i), K(value_len));
        } else {
          MEMCPY(buf, value, value_len);
          // The BR wire format includes a terminator for every non-NULL value,
          // including empty strings. putOld() expects the length without it.
          old_value.assign_ptr(buf, static_cast<int32_t>(value_len - 1));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(old_values.push_back(old_value))) {
        LOG_ERROR("save merge old column failed", KR(ret), K(i));
        if (OB_NOT_NULL(old_value.ptr())) {
          allocator.free(old_value.ptr());
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_values.count(); ++i) {
    const ObString &value = old_values.at(i);
    if (0 != (br_ret = ins_data.putOld(value.ptr(), value.length(), static_cast<VALUE_ORIGIN>(origins[i])))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("put parsed merge old column failed", KR(ret), K(br_ret), K(i));
      ins_data.clearOld();
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < old_values.count(); ++i) {
      if (OB_NOT_NULL(old_values.at(i).ptr())) {
        allocator.free(old_values.at(i).ptr());
      }
    }
  }
  if (OB_NOT_NULL(old_cols)) {
    delete old_cols;
    old_cols = nullptr;
  }
  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
