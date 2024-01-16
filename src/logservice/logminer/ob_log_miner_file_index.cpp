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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_file_index.h"
#include "logservice/logminer/ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

////////////////////////////// FileIndexItem //////////////////////////////

DEFINE_SERIALIZE(FileIndexItem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf ,buf_len, pos, "%ld:%ld:%ld\n",
      file_id_, range_.min_commit_ts_, range_.max_commit_ts_))) {
    LOG_ERROR("failed to filled data into buf when serializing file index item", KPC(this));
  }
  return ret;
}

DEFINE_DESERIALIZE(FileIndexItem)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(parse_int(buf, data_len, pos, file_id_))) {
    LOG_ERROR("failed to parse file_id", K(data_len), K(pos));
  } else if (OB_FAIL(expect_token(buf, data_len, pos, ":"))) {
    LOG_ERROR("failed tot get token ':'", K(data_len), K(pos));
  } else if (OB_FAIL(parse_int(buf, data_len, pos, range_.min_commit_ts_))) {
    LOG_ERROR("failed to parse min_commit_ts", K(data_len), K(pos));
  } else if (OB_FAIL(expect_token(buf, data_len, pos, ":"))) {
    LOG_ERROR("failed tot get token ':'", K(data_len), K(pos));
  } else if (OB_FAIL(parse_int(buf, data_len, pos, range_.max_commit_ts_))) {
    LOG_ERROR("failed to parse max_commit_ts", K(data_len), K(pos));
  } else if (OB_FAIL(expect_token(buf, data_len, pos, "\n"))) {
    LOG_ERROR("failed tot get token '\\n'", K(data_len), K(pos));
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(FileIndexItem)
{
  int64_t size = 0;
  const int64_t max_digit_len = 30;
  char digit_buf[max_digit_len];
  size += snprintf(digit_buf, max_digit_len, "%ld", file_id_);
  size += 1;
  size += snprintf(digit_buf, max_digit_len, "%ld", range_.min_commit_ts_);
  size += 1;
  size += snprintf(digit_buf, max_digit_len, "%ld", range_.max_commit_ts_);
  size += 1;
  return size;
}

////////////////////////////// FileIndex //////////////////////////////

int FileIndex::insert_index_item(const int64_t file_id,
    const int64_t min_commit_ts,
    const int64_t max_commit_ts)
{
  int ret = OB_SUCCESS;
  FileIndexItem *item;

  if (OB_FAIL(alloc_index_item_(item))) {
    LOG_ERROR("generate new index_item failed", K(item), K(file_id), K(min_commit_ts), K(max_commit_ts));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null index item", K(item));
  } else {
    item->reset(file_id, min_commit_ts, max_commit_ts);
    if (OB_FAIL(index_array_.push_back(item))) {
      LOG_ERROR("failed to push back item into index_array", KPC(item));
    } else {
      index_file_len_ += item->get_serialize_size();
    }
  }

  return ret;
}

int FileIndex::insert_index_item(const FileIndexItem &item)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(insert_index_item(item.file_id_, item.range_.min_commit_ts_, item.range_.max_commit_ts_))) {
    LOG_ERROR("failed to insert index_item", K(item));
  }

  return ret;
}

int FileIndex::get_index_item(const int64_t file_id, FileIndexItem *&item) const
{
  int ret = OB_SUCCESS;

  if (file_id >= index_array_.count() || file_id < 0) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_ERROR("file id is out of range", K(file_id), "arr_count", index_array_.count());
  } else {
    item = index_array_.at(file_id);
    if (OB_ISNULL(item)) {
      LOG_ERROR("get null item from index array", K(item), K(file_id), "arr_count", index_array_.count());
    } else if (item->file_id_ != file_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the item is not wanted, unexpected", KPC(item), K(file_id));
    }
  }

  return ret;
}

int64_t FileIndex::get_index_file_len() const
{
  return index_file_len_;
}

DEFINE_SERIALIZE(FileIndex)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(index_array_, idx)
  {
    FileIndexItem *item = index_array_.at(idx);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null item when calculating serialized size", K(idx), K(item));
    } else if (OB_FAIL(item->serialize(buf, buf_len, pos))) {
      LOG_ERROR("failed to serialize item into buf", K(buf_len), K(pos), K(idx));
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(FileIndex)
{
  int ret = OB_SUCCESS;
  FileIndexItem item;
  int64_t last_valid_pos = pos;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(item.deserialize(buf, data_len, pos))) {
      if (OB_INVALID_ARGUMENT != ret) {
        LOG_WARN("failed to deserialize file_index_item", K(data_len), K(pos));
      }
      ret = OB_SUCCESS;
      pos = last_valid_pos;
      done = true;
    } else if (OB_FAIL(insert_index_item(item))) {
      LOG_ERROR("failed to insert item into index array", K(item));
    } else {
      last_valid_pos = pos;
      if (pos >= data_len) {
        done = true;
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(FileIndex)
{
  int64_t size = 0;
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(index_array_, idx)
  {
    FileIndexItem *item = index_array_.at(idx);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null item when calculating serialized size", K(idx), K(item));
    } else {
      size += item->get_serialize_size();
    }
  }
  return size;
}


int FileIndex::alloc_index_item_(FileIndexItem *&item)
{
  int ret = OB_SUCCESS;

  void *item_buf = alloc_.alloc(sizeof(FileIndexItem));
  if (OB_ISNULL(item_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for file index item", K(item_buf));
  } else {
    item = new(item_buf) FileIndexItem;
  }

  return ret;
}

}
}