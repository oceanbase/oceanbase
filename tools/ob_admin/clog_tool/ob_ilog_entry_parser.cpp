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

#include <string.h>
#include "ob_ilog_entry_parser.h"

namespace oceanbase {
using namespace common;
using namespace memtable;
namespace clog {

class ObILogEntryParser::DumpIlogEntryFunctor {
public:
  DumpIlogEntryFunctor(file_id_t file_id, char* buf, int64_t len) : file_id_(file_id), buf_(buf), buf_len_(len)
  {}
  ~DumpIlogEntryFunctor()
  {}
  bool operator()(const common::ObPartitionKey& partition_key, const IndexInfoBlockEntry& index_info_block_entry)
  {
    int ret = OB_SUCCESS;
    bool bool_ret = true;
    offset_t start_offset = index_info_block_entry.start_offset_;
    int64_t cur_pos = start_offset;
    ObLogCursorExt cursor;
    // int64_t cursor_size = cursor.get_serialize_size();
    for (uint64_t log_id = index_info_block_entry.min_log_id_;
         OB_SUCC(ret) && bool_ret && log_id <= index_info_block_entry.max_log_id_;
         ++log_id) {
      if (OB_FAIL(cursor.deserialize(buf_, buf_len_, cur_pos))) {
        bool_ret = false;
      } else {
        fprintf(stdout,
            "ilog_file_id: %d INDEX_LOG: pk:%s log_id:%lu %s||\n",
            file_id_,
            to_cstring(partition_key),
            log_id,
            to_cstring(cursor));
      }
    }
    return bool_ret;
  }

private:
  file_id_t file_id_;
  char* buf_;
  int64_t buf_len_;
};

int ObILogEntryParser::init(file_id_t file_id, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(OB_INVALID_FILE_ID == file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalie buf or buf_len", KP(buf), K(buf_len), K(file_id), K(ret));
  } else {
    is_inited_ = true;
    file_id_ = file_id;
    buf_ = buf;
    buf_len_ = buf_len;
  }
  return ret;
}

int ObILogEntryParser::parse_all_entry()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_trailer_and_info_block_map())) {
    CLOG_LOG(ERROR, "failed to resolve trailer and info block map", K(ret));
  } else if (OB_FAIL(dump_all_entry())) {
    CLOG_LOG(ERROR, "failed to dump all entry", K(ret));
  }
  return ret;
}

int ObILogEntryParser::resolve_trailer_and_info_block_map()
{
  int ret = OB_SUCCESS;
  int64_t pos = buf_len_ - CLOG_TRAILER_SIZE;
  if (OB_FAIL(trailer_.deserialize(buf_, buf_len_, pos))) {
    CLOG_LOG(ERROR, "index_info_block_map init failed", K(ret));
  } else {
    CLOG_LOG(INFO, "DEBUG", K(trailer_));
    const int64_t info_block_size = upper_align(trailer_.get_info_block_size(), CLOG_DIO_ALIGN_SIZE);
    const int64_t MAX_ENTRY_NUM = info_block_size / index_info_block_map_.item_size() + 1000;
    int64_t local_pos = 0;
    if (OB_FAIL(index_info_block_map_.init(ObModIds::OB_CLOG_INFO_BLK_HNDLR, MAX_ENTRY_NUM))) {
      CLOG_LOG(ERROR, "index_info_block_map init failed", K(ret));
    } else if (OB_FAIL(index_info_block_map_.deserialize(
                   buf_ + trailer_.get_info_block_start_offset(), info_block_size, local_pos))) {
      CLOG_LOG(ERROR, "index_info_block_map deserialize failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObILogEntryParser::dump_all_entry()
{
  int ret = OB_SUCCESS;
  DumpIlogEntryFunctor functor(file_id_, buf_, trailer_.get_info_block_start_offset());
  if (OB_FAIL(index_info_block_map_.for_each(functor))) {
    CLOG_LOG(ERROR, "index_info_block_map_ for_each failed", K(ret));
  }
  return ret;
}
}  // namespace clog
}  // end of namespace oceanbase
