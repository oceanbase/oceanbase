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

#ifndef OCEANBASE_TOOL_iLOG_ENTRY_PARSER
#define OCEANBASE_TOOL_iLOG_ENTRY_PARSER

#include "ob_func_utils.h"
#include "clog/ob_log_block.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_info_block_handler.h"
//#include "lib/allocator/page_arena.h"

namespace oceanbase {
namespace clog {

class ObILogEntryParser {
public:
  ObILogEntryParser()
      : is_inited_(false), file_id_(OB_INVALID_FILE_ID), buf_(NULL), buf_len_(0), trailer_(), index_info_block_map_()
  {}
  virtual ~ObILogEntryParser()
  {}

  int init(file_id_t file_id, char* buf, int64_t buf_len);
  int parse_all_entry();

private:
  class DumpIlogEntryFunctor;

private:
  int resolve_trailer_and_info_block_map();
  int dump_all_entry();

private:
  static const int64_t PRINT_BUF_SIZE = 5 * 1024 * 1024;
  bool is_inited_;
  file_id_t file_id_;
  char* buf_;
  int64_t buf_len_;
  ObIlogFileTrailerV2 trailer_;
  IndexInfoBlockMap index_info_block_map_;

  DISALLOW_COPY_AND_ASSIGN(ObILogEntryParser);
};

}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_RAW_ENTRY_ITERATOR_
