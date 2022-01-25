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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_ENTRY_PARSER_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_ENTRY_PARSER_

#include "../clog_tool/ob_log_entry_parser.h"
#include "../clog_tool/ob_func_utils.h"
#include "archive/ob_archive_entry_iterator.h"

namespace oceanbase
{
namespace archive
{

class ObArchiveEntryParser : public clog::ObLogEntryParserImpl
{
public:
  ObArchiveEntryParser() : is_inited_(false),
    file_id_(0),
    tenant_id_(0) {}
  virtual ~ObArchiveEntryParser() {}

  int init(uint64_t file_id, const uint64_t tenant_id, const common::ObString &host,
           const int32_t port);
  bool is_inited() const {return is_inited_;}
  int dump_all_entry_1();
  int dump_all_entry(const char *path);

 TO_STRING_KV(K(is_inited_),
              K(file_id_),
              K(tenant_id_));
protected:
  int parse_next_entry_();
  int get_entry_type_(ObArchiveItemType &item_type);
  void advance_(const int64_t step);
  void skip_block_offset_tail_();

protected:
  static const int64_t MAGIC_NUM_LEN = 2L;
  static const int64_t PRINT_BUF_SIZE = 5 * 1024 * 1024;
  bool is_inited_;
  uint64_t file_id_;
  uint64_t tenant_id_;

  DISALLOW_COPY_AND_ASSIGN(ObArchiveEntryParser);
};
}//end of namespace clog
}//end of namespace oceanbase

#endif  //OCEANBASE_ARCHIVE_OB_ARCHIVE_ENTRY_PARSER_
