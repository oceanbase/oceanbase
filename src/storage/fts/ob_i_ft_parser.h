/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_I_FT_PARSER_H_
#define OB_I_FT_PARSER_H_

#include "plugin/interface/ob_plugin_ftparser_intf.h"

namespace oceanbase
{
namespace storage
{
class ObIFTParser : public plugin::ObITokenIterator
{
public:
  ObIFTParser() = default;
  virtual ~ObIFTParser() = default;
  virtual int reuse_parser(const char *fulltext, const int64_t fulltext_len) = 0;
};
} // end namespace storage
} // end namespace oceanbase

#endif// OB_I_FT_PARSER_H_
