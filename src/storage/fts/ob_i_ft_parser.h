/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
