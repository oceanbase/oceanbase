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

#ifndef MOCK_FT_PARSER_H_
#define MOCK_FT_PARSER_H_

#include "lib/ob_plugin.h"

namespace oceanbase
{
namespace storage
{

class ObMockFTParserDesc final : public lib::ObIFTParserDesc
{
public:
  ObMockFTParserDesc() = default;
  virtual ~ObMockFTParserDesc() = default;
  virtual int init(lib::ObPluginParam *param) override;
  virtual int deinit(lib::ObPluginParam *param) override;
  virtual int segment(lib::ObFTParserParam *param, lib::ObITokenIterator *&iter) const override;
};

int ObMockFTParserDesc::init(lib::ObPluginParam *param)
{
  UNUSEDx(param);
  return OB_SUCCESS;
}

int ObMockFTParserDesc::deinit(lib::ObPluginParam *param)
{
  UNUSED(param);
  return OB_SUCCESS;
}

int ObMockFTParserDesc::segment(lib::ObFTParserParam *param, lib::ObITokenIterator *&iter) const
{
  UNUSED(param);
  return OB_SUCCESS;
}

static ObMockFTParserDesc mock_ft_parser;

} // end storage
} // end oceanbase

#endif // MOCK_FT_PARSER_H_
