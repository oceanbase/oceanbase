/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef MOCK_FT_PARSER_H_
#define MOCK_FT_PARSER_H_

#include "plugin/interface/ob_plugin_ftparser_intf.h"

namespace oceanbase
{
namespace storage
{

class ObMockFTParserDesc final : public plugin::ObIFTParserDesc
{
public:
  ObMockFTParserDesc() = default;
  virtual ~ObMockFTParserDesc() = default;
  virtual int init(plugin::ObPluginParam *param) override;
  virtual int deinit(plugin::ObPluginParam *param) override;
  virtual int segment(plugin::ObFTParserParam *param, plugin::ObITokenIterator *&iter) const override;
};

int ObMockFTParserDesc::init(plugin::ObPluginParam *param)
{
  UNUSEDx(param);
  return OB_SUCCESS;
}

int ObMockFTParserDesc::deinit(plugin::ObPluginParam *param)
{
  UNUSED(param);
  return OB_SUCCESS;
}

int ObMockFTParserDesc::segment(plugin::ObFTParserParam *param, plugin::ObITokenIterator *&iter) const
{
  UNUSED(param);
  return OB_SUCCESS;
}

static ObMockFTParserDesc mock_ft_parser;

} // end storage
} // end oceanbase

#endif // MOCK_FT_PARSER_H_
