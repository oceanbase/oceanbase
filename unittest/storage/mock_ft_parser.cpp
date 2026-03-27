/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "mock_ft_parser.h"

OB_DECLARE_PLUGIN(mock_ft_parser)
{
  oceanbase::lib::ObPluginType::OB_FT_PARSER_PLUGIN,
  "mock_ft_parser",
  OB_PLUGIN_AUTHOR_OCEANBASE,
  "This is mock fulltext parser plugin.",
  0x00001,
  oceanbase::lib::ObPluginLicenseType::OB_Mulan_PubL_V2_LICENSE,
  &oceanbase::storage::mock_ft_parser,
};
