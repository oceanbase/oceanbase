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
