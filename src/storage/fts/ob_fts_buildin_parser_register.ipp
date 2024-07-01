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

#ifndef OB_FTS_BUILD_IN_PARSER_REGISTER_H_
#define OB_FTS_BUILD_IN_PARSER_REGISTER_H_

#include "storage/fts/ob_whitespace_ft_parser.h"
#include "storage/fts/ob_ngram_ft_parser.h"
#include "storage/fts/ob_beng_ft_parser.h"

///////////////////////////////////// Default fulltext parser //////////////////////////////////////////

OB_DECLARE_PLUGIN(whitespace_parser)
{
  oceanbase::lib::ObPluginType::OB_FT_PARSER_PLUGIN,             // fulltext parser type
  "space",                                                       // name
  OB_PLUGIN_AUTHOR_OCEANBASE,                                    // author
  "This is a default whitespace parser plugin.",                 // brief specification
  0x00001,                                                       // version
  oceanbase::lib::ObPluginLicenseType::OB_Mulan_PubL_V2_LICENSE, // Mulan PubL v2 license
  &oceanbase::storage::whitespace_parser,                        // default space parser plugin instance
};

OB_DECLARE_BUILDIN_PLUGIN_HANDLER(ObBuildInWhitespaceFTParser, whitespace_parser);

///////////////////////////////////// Ngram fulltext parser //////////////////////////////////////////

OB_DECLARE_PLUGIN(ngram_parser)
{
  oceanbase::lib::ObPluginType::OB_FT_PARSER_PLUGIN,             // fulltext parser type
  "ngram",                                                       // name
  OB_PLUGIN_AUTHOR_OCEANBASE,                                    // author
  "This is a ngram fulltext parser plugin.",                     // brief specification
  0x00001,                                                       // version
  oceanbase::lib::ObPluginLicenseType::OB_Mulan_PubL_V2_LICENSE, // Mulan PubL v2 license
  &oceanbase::storage::ngram_parser,                             // ngram parser plugin instance
};

OB_DECLARE_BUILDIN_PLUGIN_HANDLER(ObBuildInNgramFTParser, ngram_parser);

///////////////////////////////////// BEng fulltext parser //////////////////////////////////////////

OB_DECLARE_PLUGIN(beng_parser)
{
  oceanbase::lib::ObPluginType::OB_FT_PARSER_PLUGIN,             // fulltext parser type
  "beng",                                                        // name
  OB_PLUGIN_AUTHOR_OCEANBASE,                                    // author
  "This is a basic english parser plugin.",                      // brief specification
  0x00001,                                                       // version
  oceanbase::lib::ObPluginLicenseType::OB_Mulan_PubL_V2_LICENSE, // Mulan PubL v2 license
  &oceanbase::storage::beng_parser,                              // basic english parser plugin instance
};

OB_DECLARE_BUILDIN_PLUGIN_HANDLER(ObBuildInBEngFTParser, beng_parser);

#endif // OB_FTS_BUILD_IN_PARSER_REGISTER_H_
