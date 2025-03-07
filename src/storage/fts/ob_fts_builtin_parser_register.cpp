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

#include "storage/fts/ob_fts_literal.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_whitespace_ft_parser.h"
#include "storage/fts/ob_ngram_ft_parser.h"
#include "storage/fts/ob_beng_ft_parser.h"
#include "storage/fts/ob_ik_ft_parser.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "lib/ob_errno.h"

using namespace oceanbase::plugin;
using namespace oceanbase::storage;

static int plugin_init(ObPluginParamPtr plugin)
{
  int ret = OBP_SUCCESS;
  if (OB_FAIL(ObFTParsePluginData::init_global())) {
    LOG_WARN("failed to init global ftparse plugin data", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_builtin_ftparser<ObWhiteSpaceFTParserDesc>(
                 plugin,
                 ObFTSLiteral::PARSER_NAME_SPACE,
                 "This is a default whitespace parser plugin."))) {
    LOG_WARN("failed to init whitespace builtin ftparser", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_builtin_ftparser<ObNgramFTParserDesc>(
                 plugin,
                 ObFTSLiteral::PARSER_NAME_NGRAM,
                 "This is a ngram fulltext parser plugin."))) {
    LOG_WARN("failed to init ngram builtin ftparser", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_builtin_ftparser<ObBasicEnglishFTParserDesc>(
                 plugin,
                 ObFTSLiteral::PARSER_NAME_BENG,
                 "This is a basic english parser plugin."))) {
    LOG_WARN("failed to init beng builtin ftparser", K(ret));
  } else if (OB_FAIL(ObPluginHelper::register_builtin_ftparser<ObIKFTParserDesc>(
                 plugin,
                 ObFTSLiteral::PARSER_NAME_IK,
                 "This is an ik parser plugin."))) {
    LOG_WARN("failed to init ik builtin ftparser", K(ret));
  }
  return ret;
}

static int plugin_deinit(ObPluginParamPtr /*plugin*/)
{
  ObFTParsePluginData::deinit_global();
  return OBP_SUCCESS;
}

OBP_DECLARE_PLUGIN(fts_parser)
{
  OBP_AUTHOR_OCEANBASE,
  OBP_MAKE_VERSION(0, 0, 1), // version 1 is compatible with current version
  OBP_LICENSE_MULAN_PUBL_V2,
  plugin_init,
  plugin_deinit
} OBP_DECLARE_PLUGIN_END;
