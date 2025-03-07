/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "oceanbase/ob_plugin.h"
#include "oceanbase/ob_plugin_charset_info.h"

/**
 * @defgroup ObPluginFtParser
 * @{
 */

/**
 * full text parser interface version
 * @note this is the minimum version.
 * You should add new version if you add new routines.
 */
#define OBP_FTPARSER_INTERFACE_VERSION OBP_MAKE_VERSION(0, 1, 0)

/**
 * current full text parser interface version
 * @note you should change this value if you add some new routines in interface struct.
 */
#define OBP_FTPARSER_INTERFACE_VERSION_CURRENT OBP_FTPARSER_INTERFACE_VERSION

#ifdef __cplusplus
extern "C" {
#endif

/**< full text parser parameter */
typedef ObPluginDatum ObPluginFTParserParamPtr;

/**< full text parser add word flag */
enum ObPluginFTPaserAddWordFlag
{
  OBP_FTPARSER_AWF_NONE           = 0LL,

  /**< filter words that are less than a minimum or greater than a maximum word length. */
  OBP_FTPARSER_AWF_MIN_MAX_WORD   = 1LL << 0,

  /**< filter by sotp word table. */
  OBP_FTPARSER_AWF_STOPWORD       = 1LL << 1,

  /**< convert characters from uppercase to lowercase. */
  OBP_FTPARSER_AWF_CASEDOWN       = 1LL << 2,

  /**< distinct and word aggregation */
  OBP_FTPARSER_AWF_GROUPBY_WORD   = 1LL << 3,
};

/**
 * fulltext parser descriptor interface for domain index
 * splitting a document into many tokenizations.
 */
struct OBP_PUBLIC_API ObPluginFTParser
{
  /**< this routine will be called when loading the library */
  int (*init)(ObPluginParamPtr param);

  /**< this routine will be called when unloading the library */
  int (*deinit)(ObPluginParamPtr param);

  int (*scan_begin)(ObPluginFTParserParamPtr param);   /**< this routine will be called before `next_token` */
  int (*scan_end)(ObPluginFTParserParamPtr param);     /**< this routine will be called after `next_token` */

  /**
   * this routine will be called many times except it return fail or OBP_ITER_END
   */
  int (*next_token)(ObPluginFTParserParamPtr param, char **word, int64_t *word_len, int64_t *char_cnt, int64_t *word_freq);

  /**< return the add_word_flag */
  int (*get_add_word_flag)(uint64_t *flag);
};

/**< return the version of this parser */
OBP_PUBLIC_API uint64_t obp_ftparser_parser_version(ObPluginFTParserParamPtr param);

/**
 * the `fulltext` is the text you should split it to tokens
 * @NOTE the `fulltext` is not terminated by '\0'.
 * You can use `obp_ftparser_fulltext_length` to get the length.
 */
OBP_PUBLIC_API const char * obp_ftparser_fulltext(ObPluginFTParserParamPtr param);

/**< the length of `fulltext` */
OBP_PUBLIC_API int64_t obp_ftparser_fulltext_length(ObPluginFTParserParamPtr param);

/**< get the charsetinfo object from param */
OBP_PUBLIC_API ObPluginCharsetInfoPtr obp_ftparser_charset_info(ObPluginFTParserParamPtr param);

/**
 * get the plugin parameter through fulltext parser parameter
 */
OBP_PUBLIC_API ObPluginParamPtr obp_ftparser_plugin_param(ObPluginFTParserParamPtr param);

/**
 * The user data of fulltext parameter
 * @details User data is a pointer (void *) so that you can set it to anything you want.
 */
OBP_PUBLIC_API ObPluginDatum obp_ftparser_user_data(ObPluginFTParserParamPtr param);

/**
 * set user data
 * @details You can retrieve user data by `obp_ftparser_user_data`
 */
OBP_PUBLIC_API void obp_ftparser_set_user_data(ObPluginFTParserParamPtr param, ObPluginDatum user_data);

/**
 * register fulltext parser plugin
 * @NOTE use `OBP_REGISTER_FTPARSER` instead
 * @param param the param of ObPluginParam which is a passed in param in `plugin::init`
 * @param name  fulltext parser name, 64 characters maximum
 * @param ftparser the ftparser struct. The object will be copied.
 * @param ftparser_sizeof the size of `ftparser`
 * @param description the description of the ftparser
 */
OBP_PUBLIC_API int obp_register_plugin_ftparser(ObPluginParamPtr param,
                                                const char *name,
                                                ObPluginVersion version,
                                                ObPluginFTParser *ftparser,
                                                int64_t ftparser_sizeof,
                                                const char *description);

#define OBP_REGISTER_FTPARSER(param, name, descriptor, description)    \
  obp_register_plugin_ftparser(param,                                  \
                               name,                                   \
                               OBP_FTPARSER_INTERFACE_VERSION_CURRENT, \
                               &descriptor,                            \
                               (int64_t)sizeof(descriptor),            \
                               description);

#ifdef __cplusplus
} // extern "C"
#endif

/** @} */
