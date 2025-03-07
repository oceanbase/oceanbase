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

#include "ob_parser_charset_utils.h"
#include "lib/charset/ob_charset.h"

#ifdef __cplusplus
extern "C"
{
#endif

namespace  oceanbase{

int obcharset_is_gb_charset_of_collation(ObCollationType collation_type, bool *is_gb) {
    int ret = OB_SUCCESS;
    *is_gb = false;
    if (collation_type == CS_TYPE_GBK_CHINESE_CI ||
        collation_type == CS_TYPE_GBK_BIN ||
        collation_type == CS_TYPE_GB2312_CHINESE_CI ||
        collation_type == CS_TYPE_GB2312_BIN ||
        collation_type == CS_TYPE_GB18030_CHINESE_CI ||
        collation_type == CS_TYPE_GB18030_BIN ||
        collation_type == CS_TYPE_GB18030_CHINESE_CS ||
        (collation_type >= CS_TYPE_GB18030_2022_BIN &&
         collation_type <= CS_TYPE_GB18030_2022_STROKE_CS)) {
        *is_gb = true;
    }
    return ret;
}

int obcharset_is_single_byte_charset_of_collation(ObCollationType collation_type, bool *is_single_byte) {
    int ret = OB_SUCCESS;
    *is_single_byte = false;
    if (collation_type == CS_TYPE_LATIN1_GERMAN1_CI ||
        collation_type == CS_TYPE_LATIN1_SWEDISH_CI ||
        collation_type == CS_TYPE_LATIN1_DANISH_CI ||
        collation_type == CS_TYPE_LATIN1_GERMAN2_CI ||
        collation_type == CS_TYPE_LATIN1_BIN ||
        collation_type == CS_TYPE_LATIN1_GENERAL_CI ||
        collation_type == CS_TYPE_LATIN1_GENERAL_CS ||
        collation_type == CS_TYPE_LATIN1_SPANISH_CI ||
        collation_type == CS_TYPE_ASCII_GENERAL_CI ||
        collation_type == CS_TYPE_ASCII_BIN ||
        collation_type == CS_TYPE_TIS620_BIN ||
        collation_type == CS_TYPE_TIS620_THAI_CI ||
        collation_type == CS_TYPE_DEC8_BIN ||
        collation_type == CS_TYPE_DEC8_SWEDISH_CI ||
        collation_type == CS_TYPE_CP850_GENERAL_CI ||
        collation_type == CS_TYPE_CP850_BIN ||
        collation_type == CS_TYPE_HP8_ENGLISH_CI ||
        collation_type == CS_TYPE_HP8_BIN ||
        collation_type == CS_TYPE_MACROMAN_GENERAL_CI ||
        collation_type == CS_TYPE_MACROMAN_BIN ||
        collation_type == CS_TYPE_SWE7_SWEDISH_CI ||
        collation_type == CS_TYPE_SWE7_BIN) {
        *is_single_byte = true;
    }
    return ret;
}

int obcharset_is_utf8_charset_of_collation(ObCollationType collation_type, bool *is_utf8) {
    int ret = OB_SUCCESS;
    *is_utf8 = false;
    if (collation_type == CS_TYPE_UTF8MB4_GENERAL_CI ||
        collation_type == CS_TYPE_UTF8MB4_BIN ||
        (collation_type >= CS_TYPE_UTF8MB4_UNICODE_CI &&
         collation_type <= CS_TYPE_UTF8MB4_VIETNAMESE_CI) ||
        collation_type == CS_TYPE_BINARY ||
        (collation_type >= CS_TYPE_UTF8MB4_0900_AI_CI &&
         collation_type <= CS_TYPE_UTF8MB4_MN_CYRL_0900_AS_CS)
    ) {
        *is_utf8 = true;
    }
    return ret;
}

int obcharset_get_parser_type_by_coll(const int collation_type, ObCharsetParserType *parser_type) {
    int ret = OB_SUCCESS;
    bool is_gb = false;
    bool is_single_byte = false;
    bool is_utf8 = false;
    ObCollationType coll_type = static_cast<ObCollationType>(collation_type);
    if (OB_ISNULL(parser_type)) {
        ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(obcharset_is_gb_charset_of_collation(coll_type, &is_gb))) {
        /* do nothing */
    } else if (is_gb) {
        *parser_type = CHARSET_PARSER_TYPE_GB;
    } else if (coll_type == CS_TYPE_HKSCS_BIN || coll_type == CS_TYPE_HKSCS31_BIN) {
        *parser_type = CHARSET_PARSER_TYPE_HKSCS;
    } else if (OB_FAIL(obcharset_is_single_byte_charset_of_collation(coll_type, &is_single_byte))) {
        /* do nothing */
    } else if (is_single_byte) {
        *parser_type = CHARSET_PARSER_TYPE_SINGLE_BYTE;
    } else if (OB_FAIL(obcharset_is_utf8_charset_of_collation(coll_type, &is_utf8))){
        /* do nothing */
    } else if (is_utf8) {
        *parser_type = CHARSET_PARSER_TYPE_UTF8MB4;
    } else {
        ret = -1;
    }
    return ret;
}
}

#ifdef __cplusplus
}
#endif
