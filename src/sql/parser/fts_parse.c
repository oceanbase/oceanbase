/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "fts_parse.h"
#include "ftsblex_lex.h"
#include "ftsparser_tab.h"  // Bison 生成的头文件


extern int obsql_fts_yyparse(void* yyscanner);

static const char FTS_INVALID_INPUT_ERR_MSG[] =
    "fulltext boolean query input is invalid";
static const char FTS_NESTING_DEPTH_ERR_MSG[] =
    "fulltext boolean query with nesting depth exceeding the maximum of 256";

static int fts_check_boolean_nesting_depth(FtsParserResult *ss, const char *input, const int length)
{
    int ret = FTS_OK;
    int32_t depth = 0;
    int i = 0;
    if (OB_UNLIKELY(NULL == input || length < 0)) {
        ret = FTS_ERROR_INVALID_ARGUMENT;
        ss->err_info_.str_ = (char *)FTS_INVALID_INPUT_ERR_MSG;
        ss->err_info_.len_ = sizeof(FTS_INVALID_INPUT_ERR_MSG) - 1;
    }
    for (i = 0; FTS_OK == ret && i < length; ++i) {
        if ('(' == input[i]) {
            if (OB_UNLIKELY(++depth > FTS_MAX_BOOLEAN_NESTING_DEPTH)) {
                ret = FTS_ERROR_INVALID_ARGUMENT;
                ss->err_info_.str_ = (char *)FTS_NESTING_DEPTH_ERR_MSG;
                ss->err_info_.len_ = sizeof(FTS_NESTING_DEPTH_ERR_MSG) - 1;
            }
        } else if (')' == input[i] && depth > 0) {
            --depth;
        }
    }
    return ret;
}

void fts_parse_docment(const char *input, const int length, void * pool, FtsParserResult *ss)
{
    void *scanner = NULL;
    YY_BUFFER_STATE bufferState = NULL;
    ss->ret_ = FTS_OK;
    ss->err_info_.str_ = NULL;
    ss->err_info_.len_ = 0;
    ss->yyscanner_ = NULL;
    ss->root_ = NULL;
    ss->list_.head = NULL;
    ss->list_.tail = NULL;
    ss->charset_info_ = NULL;
    ss->malloc_pool_ = pool;
    ss->ret_ = fts_check_boolean_nesting_depth(ss, input, length);
    if (FTS_OK == ss->ret_) {
        obsql_fts_yylex_init_extra(ss, &scanner);
        bufferState = obsql_fts_yy_scan_bytes(input, length, scanner);  // 读取字符串
        ss->yyscanner_ = scanner;
        obsql_fts_yyparse(ss);  // 调用语法分析器
        obsql_fts_yy_delete_buffer(bufferState, scanner);  // 删除缓冲区
        obsql_fts_yylex_destroy(scanner);
    }
}
