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

void fts_parse_docment(const char *input, void * pool, FtsParserResult *ss)
{
    void *scanner = NULL;
    ss->ret_ = 0;
    ss->err_info_.str_ = NULL;
    ss->err_info_.len_ = 0;
    ss->root_ = NULL;
    ss->list_.head = NULL;
    ss->list_.tail = NULL;
    ss->charset_info_ = NULL;
    ss->malloc_pool_ = pool;
    obsql_fts_yylex_init_extra(ss, &scanner);
    YY_BUFFER_STATE bufferState = obsql_fts_yy_scan_string(input, scanner);  // 读取字符串
    ss->yyscanner_ = scanner;
    obsql_fts_yyparse(ss);  // 调用语法分析器
    obsql_fts_yy_delete_buffer(bufferState, scanner);  // 删除缓冲区
    obsql_fts_yylex_destroy(scanner);
}
