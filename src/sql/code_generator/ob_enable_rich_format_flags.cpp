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

#include "ob_enable_rich_format_flags.h"

namespace oceanbase
{
namespace sql
{

EnableOpRichFormat::PhyOpInfo EnableOpRichFormat::PHY_OPS_[PHY_END] = {};

#define DEF_OP_DISABLE_RICH_FORMAT(phy_op, name, flag)                                                 \
  EnableOpRichFormat::PHY_OPS_[phy_op] = EnableOpRichFormat::PhyOpInfo(name, flag);
struct InitPhyOpInfo
{
  static int init()
  {
    MEMSET(EnableOpRichFormat::PHY_OPS_, 0, sizeof(EnableOpRichFormat::PhyOpInfo) * PHY_END);
    // all exchange operators are also not supported

    DEF_OP_DISABLE_RICH_FORMAT(PHY_LIMIT,                     "LIMIT",                     (1LL << 0));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MERGE_DISTINCT,            "MERGE_DISTINCT",            (1LL << 1));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_HASH_DISTINCT,             "HASH_DISTINCT",             (1LL << 2));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MATERIAL,                  "MATERIAL",                  (1LL << 3));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MERGE_JOIN,                "MERGE_JOIN",                (1LL << 4));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_GRANULE_ITERATOR,          "GRANULE_ITERATOR",          (1LL << 5));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_EXPAND,                    "EXPAND",                    (1LL << 6));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_SORT,                      "SORT",                      (1LL << 7));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_HASH_UNION,                "HASH_UNION",                (1LL << 8));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_HASH_INTERSECT,            "HASH_INTERSECT",            (1LL << 9));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_HASH_EXCEPT,               "HASH_EXCEPT",               (1LL << 10));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MERGE_UNION,               "MERGE_UNION",               (1LL << 11));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MERGE_INTERSECT,           "MERGE_INTERSECT",           (1LL << 12));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MERGE_EXCEPT,              "MERGE_EXCEPT",              (1LL << 13));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_TABLE_SCAN,                "TABLE_SCAN",                (1LL << 14));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_HASH_JOIN,                 "HASH_JOIN",                 (1LL << 15));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_NESTED_LOOP_JOIN,          "NESTED_LOOP_JOIN",          (1LL << 16));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_SUBPLAN_FILTER,            "SUBPLAN_FILTER",            (1LL << 17));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_SUBPLAN_SCAN,              "SUBPLAN_SCAN",              (1LL << 18));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_UNPIVOT_V2,                "UNPIVOT",                   (1LL << 19));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_SCALAR_AGGREGATE,          "SCALAR_AGGREGATE",          (1LL << 20));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_MERGE_GROUP_BY,            "MERGE_GROUP_BY",            (1LL << 21));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_HASH_GROUP_BY,             "HASH_GROUP_BY",             (1LL << 22));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_WINDOW_FUNCTION,           "WINDOW_FUNCTION",           (1LL << 23));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_TEMP_TABLE_ACCESS,         "TEMP_TABLE_ACCESS",         (1LL << 24));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_TEMP_TABLE_INSERT,         "TEMP_TABLE_INSERT",         (1LL << 25));
    DEF_OP_DISABLE_RICH_FORMAT(PHY_TEMP_TABLE_TRANSFORMATION, "TEMP_TABLE_TRANSFORMATION", (1LL << 26));
    // TODO: support join filter
    // DEF_OP_DISABLE_RICH_FORMAT(PHY_JOIN_FILTER,               "JOIN_FILTER",               (1LL << 27));
    return 0;
  }
};

static int g_init_disable_flags = InitPhyOpInfo::init();
} // end sql
} // end oceanbase