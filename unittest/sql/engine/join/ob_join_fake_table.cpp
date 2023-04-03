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

#include "ob_join_fake_table.h"
#include "sql/engine/table/ob_table_scan.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace sql
{
namespace test
{

// 0 means null to simplify the case data.
static JoinData g_join_data[][6] =
{
    // merge join
    {
        // case 0
        {
          {{11,11}, {21,21}, {21,21},          {31,31}, {31,31}, {36,36}, {41,41},                   {61,61}, {-1,-1}},
          {         {21,21},          {26,26}, {31,31}, {31,31},          {41,41}, {41,41}, {51,51},          {-1,-1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {36,36,0,0},
            {41,41,41,41}, {41,41,41,41},
            {61,61,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,21,21},
            {0,0,26,26},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,51},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {0,0,21,21},
            {0,0,26,26},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {36,36,0,0},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,51},
            {61,61,0,0},
            {-1,-1,-1,-1}
          }
        },
        // case 1
        {
          {{11,11}, {21,0}, {21,21}, {21,21},         {31,31},          {41, 0}, {51, 51}, {61, 61},           {-1, -1}},
          {                 {21,21},          {31,0}, {31,31}, {31,31}, {41, 0}, {51, 51},           {71, 71}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31},
            {51,51,51,51},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,0,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {31,31,31,31}, {31,31,31,31},
            {41,0,0,0},
            {51,51,51,51},
            {61,61,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,21,21},
            {0,0,31,0},
            {31,31,31,31}, {31,31,31,31},
            {0,0,41,0},
            {51,51,51,51},
            {0,0,71,71},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,0,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {0,0,21,21},
            {0,0,31,0},
            {31,31,31,31}, {31,31,31,31},
            {41,0,0,0},
            {0,0,41,0},
            {51,51,51,51},
            {61,61,0,0},
            {0,0,71,71},
            {-1,-1,-1,-1}
          }
        },
        // case 2
        {
          {{31,31}, {41,41}, {-1, -1}},
          {                  {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,0,0},
            {41,41,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,0,0},
            {41,41,0,0},
            {-1,-1,-1,-1}
          }
        },
        // case 3
        {
          {                  {-1, -1}},
          {{31,31}, {41,41}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,31},
            {0,0,41,41},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,31},
            {0,0,41,41},
            {-1,-1,-1,-1}
          }
        },
        // case 4
        {
          {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
          {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          }
        },
        // case 5
        {
          {{31,0}, {41,0}, {41,41},          {51,0},         {61,61}, {61,61}, {71,0}, {-1, -1}},
          {{31,0},         {41,41}, {41,41}, {51,0}, {61,0}, {61,61},          {71,0}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {41,41,41,41}, {41,41,41,41},
            {61,61,61,61}, {61,61,61,61},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,0,0,0},
            {41,0,0,0},
            {41,41,41,41}, {41,41,41,41},
            {51,0,0,0},
            {61,61,61,61}, {61,61,61,61},
            {71,0,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,0},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,0},
            {0,0,61,0},
            {61,61,61,61}, {61,61,61,61},
            {0,0,71,0},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,0,0,0},
            {0,0,31,0},
            {41,0,0,0},
            {41,41,41,41}, {41,41,41,41},
            {51,0,0,0},
            {0,0,51,0},
            {0,0,61,0},
            {61,61,61,61}, {61,61,61,61},
            {71,0,0,0},
            {0,0,71,0},
            {-1,-1,-1,-1}
          }
        }
    },

    {
        // case 0
        {
          {{11,11}, {21,21}, {21,21},          {31,31}, {31,31}, {36,36}, {41,41},                   {61,61}, {-1,-1}},
          {         {21,21},          {26,26}, {31,31}, {31,31},          {41,41}, {41,41}, {51,51},          {-1,-1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {36,36,0,0},
            {41,41,41,41}, {41,41,41,41},
            {61,61,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,21,21},
            {0,0,26,26},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,51},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {0,0,21,21},
            {0,0,26,26},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {36,36,0,0},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,51},
            {61,61,0,0},
            {-1,-1,-1,-1}
          }
        },
        // case 1
        {
          {{11,11}, {21,0}, {21,21}, {21,21},         {31,31},          {41, 0}, {51, 51}, {61, 61},           {-1, -1}},
          {                 {21,21},          {31,0}, {31,31}, {31,31}, {41, 0}, {51, 51},           {71, 71}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31},
            {51,51,51,51},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,0,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {31,31,31,31}, {31,31,31,31},
            {41,0,0,0},
            {51,51,51,51},
            {61,61,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,21,21},
            {0,0,31,0},
            {31,31,31,31}, {31,31,31,31},
            {0,0,41,0},
            {51,51,51,51},
            {0,0,71,71},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,0,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {0,0,21,21},
            {0,0,31,0},
            {31,31,31,31}, {31,31,31,31},
            {41,0,0,0},
            {0,0,41,0},
            {51,51,51,51},
            {61,61,0,0},
            {0,0,71,71},
            {-1,-1,-1,-1}
          }
        },
        // case 2
        {
          {{31,31}, {41,41}, {-1, -1}},
          {                  {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,0,0},
            {41,41,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,0,0},
            {41,41,0,0},
            {-1,-1,-1,-1}
          }
        },
        // case 3
        {
          {                  {-1, -1}},
          {{31,31}, {41,41}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,31},
            {0,0,41,41},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,31},
            {0,0,41,41},
            {-1,-1,-1,-1}
          }
        },
        // case 4
        {
          {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
          {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          }
        },
        // case 5
        {
          {{31,0}, {41,0}, {41,41},          {51,0},         {61,61}, {61,61}, {71,0}, {-1, -1}},
          {{31,0},         {41,41}, {41,41}, {51,0}, {61,0}, {61,61},          {71,0}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {41,41,41,41}, {41,41,41,41},
            {61,61,61,61}, {61,61,61,61},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,0,0,0},
            {41,0,0,0},
            {41,41,41,41}, {41,41,41,41},
            {51,0,0,0},
            {61,61,61,61}, {61,61,61,61},
            {71,0,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,0},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,0},
            {0,0,61,0},
            {61,61,61,61}, {61,61,61,61},
            {0,0,71,0},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,0,0,0},
            {0,0,31,0},
            {41,0,0,0},
            {41,41,41,41}, {41,41,41,41},
            {51,0,0,0},
            {0,0,51,0},
            {0,0,61,0},
            {61,61,61,61}, {61,61,61,61},
            {71,0,0,0},
            {0,0,71,0},
            {-1,-1,-1,-1}
          }
        }
    },

    {
        // case 0
        {
          {{11,11}, {21,21}, {21,21},          {31,31}, {31,31}, {36,36}, {41,41},                   {61,61}, {-1,-1}},
          {         {21,21},          {26,26}, {31,31}, {31,31},          {41,41}, {41,41}, {51,51},          {-1,-1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {11,11,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {36,36,0,0},
            {61,61,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41},
            {0,0,21,21},
            {0,0,26,26},
            {0,0,51,51},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {0,0,21,21},
            {0,0,26,26},
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {36,36,0,0},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,51},
            {61,61,0,0},
            {-1,-1,-1,-1}
          }
        },
        // case 1
        {
          {{11,11}, {21,0}, {21,21}, {21,21},         {31,31},          {41, 0}, {51, 51}, {61, 61},           {-1, -1}},
          {                 {21,21},          {31,0}, {31,31}, {31,31}, {41, 0}, {51, 51},           {71, 71}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31},
            {51,51,51,51},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31},
            {51,51,51,51},
            {11,11,0,0},
            {21,0,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {41,0,0,0},
            {61,61,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,21,21},
            {0,0,31,0},
            {31,31,31,31}, {31,31,31,31},
            {0,0,41,0},
            {51,51,51,51},
            {0,0,71,71},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {11,11,0,0},
            {21,0,0,0},
            {21,21,0,0},
            {21,21,0,0},
            {0,0,21,21},
            {0,0,31,0},
            {31,31,31,31}, {31,31,31,31},
            {41,0,0,0},
            {0,0,41,0},
            {51,51,51,51},
            {61,61,0,0},
            {0,0,71,71},
            {-1,-1,-1,-1}
          }
        },
        // case 2
        {
          {{31,31}, {41,41}, {-1, -1}},
          {                  {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,0,0},
            {41,41,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,0,0},
            {41,41,0,0},
            {-1,-1,-1,-1}
          }
        },
        // case 3
        {
          {                  {-1, -1}},
          {{31,31}, {41,41}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,31},
            {0,0,41,41},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,31},
            {0,0,41,41},
            {-1,-1,-1,-1}
          }
        },
        // case 4
        {
          {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
          {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
            {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
            {-1,-1,-1,-1}
          }
        },
        // case 5
        {
          {{31,0}, {41,0}, {41,41},          {51,0},         {61,61}, {61,61}, {71,0}, {-1, -1}},
          {{31,0},         {41,41}, {41,41}, {51,0}, {61,0}, {61,61},          {71,0}, {-1, -1}},
          // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {41,41,41,41}, {41,41,41,41},
            {61,61,61,61}, {61,61,61,61},
            {-1,-1,-1,-1}
          },
          // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {41,41,41,41}, {41,41,41,41},
            {61,61,61,61}, {61,61,61,61},
            {31,0,0,0},
            {41,0,0,0},
            {51,0,0,0},
            {71,0,0,0},
            {-1,-1,-1,-1}
          },
          // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {0,0,31,0},
            {41,41,41,41}, {41,41,41,41},
            {0,0,51,0},
            {0,0,61,0},
            {61,61,61,61}, {61,61,61,61},
            {0,0,71,0},
            {-1,-1,-1,-1}
          },
          // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
          {
            {31,0,0,0},
            {0,0,31,0},
            {41,0,0,0},
            {41,41,41,41}, {41,41,41,41},
            {51,0,0,0},
            {0,0,51,0},
            {0,0,61,0},
            {61,61,61,61}, {61,61,61,61},
            {71,0,0,0},
            {0,0,71,0},
            {-1,-1,-1,-1}
          }
        }
    },

    // hash join
    {

      // case 0
      {
        {{11,11}, {21,21}, {21,21},          {31,31}, {31,31}, {36,36}, {41,41},                   {61,61}, {-1,-1}},
        {         {21,21},          {26,26}, {31,31}, {31,31},          {41,41}, {41,41}, {51,51},          {-1,-1}},
        // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41},
          {-1,-1,-1,-1}
        },
        // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41},
          {61,61,0,0},
          {11,11,0,0},
          {21,21,0,0},
          {21,21,0,0},
          {36,36,0,0},
          {-1,-1,-1,-1}
        },
        // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,21,21},
          {0,0,26,26},
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41},
          {0,0,51,51},
          {-1,-1,-1,-1}
        },
        // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,21,21},
          {0,0,26,26},
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41},
          {0,0,51,51},
          {61,61,0,0},
          {11,11,0,0},
          {21,21,0,0},
          {21,21,0,0},
          {36,36,0,0},
          {-1,-1,-1,-1}
        }
      },
      // case 1
      {
        {{11,11}, {21,0}, {21,21}, {21,21},         {31,31},          {41, 0}, {51, 51}, {61, 61},           {-1, -1}},
        {                 {21,21},          {31,0}, {31,31}, {31,31}, {41, 0}, {51, 51},           {71, 71}, {-1, -1}},
        // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31},
          {51,51,51,51},
          {-1,-1,-1,-1}
        },
        // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31},
          {51,51,51,51},
          {21,0,0,0},
          {61,61,0,0},
          {11,11,0,0},
          {41,0,0,0},
          {21,21,0,0},
          {21,21,0,0},
          {-1,-1,-1,-1}
        },
        // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,21,21},
          {0,0,31,0},
          {31,31,31,31}, {31,31,31,31},
          {0,0,41,0},
          {51,51,51,51},
          {0,0,71,71},
          {-1,-1,-1,-1}
        },
        // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,21,21},
          {0,0,31,0},
          {31,31,31,31}, {31,31,31,31},
          {0,0,41,0},
          {51,51,51,51},
          {0,0,71,71},
          {21,0,0,0},
          {61,61,0,0},
          {11,11,0,0},
          {41,0,0,0},
          {21,21,0,0},
          {21,21,0,0},
          {-1,-1,-1,-1}
        }
      },
      // case 2
      {
        {{31,31}, {41,41}, {-1, -1}},
        {                  {-1, -1}},
        // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {-1,-1,-1,-1}
        },
        // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,0,0},
          {41,41,0,0},
          {-1,-1,-1,-1}
        },
        // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {-1,-1,-1,-1}
        },
        // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,0,0},
          {41,41,0,0},
          {-1,-1,-1,-1}
        }
      },
      // case 3
      {
        {                  {-1, -1}},
        {{31,31}, {41,41}, {-1, -1}},
        // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {-1,-1,-1,-1}
        },
        // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {-1,-1,-1,-1}
        },
        // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,31,31},
          {0,0,41,41},
          {-1,-1,-1,-1}
        },
        // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,31,31},
          {0,0,41,41},
          {-1,-1,-1,-1}
        }
      },
      // case 4
      {
        {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
        {{31,31}, {31,31}, {41,41}, {41,41}, {-1, -1}},
        // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
          {-1,-1,-1,-1}
        },
        // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
          {-1,-1,-1,-1}
        },
        // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
          {-1,-1,-1,-1}
        },
        // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {31,31,31,31}, {31,31,31,31}, {31,31,31,31}, {31,31,31,31},
          {41,41,41,41}, {41,41,41,41}, {41,41,41,41}, {41,41,41,41},
          {-1,-1,-1,-1}
        }
      },
      // case 5
      {
        {{31,0}, {41,0}, {41,41},          {51,0},         {61,61}, {61,61}, {71,0}, {-1, -1}},
        {{31,0},         {41,41}, {41,41}, {51,0}, {61,0}, {61,61},          {71,0}, {-1, -1}},
        // t1 inner join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {41,41,41,41}, {41,41,41,41},
          {61,61,61,61}, {61,61,61,61},
          {-1,-1,-1,-1}
        },
        // t1 left join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {41,41,41,41}, {41,41,41,41},
          {61,61,61,61}, {61,61,61,61},
          {51,0,0,0},
          {41,0,0,0},
          {71,0,0,0},
          {31,0,0,0},
          {-1,-1,-1,-1}
        },
        // t1 right join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,31,0},
          {41,41,41,41}, {41,41,41,41},
          {0,0,51,0},
          {0,0,61,0},
          {61,61,61,61}, {61,61,61,61},
          {0,0,71,0},
          {-1,-1,-1,-1}
        },
        // t1 full join t2 on t1.a = t2.b and t1.c = t2.d and t1.a + t2.b > 60
        {
          {0,0,31,0},
          {41,41,41,41}, {41,41,41,41},
          {0,0,51,0},
          {0,0,61,0},
          {61,61,61,61}, {61,61,61,61},
          {0,0,71,0},
          {51,0,0,0},
          {41,0,0,0},
          {71,0,0,0},
          {31,0,0,0},
          {-1,-1,-1,-1}
        }
      }
    }
};

//int ObJoinFakeTableScanInput::init(ObExecContext &ctx, ObTaskInfo &task_info, ObPhyOperator &op)
//{
//  UNUSED(ctx);
//  UNUSED(task_info);
//  UNUSED(op);
//  return OB_SUCCESS;
//}
//
//ObPhyOperatorType ObJoinFakeTableScanInput::get_phy_op_type() const
//{
//  return PHY_TABLE_SCAN;
//}

ObJoinFakeTable::ObJoinFakeTable()
    : ObPhyOperator(alloc_), left_data_(NULL),
      right_data_(NULL),
      out_data_(NULL),
      join_op_type_(JOIN_TEST_TYPE_NUM),
      op_type_(PHY_INVALID),
      is_inited_(false)
{
}

ObJoinFakeTable::~ObJoinFakeTable()
{
}

int ObJoinFakeTable::init(JoinOpTestType join_op_type)
{
  int ret = OB_SUCCESS;
  join_op_type_ = join_op_type;
  is_inited_ = true;
  return ret;
}

int ObJoinFakeTable::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  UNUSED(child_idx);
  UNUSED(child_operator);
  return OB_SUCCESS;
}

ObPhyOperator *ObJoinFakeTable::get_child(int32_t child_idx) const
{
  UNUSED(child_idx);
  return NULL;
}

int ObJoinFakeTable::inner_open(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else if (OB_FAIL(init_op_ctx(exec_ctx))) {
    SQL_ENG_LOG(WARN, "join fake table init op_ctx failed", K(ret));
  } else if (OB_FAIL(create_operator_input(exec_ctx))) {
    SQL_ENG_LOG(WARN, "join fake table create operator input failed", K(ret));
  }
  return ret;
}

int ObJoinFakeTable::rescan(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else {
    ObJoinFakeTableCtx *table_ctx = GET_PHY_OPERATOR_CTX(ObJoinFakeTableCtx, exec_ctx, get_id());
    OB_ASSERT(NULL != table_ctx);
    table_ctx->iter_ = 0;
  }
  return OB_SUCCESS;
}

int ObJoinFakeTable::init_op_ctx(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else {
    ObPhyOperatorCtx *op_ctx = NULL;
    if (OB_SUCCESS != (ret = CREATE_PHY_OPERATOR_CTX(ObJoinFakeTableCtx,
                                                     exec_ctx,
                                                     get_id(),
                                                     get_type(),
                                                     op_ctx))) {
      SQL_EXE_LOG(WARN, "create physical operator context failed", K(ret));
    } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
      SQL_EXE_LOG(WARN, "create current row failed", K(ret));
    }
  }
  return ret;
}

int ObJoinFakeTable::create_operator_input(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else {
    if (PHY_TABLE_SCAN == op_type_) {
      ObJoinFakeTableScanInput *scan_input = NULL;
      OB_ASSERT(OB_SUCC(CREATE_PHY_OP_INPUT(ObJoinFakeTableScanInput, exec_ctx, get_id(), get_type(), scan_input)));
      ObJoinFakeTableCtx *table_ctx = GET_PHY_OPERATOR_CTX(ObJoinFakeTableCtx, exec_ctx, get_id());
      OB_ASSERT(NULL != table_ctx);
      ObTaskInfo task_info(exec_ctx.get_allocator());
      ObJoinFakeTable tmp_op;
      scan_input->init(exec_ctx, task_info, tmp_op);
    }
  }
  return ret;
}

int ObJoinFakeTable::inner_get_next_row(ObExecContext &exec_ctx, const common::ObNewRow *&row) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else {
    ObJoinFakeTableCtx *table_ctx = GET_PHY_OPERATOR_CTX(ObJoinFakeTableCtx, exec_ctx, get_id());
    OB_ASSERT(NULL != table_ctx);
    int64_t &iter = table_ctx->iter_;
    ObNewRow &cur_row = table_ctx->cur_row_;
    row = NULL;
    if (NULL != left_data_) {
      int64_t *data = left_data_[iter];
      ret = cons_row(data[0], data[1], cur_row);
    } else if (NULL != right_data_) {
      if (PHY_TABLE_SCAN == op_type_) {
        ObJoinFakeTableScanInput *scan_input = GET_PHY_OP_INPUT(ObJoinFakeTableScanInput, exec_ctx, get_id());
        if (NULL == scan_input)
        {
          ret = OB_ERR_NULL_VALUE;
          SQL_ENG_LOG(WARN, "scan input is null", K(ret));
        } else if (BNL_JOIN_TEST != join_op_type_) {
          int64_t scan_key = scan_input->get_query_range().get_scan_key_value();
          while (scan_key != right_data_[iter][0] && -1 != right_data_[iter][0]) {
            ++iter;
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t *data = right_data_[iter];
        ret = cons_row(data[0], data[1], cur_row);
      }
    } else if (NULL != out_data_) {
      int64_t *data = out_data_[iter];
      ret = cons_row(data[0], data[1], data[2], data[3], cur_row);
    } else {
      OB_ASSERT(false);
    }
    if (OB_SUCC(ret)) {
      ++iter;
      row = &cur_row;
    }
  }
  return ret;
}

int ObJoinFakeTable::prepare_data(int64_t case_id,
                                  TableType table_type,
                                  ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else if (!(join_op_type_ >= 0 && join_op_type_ < JOIN_TEST_TYPE_NUM)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "error join op type", K(join_op_type_), K(ret));
  } else {
    if (TT_LEFT_TABLE == table_type) {
      left_data_ = g_join_data[join_op_type_][case_id].left_;
    } else if (TT_RIGHT_TABLE == table_type) {
      right_data_ = g_join_data[join_op_type_][case_id].right_;
    } else if (TT_OUT_TABLE == table_type) {
      if (INNER_JOIN == join_type) {
        out_data_ = g_join_data[join_op_type_][case_id].out_inner_;
      } else if (LEFT_OUTER_JOIN == join_type) {
        out_data_ = g_join_data[join_op_type_][case_id].out_left_;
      } else if (RIGHT_OUTER_JOIN == join_type) {
        out_data_ = g_join_data[join_op_type_][case_id].out_right_;
      } else if (FULL_OUTER_JOIN == join_type) {
        out_data_ = g_join_data[join_op_type_][case_id].out_full_;
      } else {
        OB_ASSERT(false);
      }
    } else {
      OB_ASSERT(false);
    }
  }
  return ret;
}

int ObJoinFakeTable::cons_row(int64_t col1, int64_t col2, ObNewRow &row) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else if (2 != column_count_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "column count <> 2", K(ret));
  } else {
    if (-1 == col1) {
      ret = OB_ITER_END;
    } else {
      (0 == col1) ? row.cells_[0].set_null() : row.cells_[0].set_int(col1);
      (0 == col2) ? row.cells_[1].set_null() : row.cells_[1].set_int(col2);
    }
  }
  return ret;
}

int ObJoinFakeTable::cons_row(int64_t col1, int64_t col2, int64_t col3, int64_t col4, ObNewRow &row) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "join fake table not inited", K(ret));
  } else if (4 != column_count_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "column count <> 4", K(ret));
  } else {
    if (-1 == col1) {
      ret = OB_ITER_END;
    } else {
      (0 == col1) ? row.cells_[0].set_null() : row.cells_[0].set_int(col1);
      (0 == col2) ? row.cells_[1].set_null() : row.cells_[1].set_int(col2);
      (0 == col3) ? row.cells_[2].set_null() : row.cells_[2].set_int(col3);
      (0 == col4) ? row.cells_[3].set_null() : row.cells_[3].set_int(col4);
    }
  }
  return ret;
}

} // namespace test
} // namespace sql
} // namespace oceanbase
