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

#define USING_LOG_PREFIX \
  SQL:                   \
  q
#include "test_trans_utils.h"
#include <sql/rewrite/ob_transformer_impl.h>
#include <iostream>

namespace test {
const int TestTransUtils::trans_type_[9] = {
    SIMPLIFY,
    ANYALL,
    AGGR,
    ELIMINATE_OJ,
    VIEW_MERGE,
    WHERE_SQ_PULL_UP,
    SET_OP,
};

bool TestTransUtils::parse_cmd(int argc, char* argv[], CmdLineParam& param)
{
  bool ret = true;

  int opt = 0;
  const char* opt_string = "o:e:r:a:h";
  struct option longopts[] = {{"all_rules", 1, NULL, 'a'},
      {"only_rule", 1, NULL, 'o'},
      {"except_rule", 1, NULL, 'e'},
      {"rules", 1, NULL, 'r'},
      {"help", 1, NULL, 'h'},
      {0, 0, 0, 0}};

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    param.use_it_ = true;
    switch (opt) {
      case 'a': {
        // default rules is already all
        break;
      }
      case 'o': {
        int tmp = static_cast<int>(strtol(optarg, NULL, 10));
        if (tmp < 0 || tmp >= 9) {
          return false;
        }
        std::cout << tmp << std::endl;
        param.rules_ &= trans_type_[tmp];
        break;
      }
      case 'e': {
        int tmp = static_cast<int>(strtol(optarg, NULL, 10));
        if (tmp < 0 || tmp >= 9) {
          return false;
        }
        std::cout << tmp << std::endl;
        param.rules_ &= ~trans_type_[tmp];
        break;
      }
      case 'r': {
        param.rules_ = INVALID_TRANSFORM_TYPE;
        const char* split = ",";
        char* c;
        c = strtok(optarg, split);
        while (c != NULL) {
          if (*c < '0' || *c >= '9') {
            return false;
          }
          int tmp = static_cast<int>(*c - '0');
          std::cout << tmp << std::endl;
          param.rules_ |= static_cast<TRANSFORM_TYPE>(trans_type_[tmp]);
          c = strtok(NULL, split);
        }
        break;
      }
      case 'h': {
        print_help();
        break;
      }
      default: {
        ret = false;
        break;
      }
    }
  }

  return ret;
}

void TestTransUtils::print_help()
{
  const char* options[] = {
      "-a :    apply all rules",
      "-o :    apply just rule x",
      "-e :    apply all rules except x",
      "-r :    apply rules(-r x,y,z...)",
      NULL,
  };
  std::cout << "================ options begin =====================\n";
  for (int i = 0; options[i] != NULL; ++i) {
    std::cout << options[i] << "\n";
  }
  std::cout << "================ options end ======================\n";

  std::cout << "\n";
  const char* rules[] = {
      "SIMPLIFY :         0",
      "ANYALL :           1",
      "AGGR :             2",
      "ELIMINATE_OJ :     3",
      "VIEW_MERGE :       4",
      "WHERE_SQ_PULL_UP : 5",
      "EQ_COND_PULL_UP :  6",
      "PREDICATE_DEDUCE :   7",
      NULL,
  };
  std::cout << "=============== rules number begin ========================\n";
  for (int i = 0; rules[i] != NULL; ++i) {
    std::cout << rules[i] << "\n";
  }
  std::cout << "=============== rules number end ========================\n";
}

}  // namespace test
