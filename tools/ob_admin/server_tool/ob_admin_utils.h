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

#ifndef _OB_ADMIN_UTILS_H_
#define _OB_ADMIN_UTILS_H_

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <sstream>

using std::vector;
using std::string;
using std::stringstream;

#define NONE "\033[m"
#define RED "\033[0;32;31m"
#define LIGHT_RED "\033[1;31m"
#define GREEN "\033[0;32;32m"
#define LIGHT_GREEN "\033[1;32m"
#define BLUE "\033[0;32;34m"
#define LIGHT_BLUE "\033[1;34m"
#define DARY_GRAY "\033[1;30m"
#define CYAN "\033[0;36m"
#define LIGHT_CYAN "\033[1;36m"
#define PURPLE "\033[0;35m"
#define LIGHT_PURPLE "\033[1;35m"
#define BROWN "\033[0;33m"
#define YELLOW "\033[1;33m"
#define LIGHT_GRAY "\033[0;37m"
#define WHITE "\033[1;37m"

namespace oceanbase {
  namespace tools {
  void split(const string &s, char delim, vector<string> &elems)
  {
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim)) {
      elems.push_back(item);
    }
  }

  void execv(const string &path, vector<string> &elems)
  {
    char *vec[1024] = {};
    if (elems.size() < 1024) {
      vec[0] = const_cast<char*>(path.c_str());
      for (size_t i = 0; i < elems.size(); ++i) {
        vec[i+1] = const_cast<char*>(elems[i].c_str());
      }
      ::execv(path.c_str(), vec);
    }
  }
} /* end of namespace tools */
} /* end of namespace oceanbase */

#endif /* _OB_ADMIN_UTILS_H_ */
