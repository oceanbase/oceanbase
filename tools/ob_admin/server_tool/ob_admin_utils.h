/**
 * (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <>
 * Version: $Id$
 * Filename: ob_admin_utils.h
 *
 * Authors:
 *   Shi Yudi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_ADMIN_UTILS_H_
#define _OB_ADMIN_UTILS_H_

#include <vector>
#include <string>
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
