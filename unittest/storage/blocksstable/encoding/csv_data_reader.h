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

#ifndef CSV_DATA_READER_H_
#define CSV_DATA_READER_H_

#include <vector>
#include <string>
#include <fstream>
#include "share/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace blocksstable
{

class CSVDataReader
{
public:
  CSVDataReader();
  virtual ~CSVDataReader();
  int init(const char *file);
  int next_data(std::vector<common::ObString> &data);
  int reset()
  {
    ifs_.clear();
    ifs_.seekg(0, std::ios::beg);
    return common::OB_SUCCESS;
  }
private:
  int parse_line();
private:
  static const int64_t MAX_STR_LEN = 64L * 1024L;
  char *buf_;
  int64_t buf_size_;
  std::string cur_line_;
  std::vector<common::ObString> datas_;
  std::ifstream ifs_;
};

}
}

#endif // CSV_DATA_READER_H_
