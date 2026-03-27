/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
