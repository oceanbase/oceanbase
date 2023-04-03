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

#define USING_LOG_PREFIX STORAGE

#include "csv_data_reader.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{

CSVDataReader::CSVDataReader()
{
}

CSVDataReader::~CSVDataReader()
{
  ifs_.close();
  free(buf_);
}

int CSVDataReader::init(const char *file)
{
  int ret = OB_SUCCESS;
  ifs_.open(file);
  if (ifs_.fail()) {
    ret = OB_ERROR;
    LOG_WARN("failed to open csv data file", K(ret));
  } else {
    buf_ = new char [MAX_STR_LEN];
    buf_size_ = MAX_STR_LEN;
  }
  return ret;
}

int CSVDataReader::next_data(std::vector<ObString> &data)
{
  int ret = OB_SUCCESS;
  while (std::getline(ifs_, cur_line_)) {
    if (!cur_line_.empty()) {
      if (cur_line_.length() > buf_size_) {
        free(buf_);
        // next power of 2
        buf_size_ = (cur_line_.length() + 1) * 2;
        if (0 != (buf_size_ & (buf_size_ - 1))) {
          while (0 != (buf_size_ & (buf_size_ - 1))) {
            buf_size_ = buf_size_ & (buf_size_ - 1);
          }
        }
        if (buf_size_ < cur_line_.length()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected size", K(ret), K(buf_size_), K(cur_line_.length()));
        } else {
          buf_ = new char [buf_size_];
        }
      }
      break;
    }
  }
  if (cur_line_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(parse_line())) {
    LOG_WARN("failed to parse csv line", K(ret));
  } else {
    data.clear();
    data.reserve(datas_.size());
    for (int64_t i = 0; i < datas_.size(); i++) {
      data.push_back(datas_.at(i));
    }
  }
  return ret;
}

int CSVDataReader::parse_line()
{
  int ret = OB_SUCCESS;
  if (cur_line_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cur_line_ is empty", K(ret));
  } else {
    datas_.clear();
    const char *p = cur_line_.c_str();
    int64_t pos = 0;
    int64_t begin = -1;
    bool quote = false;
    while (*p) {
      if (quote) {
        switch (*p) {
          // according to ObHexEscapeSqlStr::to_string fun
          case '\\':
            p++;
            if (!*p) {
              ret = OB_ERROR;
              LOG_WARN("unterminated string", K(ret));
              return ret;
            } else {
              switch (*p) {
                case '\\':
                case '\'':
                case '\"':
                  buf_[pos++] = *p;
                  break;
                case 'n':
                  buf_[pos++] = '\n';
                  break;
                case '0':
                  buf_[pos++] = '\0';
                  break;
                case 'r':
                  buf_[pos++] = '\r';
                  break;
                case 't':
                  buf_[pos++] = '\t';
                  break;
                default:
                  ret = OB_ERROR;
                  LOG_WARN("unexpected char", K(ret), K(*p));
                  break;
              }
            }
            p++;
            break;
          case '\'':
            buf_[pos++] = *(p++);
            if (*p == ',') {
              quote = false;
              datas_.push_back(ObString(pos - begin, &buf_[begin]));
              begin = -1;
            }
            break;
          default:
            buf_[pos++] = *(p++);
        }
      } else {
        switch (*p) {
          case ',':
            if (-1 != begin) {
              datas_.push_back(ObString(pos - begin, &buf_[begin]));
              begin = -1;
            }
            p++;
            break;
          case '\'':
            quote = true;
          default:
            if (-1 == begin) {
              begin = pos;
            }
            buf_[pos++] = *(p++);
        }
      }
    }
    if (-1 != begin) {
      datas_.push_back(ObString(pos - begin, &buf_[begin]));
      begin = -1;
    }
  }
  return ret;
}

}
}
