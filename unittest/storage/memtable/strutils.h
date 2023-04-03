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

class Printer
{
public:
  enum { MAX_BUF_SIZE = 4096};
  Printer(): limit_(MAX_BUF_SIZE), pos_(0)
  {
    memset(buf_, 0, MAX_BUF_SIZE);
  }

  ~Printer()
  {
    pos_ = 0;
  }
  void reset()
  {
    pos_ = 0;
    *buf_ = 0;
  }
  char *get_str() { return NULL != buf_ && limit_ > 0 ? buf_ : NULL; }
  char *append(const char *format, ...)
  {
    char *src = NULL;
    int64_t count = 0;
    va_list ap;
    va_start(ap, format);
    if (NULL != buf_ && limit_ > 0 && pos_ < limit_
        && pos_ + (count = vsnprintf(buf_ + pos_, limit_ - pos_, format, ap)) < limit_) {
      src = buf_ + pos_;
      pos_ += count;
    }
    va_end(ap);
    return src;
  }
  char *new_str(const char *format, ...)
  {
    char *src = NULL;
    int64_t count = 0;
    va_list ap;
    va_start(ap, format);
    if (NULL != buf_ && limit_ > 0 && pos_ < limit_
        && pos_ + (count = vsnprintf(buf_ + pos_, limit_ - pos_, format, ap)) + 1 < limit_) {
      src = buf_ + pos_;
      pos_ += count + 1;
    }
    va_end(ap);
    return src;
  }
private:
  char buf_[MAX_BUF_SIZE];
  int64_t limit_;
  int64_t pos_;
};

class Tokenizer
{
public:
  Tokenizer(char* str, const char* delim): str_(str), delim_(delim), saveptr_(NULL)
  {}
  ~Tokenizer() {}
  const char* next() {
    char* ret = NULL;
    if (NULL == saveptr_) {
      ret = strtok_r(str_, delim_, &saveptr_);
    } else {
      ret = strtok_r(NULL, delim_, &saveptr_);
    }
    return ret;
  }
private:
  char* str_;
  const char* delim_;
  char* saveptr_;
};

