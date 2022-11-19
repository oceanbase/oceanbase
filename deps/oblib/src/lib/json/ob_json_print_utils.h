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

#ifndef LIB_JSON_OB_JSON_PRINT_UTILS_
#define LIB_JSON_OB_JSON_PRINT_UTILS_
#include "lib/utility/ob_print_utils.h"
#include "lib/json/ob_json.h"
#include "lib/allocator/ob_mod_define.h"
namespace oceanbase
{
namespace json
{

// To be more readable, we don't quote name in our logging json.
// This class convert it to standard json format, for example:
//
//   From:
//     {name1:123, name2:"string1", "name3":[]}
//   To:
//     {"name1":123, "name2":"string1", "name3":[]}
//
class ObStdJsonConvertor
{
public:
  ObStdJsonConvertor()
      : backslash_escape_(true), json_(NULL), buf_(NULL), buf_size_(0), pos_(0) {}
  virtual ~ObStdJsonConvertor() {}

  int init(const char *json, char *buf, const int64_t buf_size_);

  void disable_backslash_escape() { backslash_escape_ = false; }
  void enable_backslash_escape() { backslash_escape_ = true; }

  int convert(int64_t &out_len);

private:
  // output [begin, p]
  int output(const char *p, const char *&begin, int64_t &out_len);
  int quoted_output(const char *p, const char *&begin, int64_t &out_len);
  int add_quote_mark(int64_t &out_len);

private:
  bool backslash_escape_;
  const char *json_;
  char *buf_;
  int64_t buf_size_;
  int64_t pos_;
};

// Helper class to print object in formated json.
template <typename T>
struct ObFormatedJsonPrinter
{
  static const int64_t MAX_PRINTABLE_SIZE = 2 * 1024 * 1024;
  static const int64_t BUFFER_SIZE = MAX_PRINTABLE_SIZE;
  explicit ObFormatedJsonPrinter(const T *obj) : obj_(obj) {}
  explicit ObFormatedJsonPrinter(const T &obj) : obj_(&obj) {}

  DECLARE_TO_STRING{
    int64_t pos = 0;
    int ret = common::OB_SUCCESS;
    common::ObArenaAllocator allocator(lib::ObLabel("FmtJsonPrint"));
    char *js_buf = NULL;
    if (NULL == obj_) {
      BUF_PRINTF("NULL");
    } else if (NULL == (js_buf = static_cast<char *>(allocator.alloc(BUFFER_SIZE * 2)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "alloc buffer failed", K(ret));
    } else {
      int64_t out_len = obj_->to_string(js_buf, BUFFER_SIZE);
      js_buf[std::min(out_len, BUFFER_SIZE - 1)] = '\0';
      out_len = 0;
      ObStdJsonConvertor convertor;
      if (OB_FAIL(convertor.init(js_buf, js_buf + BUFFER_SIZE, BUFFER_SIZE))) {
        LIB_LOG(WARN, "fail to init convertor", K(ret));
      } else {
        convertor.disable_backslash_escape();
        if (OB_FAIL(convertor.convert(out_len))) {
          LIB_LOG(WARN, "convert failed", K(ret));
        } else {
          js_buf[BUFFER_SIZE + std::min(out_len, BUFFER_SIZE - 1)] = '\0';
        }
      }
      if (OB_SUCC(ret)) {
        Parser json_parser;
        Value *root = NULL;
        if (OB_FAIL(json_parser.init(&allocator))) {
          LIB_LOG(WARN, "fail to init json parser", K(ret));
        } else if (OB_FAIL(json_parser.parse(js_buf + BUFFER_SIZE, out_len, root))) {
          LIB_LOG(WARN, "fail to parse json", K(ret));
          BUF_PRINTF("%s", js_buf + BUFFER_SIZE);
        } else {
          Tidy tidy(root);
          pos = tidy.to_string(buf, buf_len);
        }
      }
    }
    return pos;
  }

  struct JBuf
  {
    char buf_[BUFFER_SIZE];
  };
  operator const char *() const
  {
    char *buf = nullptr;
    JBuf *jbuf = GET_TSI(JBuf);
    if (NULL != jbuf) {
      buf = jbuf->buf_;
      int pos = this->to_string(buf, BUFFER_SIZE);
      if (pos >= 0 && pos < BUFFER_SIZE) {
        buf[pos] = '\0';
      } else {
        buf[0] = '\0';
      }
    }
    return buf;
  }

private:
  const T *obj_;
};

} // end namespace json
} // end namespace oceanbase

#define SJ(X) oceanbase::json::ObFormatedJsonPrinter< \
    typename std::remove_pointer<typeof(X)>::type>((X))

#endif /* LIB_JSON_OB_JSON_PRINT_UTILS_ */
