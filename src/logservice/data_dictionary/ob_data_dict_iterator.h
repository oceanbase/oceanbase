/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* ObDataDict iterator for deserializ
*/

#ifndef OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_ITERATOR_
#define OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_ITERATOR_

#include "ob_data_dict_struct.h"
#include "ob_data_dict_utils.h"

namespace oceanbase
{
namespace datadict
{

class ObDataDictIterator
{
public:
  ObDataDictIterator();
  ~ObDataDictIterator() { reset(); }
  int init(const uint64_t tenant_id);
  void reset();
public:
  int append_log_buf(const char *buf, const int64_t buf_len, const int64_t pos); // without log_base_header
  int next_dict_header(ObDictMetaHeader &meta_header);
  template<class DICT_ENTRY>
  int next_dict_entry(const ObDictMetaHeader &header, DICT_ENTRY &dict_entry);
private:
  OB_INLINE void release_palf_buf_()
  {
    palf_buf_ = NULL;
    palf_buf_len_ = 0;
    palf_pos_ = 0;
  }
private:
  int append_log_buf_with_base_header_(const char *buf, const int64_t buf_len);
  int deserializ_base_log_header_();
  int prepare_dict_buf_(const int64_t required_size);
  int memcpy_palf_buf_to_dict_buf_(const int64_t copy_size);
private:
  static const int64_t DEFAULT_DICT_BUF_SIZE_FOR_ITERATOR = 4 * _M_;
private:
  uint64_t tenant_id_;
  const char *palf_buf_; // buf points to data stored in palf log_entry
  int64_t palf_buf_len_;
  int64_t palf_pos_;
  char *dict_buf_; // buf stored serialized dict data, which is larger than 2M
  int64_t dict_buf_len_; // length of dict_buf_
  int64_t dict_pos_;
};

} // namespace datadict
} // namespace oceanbase
#endif
