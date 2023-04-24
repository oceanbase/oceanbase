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

#include "ob_data_dict_iterator.h"
#include "logservice/ob_log_base_header.h"

namespace oceanbase
{
namespace datadict
{

ObDataDictIterator::ObDataDictIterator()
  : tenant_id_(OB_INVALID_TENANT_ID),
    palf_buf_(NULL),
    palf_buf_len_(0),
    palf_pos_(0),
    dict_buf_(NULL),
    dict_buf_len_(0),
    dict_pos_(0)
{
}

int ObDataDictIterator::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dict_buf_ = static_cast<char*>(ob_dict_malloc(DEFAULT_DICT_BUF_SIZE_FOR_ITERATOR, tenant_id)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DDLOG(ERROR, "dict_buf_ is null", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    dict_buf_len_ = DEFAULT_DICT_BUF_SIZE_FOR_ITERATOR;
  }

  return ret;
}

void ObDataDictIterator::reset()
{
  palf_buf_ = NULL;
  palf_buf_len_ = 0;
  palf_pos_ = 0;
  if (OB_NOT_NULL(dict_buf_)) {
    ob_dict_free(dict_buf_);
    dict_buf_ = NULL;
  }
  dict_buf_ = NULL;
  dict_buf_len_ = 0;
  dict_pos_ = 0;
}

int ObDataDictIterator::append_log_buf(const char *buf, const int64_t buf_len, const int64_t pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid arg", KR(ret), K(buf_len));
  } else if (OB_NOT_NULL(palf_buf_)
      || OB_UNLIKELY(palf_buf_len_ > 0)
      || OB_UNLIKELY(palf_pos_ > 0)
      || OB_UNLIKELY(pos >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "palf_buf status not valid", KR(ret), K_(palf_buf_len), K_(palf_pos), K(buf_len), K(pos));
  } else {
    palf_buf_ = buf;
    palf_buf_len_ = buf_len;
    palf_pos_ = pos;
  }

  return ret;
}

int ObDataDictIterator::next_dict_header(ObDictMetaHeader &header)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(palf_buf_)
      || OB_UNLIKELY(palf_buf_len_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect valid palf_buf", KR(ret), K_(palf_buf_len));
  } else if (palf_buf_len_ == palf_pos_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect header not valid, which should deserializ from palf_buf", KR(ret), K(header));
  } else if (OB_FAIL(header.deserialize(palf_buf_, palf_buf_len_, palf_pos_))) {
    DDLOG(WARN, "deserialize ObDictMetaHeader failed", KR(ret), K_(palf_buf_len), K_(palf_pos), K(header));
  } else if (OB_UNLIKELY(! header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect ObDictMetaHeader valid after deserialize", KR(ret), K(header));
  } else if (ObDictMetaStorageType::FULL == header.get_storage_type()) {
    // do nothing and return the header.
  } else if (ObDictMetaStorageType::FIRST == header.get_storage_type()) {
        if(OB_UNLIKELY(dict_pos_ > 0)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "dict_buf_ should be empty and palf_buf_ should be new(except header)",
          KR(ret), K_(palf_pos), K_(dict_pos), K(header));
    } else if (OB_FAIL(prepare_dict_buf_(header.get_dict_serialized_length()))) {
      DDLOG(WARN, "prepare_dict_buf_ failed", KR(ret), "required_size", header.get_dict_serialized_length());
    } else {
      const int64_t copy_size = palf_buf_len_ - palf_pos_;

      if (OB_FAIL(memcpy_palf_buf_to_dict_buf_(copy_size))) {
        DDLOG(WARN, "memcpy_palf_buf_to_dict_buf_ failed", KR(ret),
            K(copy_size), K_(palf_pos), K_(palf_buf_len), K_(dict_pos), K_(dict_buf_len));
      } else if (OB_UNLIKELY(palf_pos_ != palf_buf_len_)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "expect palf_buf used up", KR(ret), K_(palf_pos), K_(palf_buf_len));
      } else {
        ret = OB_ITER_END;
      }
    }
  } else if (ObDictMetaStorageType::MIDDLE == header.get_storage_type()) {
    if (OB_UNLIKELY(dict_pos_ <= 0)
        || OB_UNLIKELY(dict_buf_len_ <= 0)) {
      ret = OB_STATE_NOT_MATCH;
      DDLOG(WARN, "dict_buf_ should not be empty and palf_buf_ should be new(except header)",
          KR(ret), K_(palf_pos), K_(dict_pos));
    } else {
      const int64_t copy_size = palf_buf_len_ - palf_pos_;

      if (OB_FAIL(memcpy_palf_buf_to_dict_buf_(copy_size))) {
        DDLOG(WARN, "memcpy_palf_buf_to_dict_buf_ failed", KR(ret),
            K(copy_size), K_(palf_pos), K_(palf_buf_len), K_(dict_pos), K_(dict_buf_len));
      } else if (OB_UNLIKELY(palf_pos_ != palf_buf_len_)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "expect palf_buf used up", KR(ret), K_(palf_pos), K_(palf_buf_len));
      } else {
        ret = OB_ITER_END;
      }
    }
  } else if (ObDictMetaStorageType::LAST == header.get_storage_type()) {
      if(OB_UNLIKELY(dict_pos_ <= 0)
        || OB_UNLIKELY(dict_buf_len_ <= 0)) {
      ret = OB_STATE_NOT_MATCH;
      DDLOG(WARN, "dict_buf_ should not be empty and palf_buf_ should be new(except header)",
          KR(ret), K_(palf_pos), K_(dict_pos));
    } else {
      const int64_t copy_size = header.get_dict_serialized_length() - dict_pos_;

      if (OB_FAIL(memcpy_palf_buf_to_dict_buf_(copy_size))) {
        DDLOG(WARN, "memcpy_palf_buf_to_dict_buf_ failed", KR(ret), K(header),
            K(copy_size), K_(palf_pos), K_(palf_buf_len), K_(dict_pos), K_(dict_buf_len));
      } else if (OB_UNLIKELY(dict_pos_ != header.get_dict_serialized_length())) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "expect palf_buf used up", KR(ret), K(header), K_(palf_pos), K_(palf_buf_len));
      }
    }
  }
  if (OB_ITER_END == ret) {
    // reset palf_buf for next
    palf_buf_ = NULL;
    palf_buf_len_ = 0;
    palf_pos_ = 0;
  }
  DDLOG(DEBUG, "get_dict_header", KR(ret), K(header));

  return ret;
}

template<class DICT_ENTRY>
int ObDataDictIterator::next_dict_entry(const ObDictMetaHeader &header, DICT_ENTRY &dict_entry)
{
  int ret = OB_SUCCESS;

  if (dict_pos_ > 0) {
    // deserialize from dict_buf_
    int64_t deserialize_pos = 0;
    if (OB_FAIL(dict_entry.deserialize(header, dict_buf_, dict_pos_, deserialize_pos))) {
      DDLOG(WARN, "deserialize DICT_ENTRY from dict_buf failed", KR(ret),
          K(header), K_(dict_pos), K(deserialize_pos));
    }
  } else if (palf_pos_ > 0) {
    // deserialize from dict_buf_
    if (OB_FAIL(dict_entry.deserialize(header, palf_buf_, palf_buf_len_, palf_pos_))) {
      DDLOG(WARN, "deserialize DICT_ENTRY from palf_buf failed", KR(ret),
          K(header), K_(palf_buf_len), K_(palf_pos));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect any of dict_pos/palf_pos is valid", KR(ret), K(header), K_(palf_pos), K_(dict_pos));
  }

  return ret;
}

template int ObDataDictIterator::next_dict_entry(const ObDictMetaHeader &header, ObDictTenantMeta &dict_entry);
template int ObDataDictIterator::next_dict_entry(const ObDictMetaHeader &header, ObDictDatabaseMeta &dict_entry);
template int ObDataDictIterator::next_dict_entry(const ObDictMetaHeader &header, ObDictTableMeta &dict_entry);

int ObDataDictIterator::append_log_buf_with_base_header_(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(append_log_buf(buf, buf_len, 0))) {
  } else if (OB_FAIL(deserializ_base_log_header_())) {
    DDLOG(WARN, "deserializ_base_log_header_ failed", KR(ret), K_(palf_pos), K_(palf_buf_len));
  }

  return ret;
}

int ObDataDictIterator::deserializ_base_log_header_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader log_base_header;

  if (OB_ISNULL(palf_buf_)
      || OB_UNLIKELY(palf_pos_ != 0)) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(WARN, "expect valid palf_buf and palf_pos", KR(ret), K_(palf_pos));
  } else if (OB_FAIL(log_base_header.deserialize(palf_buf_, palf_buf_len_, palf_pos_))) {
    DDLOG(WARN, "deserialize log_base_header failed", KR(ret), K_(palf_pos), K_(palf_buf_len));
  } else if (logservice::ObLogBaseType::DATA_DICT_LOG_BASE_TYPE != log_base_header.get_log_type()) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(WARN, "expect log with header type DATA_DICT_LOG_BASE_TYPE", KR(ret), K(log_base_header));
  }

  return ret;
}

int ObDataDictIterator::prepare_dict_buf_(const int64_t required_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(required_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid args", KR(ret), K(required_size));
  } else if (required_size <= dict_buf_len_) {
    // current dict_buf is enough.
    dict_pos_ = 0;
  } else {
    const static int64_t block_size = 2 * _M_;
    const int64_t block_cnt = (required_size / block_size) + 1;
    const int64_t alloc_size = block_size * block_cnt;

    ob_dict_free(dict_buf_);

    if (OB_ISNULL(dict_buf_ = static_cast<char*>(ob_dict_malloc(alloc_size, tenant_id_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "malloc data_dict_buf failed", KR(ret),
          K(alloc_size), K(required_size), K(block_cnt), K(block_size));
    } else {
      dict_buf_len_ = alloc_size;
      dict_pos_ = 0;
    }
  }

  return ret;
}

int ObDataDictIterator::memcpy_palf_buf_to_dict_buf_(const int64_t copy_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(copy_size <= 0)
      || OB_UNLIKELY(palf_buf_len_ < palf_pos_ + copy_size)
      || OB_UNLIKELY(dict_buf_len_ < dict_pos_ + copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid copy_size, which may cause palf_buf/dict_buf overflow", KR(ret),
        K_(palf_pos), K_(palf_buf_len), K_(dict_pos), K_(dict_buf_len), K(copy_size));
  } else {
    DDLOG(DEBUG, "memcpy_palf_buf_to_dict_buf_", K(copy_size), K_(palf_pos), K_(palf_buf_len), K_(dict_pos));
    MEMCPY(dict_buf_ + dict_pos_, palf_buf_ + palf_pos_, copy_size);
    palf_pos_ += copy_size;
    dict_pos_ += copy_size;
  }

  return ret;
}

} // namespace datadict
} // namespace oceanbase
