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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_UTIL_
#define OCEANBASE_STORAGE_OB_STORAGE_UTIL_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObStorageDatum;
}
namespace storage
{

int pad_column(const ObObjMeta &obj_meta,
               const ObAccuracy accuracy,
               common::ObIAllocator &padding_alloc,
               blocksstable::ObStorageDatum &datum);

int pad_column(const ObAccuracy accuracy,
               common::ObIAllocator &padding_alloc,
               common::ObObj &cell);

int pad_column(const common::ObAccuracy accuracy,
               sql::ObEvalCtx &ctx,
               sql::ObExpr &expr);

int pad_on_datums(const common::ObAccuracy accuracy,
                  const common::ObCollationType cs_type,
                  common::ObIAllocator &padding_alloc,
                  int64_t row_count,
                  common::ObDatum *&datums);

int cast_obj(const common::ObObjMeta &src_meta, common::ObIAllocator &cast_allocator, common::ObObj &obj);

OB_INLINE bool can_do_ascii_optimize(common::ObCollationType cs_type)
{
  return common::CS_TYPE_UTF8MB4_GENERAL_CI == cs_type
      || common::CS_TYPE_UTF8MB4_BIN == cs_type
      || common::CS_TYPE_UTF8MB4_UNICODE_CI == cs_type
      || common::CS_TYPE_GBK_CHINESE_CI == cs_type
      || common::CS_TYPE_GBK_BIN == cs_type;
}

OB_INLINE bool is_ascii_less_8(const char *str, int64_t len)
{
    bool is_not_ascii = true;
    const uint8_t *val = reinterpret_cast<const uint8_t *>(str);
    switch (len) {
    case 0:
      is_not_ascii = false;
        break;
    case 1:
        is_not_ascii = (0x80 & val[0]);
        break;
    case 2:
        is_not_ascii = 0x8080 & *((const uint16_t *)val);
        break;
    case 3:
        is_not_ascii = (0x8080 & *(const uint16_t *)val) | (0x80 & val[2]);
        break;
    case 4:
        is_not_ascii = (0x80808080U & *((const uint32_t *)val));
        break;
    case 5:
        is_not_ascii = (0x80808080U & *((const uint32_t *)val)) | (0x80 & val[4]);
        break;
    case 6:
        is_not_ascii = (0x80808080U & *(const uint32_t *)val) | (0x8080 & *(const uint16_t *)(val + 4));
        break;
    case 7:
        is_not_ascii = (0x80808080U & *(const uint32_t *)val) | (0x80808080U & *(const uint32_t *)(val + 3));
        break;
    }
    return !is_not_ascii;
}

OB_INLINE bool is_ascii_str(const char *str, const int64_t len)
{
  bool bret = true;
  if (len >= 8) {
    const int64_t length = len / 8;
    const uint64_t *vals = reinterpret_cast<const uint64_t *>(str);
    for (int64_t i = 0; bret && i < length; i++) {
      if (vals[i] & 0x8080808080808080UL) {
        bret = false;
      }
    }
    bret = bret && is_ascii_less_8(str + len / 8 * 8, len % 8);
  } else {
    bret = is_ascii_less_8(str, len);
  }
  return bret;
}

class ObObjBufArray final
{
public:
  ObObjBufArray()
      : capacity_(0),
      is_inited_(false),
      data_(NULL),
      allocator_(NULL)
  {
    MEMSET(local_data_buf_, 0, LOCAL_ARRAY_SIZE * sizeof(common::ObObj));
  }
  ~ObObjBufArray()
  {
    reset();
  }

  int init(common::ObIAllocator *allocator)
  {
    int ret = common::OB_SUCCESS;
    if (IS_INIT) {
      ret = common::OB_INIT_TWICE;
      STORAGE_LOG(WARN, "init twice", K(ret), K(is_inited_));
    } else if (OB_ISNULL(allocator)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(allocator));
    } else {
      allocator_ = allocator;
      data_ = reinterpret_cast<common::ObObj*>(local_data_buf_);
      capacity_ = LOCAL_ARRAY_SIZE;
      is_inited_ = true;
    }
    return ret;
  }

  inline bool is_inited() const { return is_inited_; }

  inline int reserve(int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObObjBufArray not inited", K(ret), K(is_inited_));
    } else if (count > capacity_) {
      int64_t new_size = count * sizeof(common::ObObj);
      common::ObObj *new_data = reinterpret_cast<common::ObObj *>(allocator_->alloc(new_size));
      if (OB_NOT_NULL(new_data)) {
        if ((char *)data_ != local_data_buf_) {
          allocator_->free(data_);
        }
        MEMSET(new_data, 0, new_size);
        data_ = new_data;
        capacity_ = count;
      } else {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "no memory", K(ret), K(new_size), K(capacity_));
      }
    }
    return ret;
  }

  inline int64_t get_count() const { return capacity_; }

  inline common::ObObj *get_data() { return data_; }

  void reset()
  {
    if (NULL != allocator_ && (char *)data_ != local_data_buf_) {
      allocator_->free(data_);
    }
    allocator_ = NULL;
    data_ = NULL;
    capacity_ = 0;
    is_inited_ = false;
  }

  inline common::ObObj &at(int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < capacity_);
    return data_[idx];
  }

protected:
  const static int64_t LOCAL_ARRAY_SIZE = 64;
  int64_t capacity_;
  bool is_inited_;
  common::ObObj *data_;
  char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(common::ObObj)];
  common::ObIAllocator *allocator_;
};

}
}

#endif // OCEANBASE_STORAGE_OB_STORAGE_UTIL_
