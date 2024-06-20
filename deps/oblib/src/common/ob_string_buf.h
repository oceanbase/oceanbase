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

#ifndef OCEANBASE_COMMON_OB_STRING_BUF_
#define OCEANBASE_COMMON_OB_STRING_BUF_

#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace common
{
// This class is not thread safe.
// ObStringBufT is used to store the ObString and ObObj object.
template <typename PageAllocatorT = ModulePageAllocator, typename PageArenaT = PageArena<char, PageAllocatorT> >
class ObStringBufT
{
public:
  explicit ObStringBufT(const lib::ObMemAttr &attr,
                        const int64_t block_size = DEF_MEM_BLOCK_SIZE);
  explicit ObStringBufT(const lib::ObLabel &label = ObModIds::OB_STRING_BUF,
                        const int64_t block_size = DEF_MEM_BLOCK_SIZE)
    : ObStringBufT(lib::ObMemAttr(OB_SERVER_TENANT_ID, label), block_size) {}
  explicit ObStringBufT(const int64_t tenant_id,
                        const lib::ObLabel &label = ObModIds::OB_STRING_BUF,
                        const int64_t block_size = DEF_MEM_BLOCK_SIZE)
    : ObStringBufT(lib::ObMemAttr(tenant_id, label), block_size) {}
  explicit ObStringBufT(PageArenaT &arena);
  ~ObStringBufT();
  int reset();
  int reuse();
public:
  // Writes a string to buf.
  // @param [in] str the string object to be stored.
  // @param [out] stored_str records the stored ptr and length of string.
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int write_string(const ObString &str, ObString *stored_str);
  int write_number(const number::ObNumber &nmb, number::ObNumber *stored_nmb);
  // Writes an obj to buf.
  // @param [in] obj the object to be stored.
  // @param [out] stored_obj records the stored obj
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int write_obj(const ObObj &obj, ObObj *stored_obj);
  // Write a rowkey
  int write_string(const ObRowkey &rowkey, ObRowkey *stored_rowkey);

  OB_INLINE int64_t used() const
  {
    return arena_.used();
  }

  OB_INLINE int64_t total() const
  {
    return arena_.total();
  };

  OB_INLINE PageArenaT &get_arena() const {return arena_;}
  OB_INLINE void *alloc(const int64_t size) { return arena_.alloc(size);}
  OB_INLINE void *alloc(const int64_t size, const lib::ObMemAttr &attr) { UNUSED(attr); return alloc(size);}
  OB_INLINE void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz)
  {
    return arena_.realloc(static_cast<char *>(ptr), oldsz, newsz);
  }
  inline void free(void *ptr) { arena_.free(static_cast<char *>(ptr)); }
private:
  static const int64_t DEF_MEM_BLOCK_SIZE;
  static const int64_t MIN_DEF_MEM_BLOCK_SIZE;
private:
  PageArenaT local_arena_;
  PageArenaT &arena_;
  DISALLOW_COPY_AND_ASSIGN(ObStringBufT);
};
typedef ObStringBufT<> ObStringBuf;

} // namespace common
} // namespace oceanbase

#include "ob_string_buf.ipp"

#endif //OCEANBASE_COMMON_OB_STRING_BUF_
