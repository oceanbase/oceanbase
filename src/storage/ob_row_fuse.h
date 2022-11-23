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

#ifndef OCEANBASE_STORAGE_OB_ROW_FUSE_
#define OCEANBASE_STORAGE_OB_ROW_FUSE_

#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{

class ObNopPos
{
public:
  ObNopPos() : allocator_(NULL),
               capacity_(0),
               count_(0),
               nops_(NULL)
  {}
  ~ObNopPos()
  {
    destroy();
  }
  void free();
  int init(common::ObIAllocator &allocator, const int64_t capacity);
  void destroy();
  void reset()
  {
    count_ = 0;
  }
  bool is_valid() const
  {
    return (capacity_ > 0 && count_ >= 0 && count_ <= capacity_);
  }
  int get_nop_pos(const int64_t idx, int64_t &pos);
  int64_t count() const { return count_; }
  int64_t capacity() const { return capacity_; }
  int set_count(const int64_t count);
  int set_nop_pos(const int64_t idx, const int64_t pos);
  TO_STRING_KV("nops", common::ObArrayWrap<int16_t>(nops_, count_));
public:
  common::ObIAllocator *allocator_;
  int64_t capacity_;
  int64_t count_;
  int16_t *nops_;
};

class ObObjShallowCopy
{
public:
  OB_INLINE int operator () (const common::ObObj &src_obj, common::ObObj &dst_obj)
  {
    dst_obj = src_obj;
    return common::OB_SUCCESS;
  }
};

class ObObjDeepCopy
{
public:
  ObObjDeepCopy(common::ObArenaAllocator &allocator) : allocator_(allocator) {}
  OB_INLINE int operator() (const common::ObObj &src_obj, common::ObObj &dst_obj)
  {
    return common::deep_copy_obj(allocator_, src_obj, dst_obj);
  }
private:
  common::ObArenaAllocator &allocator_;
};

class ObRowFuse
{
public:
  static int fuse_row(const ObStoreRow &former,
                            ObStoreRow &result,
                            ObNopPos &nop_pos,
                            bool &final_result,
                            ObObjDeepCopy *obj_copy = nullptr);

  static int fuse_row(const blocksstable::ObDatumRow &former,
                            blocksstable::ObDatumRow &result,
                            ObNopPos &nop_pos,
                            bool &final_result,
                            common::ObIAllocator *allocator = nullptr);

  static ObObjShallowCopy shallow_copy_;
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ROW_FUSE_
