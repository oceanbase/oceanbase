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
#pragma once

#include "lib/list/ob_dlist.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadTableBuilderAllocator final
{
public:
  ObDirectLoadTableBuilderAllocator()
  {
    tid_ = get_tid_cache();
  }
  ~ObDirectLoadTableBuilderAllocator()
  {
    assert_in_own_thread();
    OB_ASSERT(using_list_.is_empty());
  }
  void reset()
  {
    assert_in_own_thread();
    Item *item = nullptr;
    DLIST_REMOVE_ALL_NORET(item, using_list_)
    {
      ObIDirectLoadPartitionTableBuilder *table_builder =
        (ObIDirectLoadPartitionTableBuilder *)item->buf_;
      table_builder->~ObIDirectLoadPartitionTableBuilder();
      ob_free(item);
    }
    OB_ASSERT(using_list_.is_empty());
  }
  template <typename T, typename... Args>
  T *alloc(Args &&... args)
  {
    assert_in_own_thread();
    T *t = nullptr;
    void *buf = nullptr;
    ObMemAttr attr(MTL_ID(), "TLD_TB_Alloc");
    if (OB_NOT_NULL(buf = ob_malloc(sizeof(Item) + sizeof(T), attr))) {
      Item *item = new (buf) Item;
      t = new (item->buf_) T(args...);
      using_list_.add_last(item);
    }
    return t;
  }
  void free(ObIDirectLoadPartitionTableBuilder *table_builder)
  {
    assert_in_own_thread();
    if (OB_NOT_NULL(table_builder)) {
      table_builder->~ObIDirectLoadPartitionTableBuilder();
      Item *item = (Item *)table_builder - 1;
      using_list_.remove(item);
      item->~Item();
      ob_free(item);
    }
  }
  OB_INLINE void assert_in_own_thread()
  {
    const int64_t tid = get_tid_cache();
    OB_ASSERT(tid == tid_);
  }

private:
  struct Item : public common::ObDLinkBase<Item>
  {
    char buf_[];
  };

private:
  int64_t tid_;
  ObDList<Item> using_list_;
};

OB_INLINE ObDirectLoadTableBuilderAllocator *get_table_builder_allocator()
{
  RLOCAL_INLINE(ObDirectLoadTableBuilderAllocator, allcator);
  return &allcator;
}

} // namespace storage
} // namespace oceanbase
