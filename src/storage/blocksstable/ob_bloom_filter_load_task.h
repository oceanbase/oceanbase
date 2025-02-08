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

#ifndef OB_BLOOM_FILTER_LOAD_MAP_H_
#define OB_BLOOM_FILTER_LOAD_MAP_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/queue/ob_link_queue.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{

namespace storage
{

class ObRowsInfo;
class ObRowKeysInfo;
class ObSSTableReadHandle;

} /* namespace storage */

namespace blocksstable
{

class ObDataMacroBlockMeta;

struct ObBloomFilterLoadKey
{
public:
  ObBloomFilterLoadKey() : ls_id_(), table_key_()
  {
  }
  ObBloomFilterLoadKey(const share::ObLSID &ls_id, const storage::ObITable::TableKey &table_key)
      : ls_id_(ls_id), table_key_(table_key)
  {
  }
  ~ObBloomFilterLoadKey() {}
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator == (const ObBloomFilterLoadKey &other) const;
  bool is_valid() const;
  TO_STRING_KV(K(ls_id_), K(table_key_));

public:
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
};

class ObBloomFilterLoadTaskQueue
{
public:
  struct KeyLink : public common::ObLink
  {
  public:
    explicit KeyLink(const ObBloomFilterLoadKey &key) : key_(key) {}
    ~KeyLink() {}
    TO_STRING_KV(K(key_));

  public:
    ObBloomFilterLoadKey key_;
  };

  struct ValuePair
  {
  public:
    ValuePair(const MacroBlockId &macro_id, common::ObIAllocator &allocator)
        : macro_id_(macro_id), rowkey_(nullptr), allocator_(&allocator)
    {
    }

  private:
    ValuePair() : macro_id_(), rowkey_(nullptr), allocator_(nullptr)
    {
    }
    friend class ObArrayImpl<ValuePair>;

  public:
    // Shallow copy.
    ValuePair(const ValuePair &src);
    ~ValuePair() {}
    bool is_valid()
    {
      return nullptr != allocator_ && macro_id_.is_valid() && nullptr != rowkey_ && rowkey_->is_valid();
    }
    int alloc_rowkey(const ObCommonDatumRowkey &common_rowkey);
    int recycle_rowkey();
    TO_STRING_KV(K(macro_id_), KPC(rowkey_), KP(allocator_));

  public:
    MacroBlockId macro_id_;
    ObDatumRowkey * rowkey_;
    common::ObIAllocator * allocator_;
  };

  struct ValuePairCmpFunc
  {
  public:
    explicit ValuePairCmpFunc(const blocksstable::ObStorageDatumUtils & datum_utils) : datum_utils_(&datum_utils) {}
    ~ValuePairCmpFunc() {}
    bool operator () (const ValuePair & lhs, const ValuePair & rhs);

  public:
    const blocksstable::ObStorageDatumUtils * datum_utils_;
  };

public:
  ObBloomFilterLoadTaskQueue();
  ~ObBloomFilterLoadTaskQueue();
  int init();
  void reset();
  int push_task(const storage::ObITable::TableKey &sstable_key,
                const share::ObLSID &ls_id,
                const MacroBlockId &macro_id,
                const ObCommonDatumRowkey &common_rowkey);
  int pop_task(ObBloomFilterLoadKey &key, ObArray<ValuePair> *&array);
  int recycle_array(ObArray<ValuePair> *array);

private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterLoadTaskQueue);

private:
  common::hash::ObHashMap<ObBloomFilterLoadKey,
                          ObArray<ValuePair> *,
                          common::hash::NoPthreadDefendMode>
      load_map_;
  ObBucketLock bucket_lock_;
  common::ObLinkQueue fetch_queue_;
  common::ObFIFOAllocator allocator_;
  bool is_inited_;
};

class ObMacroBlockBloomFilterLoadTG : public lib::TGRunnable
{
private:
  using ValuePair = ObBloomFilterLoadTaskQueue::ValuePair;

public:
  ObMacroBlockBloomFilterLoadTG();
  ~ObMacroBlockBloomFilterLoadTG();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void run1() override;
  int add_load_task(const storage::ObITable::TableKey &sstable_key,
                    const share::ObLSID &ls_id,
                    const MacroBlockId &macro_id,
                    const ObCommonDatumRowkey &common_rowkey);

private:
  int do_multi_load(const ObBloomFilterLoadKey &key, ObArray<ValuePair> *array);
  int load_macro_block_bloom_filter(const ObDataMacroBlockMeta &macro_meta);
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockBloomFilterLoadTG);

private:
  int tg_id_;
  ObThreadCond idle_cond_;
  ObBloomFilterLoadTaskQueue load_task_queue_;
  common::ObFIFOAllocator allocator_;
  bool is_inited_;
};


} /* namespace blocksstable */
} /* namespace oceanbase */

#endif /* OB_BLOOM_FILTER_LOAD_MAP_H_ */
