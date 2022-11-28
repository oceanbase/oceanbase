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

#ifndef OB_DEFENSIVE_CHECK_MGR_H_
#define OB_DEFENSIVE_CHECK_MGR_H_
#include "storage/blocksstable/ob_fuse_row_cache.h"

namespace oceanbase
{
namespace transaction
{

struct ObDefensiveCheckRecordExtend
{
  ObDefensiveCheckRecordExtend() { reset(); }
  ~ObDefensiveCheckRecordExtend() { reset(); }
  void reset()
  {
    fist_access_table_start_log_ts_ = 0;
    total_table_handle_cnt_ = 0;
    start_access_table_idx_ = INT64_MAX;
    end_access_table_idx_ = INT64_MAX;
    use_fuse_cache_data_ = false;
    is_all_data_from_memtable_ = false;
    query_flag_.reset();
  }
  TO_STRING_KV(K_(fist_access_table_start_log_ts),
               K_(total_table_handle_cnt),
               K_(start_access_table_idx),
               K_(end_access_table_idx),
               K_(use_fuse_cache_data),
               K_(is_all_data_from_memtable),
               K_(query_flag));
public:
  int64_t fist_access_table_start_log_ts_;
  int64_t total_table_handle_cnt_;
  int64_t start_access_table_idx_;
  int64_t end_access_table_idx_;
  bool use_fuse_cache_data_;
  bool is_all_data_from_memtable_;
  ObQueryFlag query_flag_;
};

class SingleRowDefensiveRecord
{
public:
  SingleRowDefensiveRecord() : generate_ts_(0) {}
  ~SingleRowDefensiveRecord() { reset(); }
  void reset();
  void destroy() { reset(); }
  int deep_copy(const blocksstable::ObDatumRow &row,
                const blocksstable::ObDatumRowkey &rowkey,
                const ObDefensiveCheckRecordExtend &extend_info);

  TO_STRING_KV(K_(row), K_(generate_ts), K_(rowkey), K_(extend_info));

  blocksstable::ObDatumRow row_;
  int64_t generate_ts_;
  ObArenaAllocator allocator_;
  blocksstable::ObDatumRowkey rowkey_;
  ObDefensiveCheckRecordExtend extend_info_;
};

typedef common::ObSEArray<SingleRowDefensiveRecord *, 12> ObSingleRowDefensiveRecordArray;

class ObSingleTabletDefensiveCheckInfo : public ObTransHashLink<ObSingleTabletDefensiveCheckInfo>
{
public:
  ObSingleTabletDefensiveCheckInfo() { }
  ~ObSingleTabletDefensiveCheckInfo() { reset(); }
  int init(const ObTabletID &tablet_id);
  void reset();
  void destroy() { reset(); }
  bool contain(const ObTabletID &tablet_id) { return tablet_id_ == tablet_id; }
  int add_record(SingleRowDefensiveRecord *record);
  ObSingleRowDefensiveRecordArray &get_record_arr() { return record_arr_; }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
private:
  ObTabletID tablet_id_;
  ObSingleRowDefensiveRecordArray record_arr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSingleTabletDefensiveCheckInfo);
};

class ObSingleTabletDefensiveCheckInfoAlloc
{
public:
  static const int64_t OP_LOCAL_NUM = 128;
  ObSingleTabletDefensiveCheckInfo * alloc_value()
  {
    return NULL;
  }

  void free_value(ObSingleTabletDefensiveCheckInfo* info)
  {
    if (NULL != info) {
      op_free(info);
      info = NULL;
    }
  }
};

typedef ObTransHashMap<ObTabletID,
                       ObSingleTabletDefensiveCheckInfo,
                       ObSingleTabletDefensiveCheckInfoAlloc,
                       common::SpinRWLock> ObTxDefensiveCheckInfoMap;

class ObDefensiveCheckMgr
{
public:
  ObDefensiveCheckMgr() : is_inited_(false)  { }
  ~ObDefensiveCheckMgr() { destroy(); }
  int init();
  void reset();
  void destroy() { reset(); }
  int put(const ObTabletID &tablet_id,
          const blocksstable::ObDatumRow &row,
          const blocksstable::ObDatumRowkey &rowkey,
          const ObDefensiveCheckRecordExtend &extend_info);
  void dump(const ObTabletID &tablet_id);
private:
  static int64_t max_record_cnt_;
  typedef ObSmallSpinLockGuard<common::ObByteLock> Guard;
  bool is_inited_;
  common::ObByteLock lock_;
  ObTxDefensiveCheckInfoMap map_;
};

} /* namespace transaction*/
} /* namespace oceanbase */

#endif /* OB_DEFENSIVE_CHECK_MGR_H_ */
