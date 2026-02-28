// owner: yutong.lzs
// owner group: vector
/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_VECTOR_SEGMENT_INFO_H_
#define OB_ALL_VIRTUAL_VECTOR_SEGMENT_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace observer
{
class ObVectorSegmentInfoIterator
{
public:
  ObVectorSegmentInfoIterator()
    : allocator_("VecSegInfo"),
      index_idx_(0),
      segment_idx_(0),
      is_opened_(false),
      segment_infos_()
  {
  }
  virtual ~ObVectorSegmentInfoIterator() { reset(); }
  int open();
  int get_next_segment_info(ObVectorSegmentInfo &info);
  int load_all_index_segment();
  void reset();
  bool is_opened() const { return is_opened_; }

private:
  common::ObArenaAllocator allocator_;
  int64_t index_idx_;
  int64_t segment_idx_;
  bool is_opened_;
  common::ObSEArray<ObVectorSegmentInfo, 4> segment_infos_;
};

class ObAllVirtualVectorSegmentInfo : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    DATA_TABLE_ID,
    DATA_TABLET_ID,
    VBITMAP_TABLET_ID,
    INC_INDEX_TABLET_ID,
    SNAPSHOT_INDEX_TABLET_ID,
    BLOCK_COUNT,
    SEGMENT_TYPE,
    INDEX_TYPE,
    MEM_USED,
    MIN_VID,
    MAX_VID,
    VECTOR_CNT,
    REF_CNT,
    SEGMENT_STATE,
    SCN
  };
  ObAllVirtualVectorSegmentInfo();
  virtual ~ObAllVirtualVectorSegmentInfo();
public:
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  // whether a tenant is need return content.
  virtual bool is_need_process(uint64_t tenant_id) override;
  // deal with current tenant's row.
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  // release last tenant's resource.
  virtual void release_last_tenant() override;
private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObVectorSegmentInfo info_;
  ObVectorSegmentInfoIterator iter_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualVectorSegmentInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
