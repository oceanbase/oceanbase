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

#ifndef OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_H_
#define OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_H_
#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#endif
#include "storage/ob_i_table.h"

namespace oceanbase
{
#ifdef OB_BUILD_TDE_SECURITY
namespace storage
{
class ObTenantTabletIterator;
}
namespace blocksstable
{
class ObSSTable;
}
#endif
namespace observer
{

class ObAllVirtualTabletEncryptInfo : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:

#ifndef OB_BUILD_TDE_SECURITY
  ObAllVirtualTabletEncryptInfo() {}
  virtual ~ObAllVirtualTabletEncryptInfo() {}
  int init(common::ObIAllocator *allocator, common::ObAddr &addr) { return OB_SUCCESS; }
  int inner_get_next_row(common::ObNewRow *&row) override { return OB_ITER_END; }
  int process_curr_tenant(common::ObNewRow *&row) override { return OB_ITER_END; }
  void release_last_tenant() override {}
#else
  enum COLUMN_ID_LIST
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    TABLET_ID,
    SVR_IP,
    SVR_PORT,
    MACRO_BLOCK_COUNT,
    ENCRYPTED_MACRO_BLOCK_COUNT,
  };
  struct TabletEncryptInfo
  {
    uint64_t tenant_id_;
    int64_t tablet_id_;
    int64_t macro_block_count_;
    int64_t encrypted_macro_block_count_;
    TO_STRING_KV(K_(tenant_id), K_(tablet_id),
        K_(macro_block_count), K_(encrypted_macro_block_count));
  };
  ObAllVirtualTabletEncryptInfo();
  virtual ~ObAllVirtualTabletEncryptInfo();
  int init(common::ObIAllocator *allocator, common::ObAddr &addr);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  // ObMultiTenantOperator interface
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  virtual bool is_need_process(uint64_t tenant_id) override;
  int get_next_tablet();
  int gen_row(common::ObNewRow *&row);
  void clean_cur_sstable();
  int gen_encrypt_info();
  int check_need_ignore(bool &need_ignore);
  int gen_tablet_range(const uint64_t tablet_id, common::ObNewRange &range);
private:
  common::ObAddr addr_;
  ObTenantTabletIterator *tablet_iter_;
  common::ObArenaAllocator tablet_allocator_;
  ObTabletHandle tablet_handle_;
  uint64_t tenant_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char range_buf_[common::OB_MAX_RANGE_LENGTH + 1]; // extra byte for '\0'
  storage::ObTableStoreIterator table_store_iter_;
  blocksstable::ObSSTable *curr_sstable_;
  blocksstable::ObIMacroBlockIterator *macro_iter_;
  blocksstable::ObMacroBlockDesc macro_desc_;
  blocksstable::ObDataMacroBlockMeta macro_meta_;
  ObArenaAllocator iter_allocator_;
  blocksstable::ObDatumRange curr_range_;
  common::ObObj objs_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  void *iter_buf_;
  TabletEncryptInfo tablet_encrypt_info_;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletEncryptInfo);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_H_ */
