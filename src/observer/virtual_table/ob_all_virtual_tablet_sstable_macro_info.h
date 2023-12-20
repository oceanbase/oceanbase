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

#ifndef OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_H_
#define OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_H_
#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ob_i_table.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "observer/omt/ob_multi_tenant.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace storage
{
class ObTenantTabletIterator;
class ObTabletMeta;
}
namespace observer
{

class ObAllVirtualTabletSSTableMacroInfo : public common::ObVirtualTableScannerIterator,
                                           public omt::ObMultiTenantOperator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    END_LOG_SCN,
    MACRO_IDX_IN_SSTABLE,
    MACRO_LOGIC_VERSION,
    MACRO_BLOCK_IDX,
    DATA_SEQ,
    ROW_COUNT,
    ORIGINAL_SIZE,
    ENCODING_SIZE,
    COMPRESSED_SIZE,
    OCCUPY_SIZE,
    MICRO_BLOCK_CNT,
    DATA_CHECKSUM,
    START_KEY,
    END_KEY,
    BLOCK_TYPE,
    COMPRESSOR_NAME,
    ROW_STORE_TYPE,
    CG_IDX
  };
public:
  ObAllVirtualTabletSSTableMacroInfo();
  virtual ~ObAllVirtualTabletSSTableMacroInfo();
  int init(common::ObIAllocator *allocator, common::ObAddr &addr);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  class MacroInfo final
  {
  public:
    MacroInfo();
    ~MacroInfo();
    void reset();
    OB_INLINE bool is_valid() const
    {
      return macro_block_index_ > 1
          && (compressor_type_ > ObCompressorType::INVALID_COMPRESSOR
              && compressor_type_ < ObCompressorType::MAX_COMPRESSOR)
          && row_store_type_ < ObRowStoreType::MAX_ROW_STORE;
    }
    TO_STRING_KV(K(data_seq_), K(macro_logic_version_), K(macro_block_index_), K(micro_block_count_),
                 K(data_checksum_), K(occupy_size_), K(original_size_), K_(data_size), K(data_zsize_),
                 K(store_range_), K(row_count_), K(compressor_type_), K(row_store_type_));
  public:
    int64_t data_seq_;
    uint64_t macro_logic_version_;
    int64_t macro_block_index_;
    int64_t micro_block_count_;
    int64_t data_checksum_;
    int64_t occupy_size_;
    int64_t original_size_;
    int64_t data_size_;
    int64_t data_zsize_;
    ObStoreRange store_range_;
    int32_t row_count_;
    ObCompressorType compressor_type_;
    ObRowStoreType row_store_type_;
  };
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

  int set_key_ranges(const common::ObIArray<common::ObNewRange> &key_ranges);
  int gen_row(const MacroInfo &macro_info, common::ObNewRow *&row);
  int next_tenant();
  int get_next_macro_info(MacroInfo &info);
  int get_next_tablet();
  int get_next_sstable();
  void clean_cur_sstable();
  bool check_tenant_need_ignore(uint64_t tenant_id);
  bool check_tablet_need_ignore(const ObTabletMeta &tablet_meta);
  bool check_sstable_need_ignore(const ObITable::TableKey &table_key);
  int gen_sstable_range(common::ObNewRange &range);
  int get_macro_info(const blocksstable::MacroBlockId &macro_id, MacroInfo &info);
  int get_macro_info(const blocksstable::ObMacroBlockDesc &macro_desc, MacroInfo &info);
private:
  common::ObAddr addr_;
  ObTenantTabletIterator *tablet_iter_;
  common::ObArenaAllocator tablet_allocator_;
  ObTabletHandle tablet_handle_;
  common::ObSEArray<ObColDesc, 16> cols_desc_;
  int64_t ls_id_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char start_key_buf_[common::OB_MAX_ROW_KEY_LENGTH + 1]; // extra byte for '\0'
  char end_key_buf_[common::OB_MAX_ROW_KEY_LENGTH + 1]; // extra byte for '\0'
  storage::ObTableStoreIterator table_store_iter_;
  blocksstable::ObSSTable *curr_sstable_;
  blocksstable::ObSSTableMetaHandle curr_sstable_meta_handle_;
  blocksstable::ObIMacroBlockIterator *macro_iter_;
  blocksstable::ObMacroIdIterator other_blk_iter_;
  ObArenaAllocator iter_allocator_;
  ObArenaAllocator rowkey_allocator_;
  blocksstable::ObDatumRange curr_range_;
  common::ObObj objs_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  int64_t block_idx_;
  void *iter_buf_;
  char *io_buf_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletSSTableMacroInfo);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_H_ */
