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
#ifndef OB_ALL_VIRTUAL_SS_MACRO_CACHE_INFO_H_
#define OB_ALL_VIRTUAL_SS_MACRO_CACHE_INFO_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "lib/stat/ob_di_cache.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/blocksstable/ob_macro_block_id.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#endif

namespace oceanbase
{
namespace observer
{

class ObAllVirtualSSMacroCacheInfo : public common::ObVirtualTableScannerIterator,
                                     public omt::ObMultiTenantOperator
{
public:
ObAllVirtualSSMacroCacheInfo();
  virtual ~ObAllVirtualSSMacroCacheInfo();
  virtual void reset() override;

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

  // omt::ObMultiTenantOperator interface
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  virtual bool is_need_process(uint64_t tenant_id) override;

private:
#ifdef OB_BUILD_SHARED_STORAGE
  int get_meta_if_need();
  int get_macro_id_buf(const blocksstable::MacroBlockId &macro_id,
                       char *buf,
                       const int64_t buf_len) const;
  int get_macro_id_str_buf(const blocksstable::MacroBlockId &macro_id,
                           char *buf,
                           const int64_t buf_len) const;
  int get_path_buf(const blocksstable::MacroBlockId &macro_id, const bool is_local_cache);
#endif

private:
  enum TABLE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    MACRO_ID,
    TABLET_ID,
    OBJECT_TYPE,
    SIZE,
    LAST_ACCESS_TIME,
    CACHE_TYPE,
    IS_WRITE_CACHE,
    IS_IN_FIFO_LIST,
    REF_CNT,
    ACCESS_CNT,
    MACRO_ID_STR,

    LOCAL_PATH,
    REMOTE_PATH,
    INFO
  };

private:
  static const int64_t MAX_MACRO_ID_BUF_LENGTH = 256;
  static const int64_t MAX_MACRO_ID_STR_LENGTH = 256;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  char macro_id_buf_[MAX_MACRO_ID_BUF_LENGTH];
  char macro_id_str_buf_[MAX_MACRO_ID_STR_LENGTH];
  char local_path_buf_[common::MAX_PATH_SIZE];
  char remote_path_buf_[common::MAX_PATH_SIZE];
  uint64_t tenant_id_;
  int64_t bucket_count_;
  int64_t bucket_idx_;
  int64_t arr_idx_;
#ifdef OB_BUILD_SHARED_STORAGE
  ObSEArray<ObSSMacroCacheMetaHandle, 32> macro_cache_meta_arr_;
#endif
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_SS_MACRO_CACHE_INFO_H_ */
