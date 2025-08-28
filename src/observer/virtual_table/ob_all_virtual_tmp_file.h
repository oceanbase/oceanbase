/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
 
#ifndef OB_ALL_VIRTUAL_TMP_FILE_H_
#define OB_ALL_VIRTUAL_TMP_FILE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_clock_generator.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileInfo;
class ObTmpFileGlobal;
}
namespace observer
{

class ObAllVirtualTmpFileInfo: public common::ObVirtualTableScannerIterator,
                                 public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualTmpFileInfo();
  ~ObAllVirtualTmpFileInfo();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row) { return execute(row);}
  virtual void reset();

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int get_next_tmp_file_info_(tmp_file::ObTmpFileInfo *tmp_file_info);
  int fill_columns_(tmp_file::ObTmpFileInfo *tmp_file_info);
  int fill_sn_column_(const uint64_t col_index, tmp_file::ObSNTmpFileInfo *tmp_file_info);
  #ifdef OB_BUILD_SHARED_STORAGE
  int fill_ss_column_(const uint64_t col_index, tmp_file::ObSSTmpFileInfo *tmp_file_info);
  #endif

private:
  enum
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    FILE_ID,
    TRACE_ID,
    DIR_ID,
    DATA_BYTES,
    START_OFFSET,
    IS_DELETING,
    CACHED_DATA_PAGE_NUM,
    WRITE_BACK_DATA_PAGE_NUM,
    FLUSHED_DATA_PAGE_NUM,
    REF_CNT,
    TOTAL_WRITES,
    UNALIGNED_WRITES,
    TOTAL_READS,
    UNALIGNED_READS,
    TOTAL_READ_BYTES,
    LAST_ACCESS_TIME,
    LAST_MODIFY_TIME,
    BIRTH_TIME,
    TMP_FILE_PTR,
    LABEL,
    META_TREE_EPOCH,
    META_TREE_LEVELS,
    META_BYTES,
    CACHED_META_PAGE_NUM,
    WRITE_BACK_META_PAGE_NUM,
    PAGE_FLUSH_CNT,
    TYPE,
    COMPRESSIBLE_FD,
    PERSISTED_TAIL_PAGE_WRITES,
    LACK_PAGE_CNT,
    TOTAL_TRUNCATED_PAGE_READ_CNT,
    TRUNCATED_PAGE_HITS,
    TOTAL_KV_CACHE_PAGE_READ_CNT,
    KV_CACHE_PAGE_HITS,
    TOTAL_UNCACHED_PAGE_READ_CNT,
    UNCACHED_PAGE_HITS,
    AGGREGATE_READ_IO_CNT,
    TOTAL_WBP_PAGE_READ_CNT,
    WBP_PAGE_HITS,
  };
  static const int64_t OB_MAX_FILE_LABEL_SIZE = tmp_file::ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char trace_id_buffer_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  char file_ptr_buffer_[20];
  char file_label_buffer_[OB_MAX_FILE_LABEL_SIZE];
  ObArray<int64_t> fd_arr_;
  bool is_ready_;
  int64_t fd_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTmpFileInfo);
};

}
}
#endif /* OB_ALL_VIRTUAL_TMP_FILE_H_ */
