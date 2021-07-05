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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_MERGE_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_i_store.h"
#include "storage/ob_row_fuse.h"
#include "storage/ob_warm_up.h"
#include "storage/ob_table_store_stat_mgr.h"

namespace oceanbase {
namespace storage {
class ObIStoreRowFilter;
class ObMultipleMerge : public ObQueryRowIterator {
public:
  typedef common::ObSEArray<ObStoreRowIterator*, common::MAX_TABLE_CNT_IN_STORAGE> MergeIterators;

public:
  ObMultipleMerge();
  virtual ~ObMultipleMerge();
  virtual int init(
      const ObTableAccessParam& param, ObTableAccessContext& context, const ObGetTableParam& get_table_param);
  virtual int get_next_row(ObStoreRow*& row) override;
  virtual void reset() override;
  virtual void reuse();
  inline const ObRowStat& get_row_stat() const
  {
    return row_stat_;
  }
  inline bool is_read_memtable_only() const
  {
    return read_memtable_only_;
  }

  void disable_padding()
  {
    need_padding_ = false;
  }
  void disable_fill_default()
  {
    need_fill_default_ = false;
  }
  void disable_fill_virtual_column()
  {
    need_fill_virtual_columns_ = false;
  }
  void disable_output_row_with_nop()
  {
    need_output_row_with_nop_ = false;
  }
  virtual int switch_iterator(const int64_t range_array_idx) override;

protected:
  int open();
  virtual int calc_scan_range() = 0;
  virtual int construct_iters() = 0;
  virtual int is_range_valid() const = 0;
  virtual OB_INLINE int prepare()
  {
    return common::OB_SUCCESS;
  }
  virtual int inner_get_next_row(ObStoreRow& row) = 0;
  virtual void collect_merge_stat(ObTableStoreStat& stat) const = 0;
  int alloc_row(common::ObIAllocator& allocator, const int64_t cell_cnt, ObStoreRow& row);
  int add_iterator(ObStoreRowIterator& iter);  // for unit test
  const ObTableIterParam* get_actual_iter_param(const ObITable* table) const;
  int project_row(const ObStoreRow& unprojected_row, const common::ObIArray<int32_t>* projector,
      const int64_t range_idx_delta, ObStoreRow& projected_row);
  void reuse_iter_array();
  virtual int skip_to_range(const int64_t range_idx);

private:
  class ObTableCompartor {
  public:
    ObTableCompartor(int& ret) : ret_(ret)
    {}
    ~ObTableCompartor() = default;
    bool operator()(ObITable* left_table, ObITable* right_table)
    {
      bool bret = false;
      if (nullptr == left_table || nullptr == right_table) {
        ret_ = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid arguments", K(ret_), KP(left_table), KP(right_table));
      } else {
        int64_t comp_ret = 0;
        comp_ret = left_table->get_end_log_ts() - right_table->get_end_log_ts();
        if (0 == comp_ret) {
          comp_ret = right_table->get_start_log_ts() - left_table->get_start_log_ts();
        }
        if (0 == comp_ret) {
          comp_ret = left_table->get_snapshot_version() - right_table->get_snapshot_version();
          if (0 == comp_ret) {
            comp_ret = right_table->get_start_log_ts() - left_table->get_start_log_ts();
          }
        }
        bret = comp_ret < 0;
      }
      return bret;
    }

  private:
    int& ret_;
  };

private:
  int fuse_default(common::ObNewRow& row);
  int pad_columns(common::ObNewRow& row);
  int fill_virtual_columns(common::ObNewRow& row);
  int check_result(const common::ObNewRow& row, bool& satisfy);
  int deal_with_tables(ObTableAccessContext& context, ObTablesHandle& tables_handle);
  int project_row(const ObStoreRow& unprojected_row, ObStoreRow& projected_row);
  // project to output expressions
  int project2output_exprs(ObStoreRow& unprojected_row, ObStoreRow& cur_row);
  // destruct all iterators and reuse iter array
  int prepare_read_tables();
  int refresh_table_on_demand();
  int check_need_refresh_table(bool& need_refresh);
  int save_curr_rowkey();
  int reset_tables();
  void remove_filter();
  int report_table_store_stat();
  int get_range_array_idx(int64_t& range_array_idx);
  int check_filtered(const ObNewRow& row, bool& filtered);
  int check_row_in_current_range(const ObStoreRow& row);
  int fill_scale(common::ObNewRow& row);

protected:
  common::ObArenaAllocator padding_allocator_;
  MergeIterators iters_;
  const ObTableAccessParam* access_param_;
  ObTableAccessContext* access_ctx_;
  ObTablesHandle tables_handle_;
  ObStoreRow cur_row_;
  ObStoreRow filter_row_;
  ObStoreRow unprojected_row_;
  ObStoreRow full_row_;
  ObStoreRow* next_row_;
  const ObIArray<int32_t>* out_cols_projector_;
  int64_t curr_scan_index_;
  ObStoreRowkey curr_rowkey_;
  ObNopPos nop_pos_;
  ObRowStat row_stat_;
  int64_t scan_cnt_;
  int64_t filt_cnt_;
  bool need_padding_;
  bool need_fill_default_;          // disabled by join mv scan
  bool need_fill_virtual_columns_;  // disabled by join mv scan
  bool need_output_row_with_nop_;   // for sampling increment data
  const ObIStoreRowFilter* row_filter_;
  bool inited_;
  int64_t range_idx_delta_;
  ObGetTableParam get_table_param_;
  int64_t relocate_cnt_;
  ObTableStoreStat table_stat_;
  bool skip_refresh_table_;
  bool read_memtable_only_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleMerge);
};
}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_MULTIPLE_MERGE_
