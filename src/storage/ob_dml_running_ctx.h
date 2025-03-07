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

#ifndef OCEANBASE_STORAGE_OB_DML_RUNNING_CTX
#define OCEANBASE_STORAGE_OB_DML_RUNNING_CTX

#include "lib/container/ob_iarray.h"
#include "storage/ob_i_store.h"
#include "storage/ob_relative_table.h"
#include "share/scn.h"
#include "storage/lob/ob_lob_tablet_dml.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace storage
{
class ObTablet;
struct ObDMLBaseParam;
class ObRelativeTable;
class ObSingleRowGetter;

struct ObDMLRunningCtx
{
public:
  ObDMLRunningCtx(
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    common::ObIAllocator &allocator,
    const blocksstable::ObDmlFlag dml_flag,
    bool is_need_row_datum_utils = false);
  ~ObDMLRunningCtx();

  int init(
      const common::ObIArray<uint64_t> *column_ids,
      const common::ObIArray<uint64_t> *upd_col_ids,
      ObMultiVersionSchemaService *schema_service,
      ObTabletHandle &tablet_handle);
  static int prepare_column_desc(
      const common::ObIArray<uint64_t> &column_ids,
      const ObRelativeTable &table,
      ObColDescIArray &col_descs);
  OB_INLINE bool is_main_table_rowkey_col(const int16_t col_idx)
  {
    return main_table_rowkey_col_flag_.count() > 0 && main_table_rowkey_col_flag_.at(col_idx);
  }
private:
  int prepare_column_info(const common::ObIArray<uint64_t> &column_ids);
  int prepare_relative_table(
      const share::schema::ObTableSchemaParam &schema,
      ObTabletHandle &tablet_handle,
      const share::SCN &read_snapshot);
  int check_need_old_row_legitimacy();
  int init_cmp_funcs();
  int check_schema_version(share::schema::ObMultiVersionSchemaService &schema_service,
                           const uint64_t tenant_id,
                           const uint64_t table_id,
                           const int64_t tenant_schema_version,
                           const int64_t table_version,
                           ObTabletHandle &tablet_handle);
  int check_tenant_schema_version(
      share::schema::ObMultiVersionSchemaService &schema_service,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t tenant_schema_version);

public:
  ObStoreCtx &store_ctx_;
  const ObDMLBaseParam &dml_param_;
  common::ObIAllocator &allocator_;
  const blocksstable::ObDmlFlag dml_flag_;
  ObRelativeTable relative_table_;
  const share::schema::ColumnMap *col_map_;
  const ObColDescIArray *col_descs_;
  const common::ObIArray<uint64_t> *column_ids_;
  blocksstable::ObDatumRow datum_row_;
  blocksstable::ObStoreCmpFuncs cmp_funcs_;
  bool is_old_row_valid_for_lob_;
  bool is_need_check_old_row_;
  bool is_udf_;
  bool has_lob_rowkey_;
  ObLobTabletDmlCtx lob_dml_ctx_;
  common::ObFixedArray<bool, common::ObIAllocator> main_table_rowkey_col_flag_;

private:
  share::schema::ObSchemaGetterGuard schema_guard_;
  bool is_need_row_datum_utils_;
  bool is_inited_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_DML_RUNNING_CTX
