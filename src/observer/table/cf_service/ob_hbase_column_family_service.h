/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_HBASE_COLUMN_FAMILY_SERVICE_H
#define _OB_HBASE_COLUMN_FAMILY_SERVICE_H

#include "observer/table/common/ob_hbase_common_struct.h"
#include "observer/table/adapters/ob_i_adapter.h"

namespace oceanbase
{
namespace table
{

class ObHbaseColumnFamilyService
{
public:
  ObHbaseColumnFamilyService() = default;
  virtual ~ObHbaseColumnFamilyService() = default;

  static int alloc_family_sevice(ObIAllocator &alloc, bool is_multi_cf_req, ObHbaseColumnFamilyService *&cf_service);

  virtual int put(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx);
  virtual int query(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx, ObHbaseQueryResultIterator *&result_iter);
  virtual int del(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx);
  virtual int del(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx);
  int del(const ObHbaseQuery &query, ObNewRow &cell, ObTableExecCtx &exec_ctx);
  int construct_query(const ObITableEntity &cell, ObTableExecCtx &exec_ctx, ObHbaseQuery &hbase_query);
protected:
  virtual int delete_cell(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx,
                          const ObNewRow &cell, ObIHbaseAdapter &adapter);
  static bool is_legal_family_name(const ObString &family_name);
};

class ObHbaseMultiCFService : public ObHbaseColumnFamilyService
{
public:
  virtual int put(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx) override;
  virtual int del(const ObHbaseTableCells &table_cells, ObTableExecCtx &exec_ctx) override;
  virtual int query(const ObHbaseQuery &query, ObTableExecCtx &exec_ctx, ObHbaseQueryResultIterator *&result_iter) override;
public:
  static int construct_table_name(common::ObIAllocator &allocator,
                                  const common::ObString &table_group_name,
                                  const common::ObString &family_name,
                                  common::ObString &table_name);
private:
  int delete_all_family(const ObITableEntity &del_cell, const ObString &table_group_name,
                        ObTableExecCtx &exec_ctx, const uint64_t table_id, const ObTabletID &tablet_id);
  int delete_family(const ObITableEntity &del_cell, const ObString &family_name,
                        ObTableExecCtx &exec_ctx, const uint64_t table_id, const ObTabletID &tablet_id);
private:
  static int find_real_table_tablet_id(ObTableExecCtx &exec_ctx,
                                       const uint64_t arg_table_id,
                                       const ObTabletID arg_tablet_id,
                                       const ObString &family_name,
                                       uint64_t &real_table_id,
                                       ObTabletID &real_tablet_id);
  static int find_real_table_tablet_id(ObTableExecCtx &exec_ctx,
                                       const uint64_t arg_table_id,
                                       const ObTabletID &arg_tablet_id,
                                       const ObSimpleTableSchemaV2 &real_simple_schema,
                                       uint64_t &real_table_id,
                                       ObTabletID &real_tablet_id);
  static int get_family_from_cell(const ObITableEntity &entity, ObString &family);
  static int get_family_from_cell(const ObNewRow &cell, ObString &family);
  static int remove_family_from_qualifier(const ObITableEntity &entity);
  static int remove_family_from_qualifier(const ObNewRow &cell);
};

class ObHbaseCfServiceGuard
{
public:
  ObHbaseCfServiceGuard(common::ObIAllocator &allocator, bool is_multi_cf_req)
    : allocator_(allocator), is_multi_cf_req_(is_multi_cf_req), cf_service_(nullptr) {}
  ~ObHbaseCfServiceGuard();
  int get_cf_service(ObHbaseColumnFamilyService *&cf_service);
private:
  common::ObIAllocator &allocator_;
  bool is_multi_cf_req_;
  ObHbaseColumnFamilyService *cf_service_;
};



} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_HBASE_COLUMN_FAMILY_SERVICE_H
