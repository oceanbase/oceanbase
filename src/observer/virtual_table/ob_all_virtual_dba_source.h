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

#ifndef OB_ALL_VIRTUAL_DBA_SOURCE_H_
#define OB_ALL_VIRTUAL_DBA_SOURCE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualDbaSource : public common::ObVirtualTableScannerIterator
{

public:
  ObAllVirtualDbaSource();
  virtual ~ObAllVirtualDbaSource();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

protected:

  enum TABLE_COLUMN
  {
    OWNER = common::OB_APP_MIN_COLUMN_ID,
    NAME,
    TYPE,
    LINE,
    TEXT,
    ORIGIN_CON_ID,
    OBJECT_ID,
    DATABASE_ID
  };

  enum SourceObjectType
  {
    SOURCE_PACKAGE = 0,
    SOURCE_ROUTINE,
    SOURCE_TRIGGER,
    SOURCE_TYPE,
    MAX_SOURCE_TYPE
  };

  struct SourceIterState
  {
    SourceObjectType current_type_;
    int64_t object_idx_;
    int64_t line_idx_;
    int64_t udt_object_type_idx_;
    common::ObArray<common::ObString> lines_;

    SourceIterState() : current_type_(SOURCE_PACKAGE), object_idx_(0), line_idx_(0), udt_object_type_idx_(0) {}
    void reset() {
      current_type_ = SOURCE_PACKAGE;
      object_idx_ = 0;
      line_idx_ = 0;
      udt_object_type_idx_ = 0;
      lines_.reset();
    }
  };

  virtual int get_next_source_line(common::ObNewRow *&row);
  int split_text_into_lines(const common::ObString &text, common::ObArray<common::ObString> &lines);
  int fill_row_from_package(const share::schema::ObPackageInfo *package_info,
                           const common::ObString &line_text,
                           int64_t line_num);
  int fill_row_from_routine(const share::schema::ObRoutineInfo *routine_info,
                           const common::ObString &line_text,
                           int64_t line_num);
  int fill_row_from_trigger(const share::schema::ObTriggerInfo *trigger_info,
                           const common::ObString &line_text,
                           int64_t line_num);
  int fill_row_from_udt(const share::schema::ObUDTTypeInfo *udt_info,
                       const share::schema::ObUDTObjectType *object_type_info,
                       const common::ObString &line_text,
                       int64_t line_num);

  int get_package_arrays();
  int get_routine_arrays();
  int get_trigger_arrays();
  int get_udt_arrays();

  int get_system_package_arrays();
  int get_system_routine_arrays();
  int get_system_trigger_arrays();
  int get_system_udt_arrays();

  bool move_to_next_object();
  bool move_to_next_object_type();
  bool is_current_object_array_exhausted();

  uint64_t tenant_id_;
  SourceIterState iter_state_;

  common::ObArray<const share::schema::ObPackageInfo *> package_array_;
  common::ObArray<const share::schema::ObRoutineInfo *> routine_array_;
  common::ObArray<const share::schema::ObTriggerInfo *> trigger_array_;
  common::ObArray<const share::schema::ObUDTTypeInfo *> udt_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDbaSource);
};
}  // namespace observer
}  // namespace oceanbase
#endif
