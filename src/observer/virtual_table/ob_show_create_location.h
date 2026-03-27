/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_OBSERVER_OB_SHOW_CREATE_LOCATION_
 #define OCEANBASE_OBSERVER_OB_SHOW_CREATE_LOCATION_
 #include "lib/container/ob_se_array.h"
 #include "share/ob_virtual_table_scanner_iterator.h"
 #include "common/ob_range.h"

 namespace oceanbase
 {
 namespace common
 {
 class ObString;
 }
 namespace observer
 {
 class ObShowCreateLocation : public common::ObVirtualTableScannerIterator
 {
 public:
   ObShowCreateLocation();
   virtual ~ObShowCreateLocation();
   virtual int inner_get_next_row(common::ObNewRow *&row);
   virtual void reset();
 private:
   int calc_show_location_id(uint64_t &show_location_id);
   int fill_row_cells(uint64_t show_location_id, const common::ObString &location_name);
 private:
   DISALLOW_COPY_AND_ASSIGN(ObShowCreateLocation);
 };
 }
 }
 #endif /* OCEANBASE_OBSERVER_OB_SHOW_CREATE_LOCATION_ */
