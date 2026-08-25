/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"

namespace oceanbase {
namespace plugin {
class ObPluginEntryHandle;
}
namespace observer {

/// Iterates `__all_virtual_plugin_info` — one row per plugin known to this
/// observer, from TWO sources:
///   1. legacy plugin framework (ObPluginMgr): built-in and legacy dynamic
///      plugins (ft parser, kms, ...);
///   2. external-table format plugins (ObExtFormatRegistry): plugins lazily
///      loaded from `ext_plugin_config`, one row per config-declared entry,
///      load failures included (status carries the outcome).
/// External-table plugin rows reuse the generic columns: name=plugin_name,
/// status=load status, type="EXTERNAL TABLE", library=the absolute path dlopen'd
/// (failed rows: the intended soname/path), library_version=api->plugin_version(),
/// description=load failure reason; columns without an ext counterpart
/// (library_revision/interface_version/author/license) are NULL. The table is a process-wide diagnostic, so it is
/// not tenant-isolated; rows are tagged with the local observer's svr_ip/port.
/// Its cluster-distributed routing makes a plain `SELECT *` fan out to every
/// observer, so the user sees the per-machine plugin set in one query.
class ObAllVirtualPluginInfo final : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualPluginInfo();
  virtual ~ObAllVirtualPluginInfo();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  virtual int inner_open() override;
  virtual int inner_close() override;
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }

private:
  // status column text for an ObExtPluginLoadStatus enum value.
  static const char *load_status_to_str(share::ObExtPluginLoadStatus status);
  // Fill one output row from a legacy plugin entry / an external-table plugin
  // status snapshot. Exactly one of the two arguments is non-null.
  int fill_legacy_row_(plugin::ObPluginEntryHandle *entry_handle);
  int fill_ext_plugin_row_(const share::ObExtPluginStatus &status);

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  ObArray<plugin::ObPluginEntryHandle *> plugin_entries_;

  // Snapshot of ext-format registry status rows, copied in inner_open()
  // (read-only: does NOT trigger any load beyond the registry's own one-shot
  // lazy load). Iterated after plugin_entries_ is exhausted.
  ObSEArray<share::ObExtPluginStatus, 16> ext_plugin_statuses_;

  int64_t iter_index_ = -1;
  int64_t ext_iter_index_ = -1;

  TO_STRING_KV(K(addr_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPluginInfo);
};

} // namespace observer
} // namespace oceanbase
