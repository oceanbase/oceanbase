/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_lake_format_deducer.h
/// \brief Centralized lake-table format detection for external catalogs.
///
/// Catalog-specific clues (filesystem directory names, HMS outputFormat, Iceberg
/// metadata_location, ...) are normalized here. Plugin-backed formats are probed
/// solely via ObExtFormatRegistry::recognize_format; built-in fallbacks remain
/// only for native formats not yet plugin-backed (e.g. Iceberg).

#ifndef OB_LAKE_FORMAT_DEDUCER_H
#define OB_LAKE_FORMAT_DEDUCER_H

#include "share/catalog/ob_catalog_properties.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"

// Forward-declare the host API at global scope inside `extern "C"` to match its
// real definition in ob_external_table_plugin.h (which lives in a global
// `extern "C"` block). A plain `struct ObExtTableHostApi;` inside
// namespace oceanbase::share would declare a *different* C++-linkage type that
// shadows the global one in unqualified lookup — clang then mangles it with the
// namespace, producing a link-time symbol mismatch. Declaring it here means
// unqualified `ObExtTableHostApi` inside `oceanbase::share` falls through to
// this global C-linkage declaration.
extern "C" struct ObExtTableHostApi;

namespace oceanbase
{
namespace share
{

/// HMS table fields needed for format deduction (keeps thrift out of this header).
struct ObHmsTableDeduceInput
{
  common::ObString sd_location;
  common::ObString output_format;
  common::ObString iceberg_metadata_location;
};

class ObLakeFormatDeducer
{
public:
  /// Filesystem catalog: probe child directory names under `table_uri`.
  /// On plugin recognition, table_format is set to the enum value encoding the
  /// recognizing plugin's slot — the enum value carries the identity.
  static int deduce_from_filesystem(common::ObIAllocator &allocator,
                                    const common::ObString &table_uri,
                                    const common::ObIArray<common::ObString> &table_dirs,
                                    const ObExtTableHostApi *host,
                                    ObLakeTableFormat &table_format);

  /// HMS catalog: probe metastore parameters / sd.outputFormat.
  /// On plugin recognition, table_format is set to the enum value encoding the
  /// recognizing plugin's slot — the enum value carries the identity.
  static int deduce_from_hms(common::ObIAllocator &allocator,
                             const ObHmsTableDeduceInput &input,
                             ObLakeTableFormat &table_format,
                             common::ObString &table_location);
};

} // namespace share
} // namespace oceanbase

#endif // OB_LAKE_FORMAT_DEDUCER_H
