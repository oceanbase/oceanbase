/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TOOLS_OB_SANDBOX_OB_SANDBOX_SETUP_H_
#define OCEANBASE_TOOLS_OB_SANDBOX_OB_SANDBOX_SETUP_H_

#include <linux/filter.h>

// Relative to OB home directory (observer's working directory),
// consistent with other OB path conventions
#define OB_SANDBOX_ROOT_PATH "run/ob_sandbox/"

int prepare_sandbox_root();
int build_blacklist_bpf_program(struct sock_fprog *out_prog);

// Best-effort: discover and copy runtime shared library dependencies of
// `binary_path` into the sandbox root, using `ld.so --list`. Failures are
// logged at ERROR level but never propagated back to the caller, so that
// a missing or unparseable binary does not break sandbox request handling.
// Intended to be called on first encounter with each distinct binary_path.
int copy_binary_dependencies(const char* binary_path);

#endif // OCEANBASE_TOOLS_OB_SANDBOX_OB_SANDBOX_SETUP_H_
