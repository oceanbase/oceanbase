/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef QWGuard
#include "drwlock.h"
QRWLock qdisc_glock;
#define QWGuard(comment) QRWLock::WLockGuard guard(qdisc_glock)
#define QRGuard(comment) QRWLock::RLockGuard guard(qdisc_glock)
#define QRGuardUnsafe(comment, idx) QRWLock::RLockGuardUnsafe guard(qdisc_glock, idx)
#endif
