/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef LOGSERVICE_COORDINATOR_COMMON_DEFINE_H
#define LOGSERVICE_COORDINATOR_COMMON_DEFINE_H
#define COORDINATOR_LOG_(args...) COORDINATOR_LOG(args, PRINT_WRAPPER)
#define LC_TIME_GUARD(threshold) TIMEGUARD_INIT(COORDINATOR, threshold, 60_s)
#endif