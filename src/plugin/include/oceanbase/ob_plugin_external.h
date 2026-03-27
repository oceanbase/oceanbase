/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

/**
 * @defgroup ObPluginExternal External Table Plugin Interface
 * @{
 */

/**
 * @brief external table interface version
 * @note this is the minimum version.
 * @note You should add new version if you add new routines.
 */
#define OBP_EXTERNAL_INTERFACE_VERSION OBP_PLUGIN_API_VERSION_0_2_0

/**
 * @brief current external table interface version
 * @note you should change this value if you add some new routines in interface struct.
 */
#define OBP_EXTERNAL_INTERFACE_VERSION_CURRENT OBP_EXTERNAL_INTERFACE_VERSION

/*
 * C interface is not ready
 */

/** @} */
