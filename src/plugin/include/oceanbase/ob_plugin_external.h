/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
