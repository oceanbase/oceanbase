/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

package server

import (
	"fmt"
	"net/http"
)

type ApiResponse struct {
	Code       int         `json:"Code"`
	Message    string      `json:"Message"`
	Successful bool        `json:"Success"`
	Data       interface{} `json:"Data"`
	TraceId    string      `json:"Trace"`
	Server     string      `json:"Server"`
	Cost       int64       `json:"Cost"`
}

type IterableData struct {
	Contents interface{} `json:"Contents"`
}

func NewSuccessResponse(data interface{}) *ApiResponse {
	return &ApiResponse{
		Code:       http.StatusOK,
		Message:    "successful",
		Successful: true,
		Data:       data,
	}
}

func NewBadRequestResponse(err error) *ApiResponse {
	return &ApiResponse{
		Code:       http.StatusBadRequest,
		Message:    fmt.Sprintf("bad request: %v", err),
		Successful: false,
	}
}

func NewIllegalArgumentResponse(err error) *ApiResponse {
	return &ApiResponse{
		Code:       http.StatusBadRequest,
		Message:    fmt.Sprintf("illegal argument: %v", err),
		Successful: false,
	}
}

func NewNotFoundResponse(err error) *ApiResponse {
	return &ApiResponse{
		Code:       http.StatusNotFound,
		Message:    fmt.Sprintf("resource not found: %v", err),
		Successful: false,
	}
}

func NewNotImplementedResponse(err error) *ApiResponse {
	return &ApiResponse{
		Code:       http.StatusNotImplemented,
		Message:    fmt.Sprintf("request not implemented: %v", err),
		Successful: false,
	}
}

func NewErrorResponse(err error) *ApiResponse {
	return &ApiResponse{
		Code:       http.StatusInternalServerError,
		Message:    fmt.Sprintf("got internal error: %v", err),
		Successful: false,
	}
}
