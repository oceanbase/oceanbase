#!/bin/bash

export OB_CDC_BUILD_COMPONENTS=cdc_service
exec bash "$(dirname "$0")/oceanbase-cdc-build.sh" "$@"
