#!/bin/bash

check_command_exists() {
  return $(command -v $1 -h >/dev/null 2>&1)
}

disk_type="UNKNOWN"
storcli_check_ssd="storcli64 /c0 show | grep 'PD LIST'  -A  10 | grep SSD > /dev/null 2>&1"
storcli_check_hdd="storcli64 /c0 show | grep 'PD LIST'  -A  10 | grep HDD > /dev/null 2>&1"

if check_command_exists "storcli64" 
then
  if eval $storcli_check_ssd
  then
    disk_type="SSD"
  elif eval $storcli_check_hdd
  then
    disk_type="HDD"
  fi
fi

echo $disk_type > ./etc/disk_type
