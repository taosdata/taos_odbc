#!/bin/bash
set -e

csudo=""
script_dir="$(cd "$(dirname "$0")" && pwd)"
destination_dir="/usr/local/lib"
taos_odbc_lib_file="libtaos_odbc.so.0.1"
taos_odbc_lib_ln="libtaos_odbc.so"

if command -v sudo >/dev/null; then
  csudo="sudo "
fi

copy_file(){
    source_file="${script_dir}/taos_odbc/lib/${taos_odbc_lib_file}"
    #echo "File copied from ${source_file} to ${destination_dir}"
    ${csudo}cp "${source_file}" "${destination_dir}"
    ${csudo}ln -sf "${destination_dir}/${taos_odbc_lib_file}" "${destination_dir}/${taos_odbc_lib_ln}"
    echo "libtaos_odbc has copy to ${destination_dir}"
}

config_taos_odbc_driver(){
    odbcinst -u -s -h -n TAOS_ODBC_DSN > /dev/null
    sudo odbcinst -u -s -l -n TAOS_ODBC_DSN > /dev/null
    sudo odbcinst -u -d -l -n TAOS_ODBC_DRIVER > /dev/null
    sudo odbcinst -i -d -f ${script_dir}/taos_odbc/odbcinst.in -l

    sudo odbcinst -i -s -f ${script_dir}/taos_odbc/odbc.in -l
    odbcinst -i -s -f ${script_dir}/taos_odbc/odbc.in -h

    echo "Add taos_odbc data source into odbc.ini"
}

install(){
    echo "taos-odbc driver will install on your computer"
    copy_file
    config_taos_odbc_driver

    echo -e "\nThe default taos data source has been added for you. You can reconfigure it by editing the odbc.ini file."
    echo "The path of the odbc.ini file may be different on different systems, you can view it through the \"odbcinst -j\" command."
    echo -e "\n\033[32mtaos-odbc is installed successfully!\033[0m"
}

install
