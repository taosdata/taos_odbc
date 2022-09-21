add_custom_target(install_templates
    COMMAND odbcinst -u -s -h -n TAOS_ODBC_DSN
    COMMAND cmake -E echo TAOS_ODBC_DSN is uninstalled from ~/.odbc.ini

    COMMAND sudo odbcinst -u -s -l -n TAOS_ODBC_DSN
    COMMAND cmake -E echo TAOS_ODBC_DSN is uninstalled from /etc/odbc.ini

    COMMAND sudo odbcinst -u -d -l -n TAOS_ODBC_DRIVER
    COMMAND cmake -E echo TAOS_ODBC_DRIVER is uninstalled from /etc/odbcinst.ini

    COMMAND sudo odbcinst -i -d -f ${CMAKE_SOURCE_DIR}/templates/odbcinst.in -l
    COMMAND cmake -E echo TAOS_ODBC_DRIVER is installed in /etc/odbcinst.ini

    COMMAND sudo odbcinst -i -s -f ${CMAKE_SOURCE_DIR}/templates/odbc.in -l
    COMMAND cmake -E echo TAOS_ODBC_DSN is installed in /etc/odbc.ini

    COMMAND odbcinst -i -s -f ${CMAKE_SOURCE_DIR}/templates/odbc.in -h
    COMMAND cmake -E echo TAOS_ODBC_DSN is installed in ~/.odbc.ini)

