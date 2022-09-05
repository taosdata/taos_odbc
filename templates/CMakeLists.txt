add_custom_target(install_templates
    COMMAND sudo odbcinst -i -d -f ${CMAKE_SOURCE_DIR}/templates/odbcinst.in -l
    COMMAND cmake -E echo TAOS_ODBC_DRIVER is installed in /etc/odbcinst.ini

    COMMAND sudo odbcinst -i -s -f ${CMAKE_SOURCE_DIR}/templates/odbc.in -l
    COMMAND cmake -E echo TAOS_ODBC_DSN is installed in /etc/odbc.ini

    COMMAND odbcinst -i -s -f ${CMAKE_SOURCE_DIR}/templates/odbc.in -h
    COMMAND cmake -E echo TAOS_ODBC_DSN is installed in ~/.odbc.ini)

