###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

import pyodbc
import sys

if False:
  cnxn = pyodbc.connect('DSN=SQLSERVER_ODBC_DSN')
  cursor = cnxn.cursor()
  x = cursor.execute("select name,mark from x").fetchall()
  print(x, file=sys.stderr)

if False:
  cnxn = pyodbc.connect('DSN=TAOS_ODBC_DSN')
  cursor = cnxn.cursor()
  x = cursor.execute("select name,mark from bar.x").fetchall()
  print(x, file=sys.stderr)

# Specifying the ODBC driver, server name, database, etc. directly
cnxn = pyodbc.connect('DRIVER={TAOS_ODBC_DRIVER};SERVER=localhost;DATABASE=information_schema;UID=root;PWD=taosdata')

# Using a DSN, but providing a password as well
cnxn = pyodbc.connect('DSN=TAOS_ODBC_DSN;PWD=taosdata')

# Create a cursor from the connection
cursor = cnxn.cursor()

cursor.execute("drop database if exists bar")
cursor.execute("create database if not exists bar")
cursor.execute("use bar")

cursor.execute("select * from information_schema.ins_tables")
while True:
  row = cursor.fetchone()
  if not row:
    break
  print(row, file=sys.stderr)

cursor.execute("drop table if exists x")
cursor.execute("create table x (ts timestamp, name varchar(20), mark nchar(20))")
cursor.execute("insert into x(ts, name, mark) values (now(), '测试', '试验')")
# cnxn.commit()
cursor.execute("insert into x(ts, name, mark) values (?, ?, ?)", 1682565350033, 'xes', 'no')
# cnxn.commit()
x = cursor.execute("select name,mark from x").fetchall()
print(x, file=sys.stderr)
y = [('xes', 'no'), ('测试', '试验')]
assert str(x) == str(y), "{0} != {1}".format(x, y)

cursor.execute("drop stable if exists st")
cursor.execute("create stable st (ts timestamp, age int) tags (name varchar(20))")
cursor.execute("insert into 'suzhou' using st tags ('jiangsu') values (1665226861289, 100)")
x = cursor.execute("select name,age from st").fetchall()
print(x, file=sys.stderr)
y = [('jiangsu', 100)]
assert str(x) == str(y), "{0} != {1}".format(x, y)

cursor.execute("drop stable if exists st")
cursor.execute("create stable st (ts timestamp, age int) tags (name varchar(20))")
cursor.execute("insert into ? using st tags (?) values (?, ?)", 'suzhou', 'jiangsu', 1665226861289, 100)
x = cursor.execute("select age,name from st").fetchall()
print(x, file=sys.stderr)
y = [(100, 'jiangsu')]
assert str(x) == str(y), "{0} != {1}".format(x, y)

cursor.execute("drop stable if exists st")
cursor.execute("create stable st (ts timestamp, age int) tags (name varchar(20))")
params = [ ('suzhou', 'jiangsu', 1665226861289, 101), ('shanghai', 'Shanghai', 1665226861299, 202) ]
cursor.fast_executemany = False
cursor.executemany("insert into ? using st tags (?) values (?, ?)", params)

x = cursor.execute("select name,age from st").fetchall()
print(x, file=sys.stderr)
y = [('jiangsu', 101), ('Shanghai', 202)]
assert str(x) == str(y), "{0} != {1}".format(x, y)

cursor.execute("drop table if exists x")
cursor.execute("create table x (ts timestamp, name varchar(20), mark nchar(20))")
cursor.execute("insert into x(ts, name, mark) values (now(), '测试', '试验')")
# cnxn.commit()
cursor.execute("insert into x(ts, name, mark) values (?, ?, ?)", 1682565350033, '试验', '测试')
# cnxn.commit()
x = cursor.execute("select name,mark from x").fetchall()
print(x, file=sys.stderr)
y = [('试验', '测试'), ('测试', '试验')]
assert str(x) == str(y), "{0} != {1}".format(x, y)

print("success")
