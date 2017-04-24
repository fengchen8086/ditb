#!/usr/bin/expect

set timeout 60
set table [lindex $argv 0]
spawn hbase shell
expect "*:001:0"
send "flush \'${table}\'\n"
expect "*:002:0"
send "exit\n"
expect eof
