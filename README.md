# Pure php MySQL/MariaDB replica client.

### Configure the MAIN database
```
[mysqld]
server-id           = 1
log_bin             = /var/log/mysql/mysql-bin.log
expire_logs_days    = 10
max_binlog_size     = 100M                         # Size of binlog file
binlog-format       = row                          # Required option, make MAIN database store all the changes in binary format
log_slave_updates   = on                           # Required option
binlog_row_image    = full                         # Required option
binlog_row_metadata = full                         # Required option
binlog_do_db        = mydatabase                   # Database schema to replicate.
net_read_timeout    = 3600                         # Increase if you are facing with disconnects
net_write_timeout   = 3600                         # Increase if you are facing with disconnects
```

### Run REPLICA client
```USER=<dbuser> PASSWORD=<dbpassword> HOST=<dbhost> POST=<dbport> bin/run```

