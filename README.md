# Pure php MySQL/MariaDB replica client.

This package enables you to react to changes in your database without creating additional load on it, such as triggers, and performs faster than similar solutions. It impersonates a replica and uses binlog from MySQL/MariaDB to track changes in the database. When data changes, a database writes an event to binlog, which could be converted to JSON. This JSON can be sent to any destination, such as Apache Kafka, Kinesis, RabbitMQ or directly to other databases such as MongoDB, ClickHouse, Elastic to ensure fast and efficient data processing. With this package, you can have a copy of your database with a delay of minutes or even seconds, no matter how large your database is.

It can be useful for companies that want to keep a copy of their data in other systems to ensure more reliable storage or to process data with other tools. also it be used to send data to message queues, making it easier to process and analyze data in real-time. Therefore, companies can take advantage of the benefits of message queueing systems, such as load balancing, scalability, and fault-tolerance, while still maintaining a reliable copy of their data in other systems.

## Installation
This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require userqq/mysql-binlog 
```

## Сonfiguration
### Database configuration
In order to parse data from the binlog without additional actions and without excessive requests to the information_schema, the database must be properly configured. This allows for the re-reading of the data types and column names without additional requests, and thus the data stored in the binlog can be re-read even without the database, while still providing all the details about the data.

This library requires the `binlog_row_metadata` option, which is available in **MySQL >= 8.0.1** or in **MariaDB >= 10.5.0**. The `binlog_row_metadata` option must be set to `FULL` to ensure that all necessary metadata is included in the binlog for proper parsing.

Just add the following config from example below to your database configuration, then restart the database and prune old binary logs:
```mysql
FLUSH BINARY LOGS; 
PURGE BINARY LOGS BEFORE NOW();
```
This will flush the current binary log to disk and then remove all binary logs before the current time.

Usually you can find config in `/etc/mysql` directory.
```
[mysqld]
server-id           = 1
log_bin             = /var/log/mysql/mysql-bin.log
expire_logs_days    = 10
max_binlog_size     = 100M
binlog-format       = row
log_slave_updates   = on
binlog_row_image    = full
binlog_row_metadata = full
binlog_do_db        = mydatabase
net_read_timeout    = 3600
net_write_timeout   = 3600
```

### Binlog reader configuration
To simplify configuration library exposes `UserQQ\MySQL\Binlog\Config` class:
```php
use UserQQ\MySQL\Binlog\Config;

$config = (new Config())
    ->withUser('root')
    ->withPassword('toor');
```

Here is a list of available configuration options:
```php
withUser(string $user): static                   // Database user with replication privileges
```

```php
withPassword(string $password): static           // Database user's password
```

```php
withHost(string $host): static                   // Database host
```

```php
withPort(int $port): static                      // Database host
```

```php
withBinlogFile(?string $binlogFile): static      // Binlog file to start from
```

```php
withBinlogPosition(?int $binlogPosition): static // Position in binlog file to start from
```

Please refer to [`Config.php`](https://github.com/userqq/mysql-binlog/blob/main/src/Config.php) for more details

## Run the application
Then just create an instance of `UserQQ\MySQL\Binlog\EventsIterator` and iterate over it.
```php
<?php declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use UserQQ\MySQL\Binlog\Config;
use UserQQ\MySQL\Binlog\EventsIterator;

$config = (new Config())
    ->withUser('root')
    ->withPassword('toor');

$eventsIterator = new EventsIterator($config);

foreach ($eventsIterator as $position => $event) {
    echo json_encode($position) . PHP_EOL;
    echo json_encode($event, JSON_PRETTY_PRINT) . PHP_EOL;
    echo PHP_EOL;
}
```
Run inserts, updates, and deletes on your database and see how it reacts!


