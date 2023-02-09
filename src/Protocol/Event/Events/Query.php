<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

class Query implements Event
{
    public function __construct(
        public readonly Header $header,
        public readonly int    $slaveProxyId,
        public readonly int    $executionTime,
        public readonly int    $schemaLength,
        public readonly int    $errorCode,
        public readonly int    $statusVarsLength,
        public readonly string $schema,
        public readonly string $query,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
