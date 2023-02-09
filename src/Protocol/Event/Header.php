<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event;

use JsonSerializable;
use UserQQ\MySQL\Binlog\BinlogPosition;

class Header implements JsonSerializable
{
    public function __construct(
        public readonly BinlogPosition $position,
        public readonly int            $timestamp,
        public readonly Type           $type,
        public readonly int            $serverId,
        public readonly int            $eventSize,
        public readonly int            $nextPosition,
        public readonly int            $flags,
        public readonly int            $checksumSize,
        public readonly int            $payloadSize,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
