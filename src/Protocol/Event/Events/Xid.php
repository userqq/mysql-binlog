<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

final class Xid implements Event
{
    public function __construct(
        public readonly Header $header,
        public readonly string $xid,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
