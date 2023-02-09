<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

class Rotate implements Event
{
    public function __construct(
        public readonly Header         $header,
        public readonly int            $position,
        public readonly string         $filename,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
