<?php

namespace UserQQ\MySQL\Binlog;

use JsonSerializable;

class BinlogPosition implements JsonSerializable
{
    public function __construct(
        public readonly string $filename,
        public readonly int    $position,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
