<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap\Meta;

use UserQQ\MySQL\Binlog\Protocol\ColumnType;

final class BitMeta implements Meta
{
    public function __construct(
        public readonly ColumnType $type,
        public readonly int        $bytes,
        public readonly int        $bits,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
