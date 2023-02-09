<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap\Column;

use UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap\Meta\Meta;

class IntegerColumn implements Column
{
    public function __construct(
        public readonly int    $index,
        public readonly Meta   $meta,
        public readonly string $name,
        public readonly bool   $isSigned,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
