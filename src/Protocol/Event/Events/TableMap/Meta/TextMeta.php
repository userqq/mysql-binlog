<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap\Meta;

use UserQQ\MySQL\Binlog\Protocol\ColumnType;

final class TextMeta implements Meta
{
    public function __construct(
        public readonly ColumnType $type,
        public readonly int        $maxLength,
    ) {}

    public function jsonSerialize(): mixed
    {
        return get_object_vars($this);
    }
}
