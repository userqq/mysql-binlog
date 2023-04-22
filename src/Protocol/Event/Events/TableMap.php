<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

final class TableMap implements Event
{
    public function __construct(
        public readonly Header $header,
        public readonly int    $tableId,
        public readonly int    $flags,
        public readonly string $schema,
        public readonly string $table,
        public readonly int    $columnCount,
        public readonly array  $columns,
        public readonly string $nullBitMap,
        public readonly ?array $primaryKeyColumns,
    ) {}

    public function jsonSerialize(): mixed
    {
        $vars = get_object_vars($this);
        unset($vars['nullBitMap']);
        return $vars;
    }
}
