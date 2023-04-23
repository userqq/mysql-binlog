<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;
use UserQQ\MySQL\Binlog\Protocol\Event\RowEvent;

final class UpdateRows implements Event, RowEvent
{
    public const ACTION = 'update';

    public function __construct(
        public readonly Header   $header,
        public readonly int      $tableId,
        public readonly TableMap $tableMap,
        public readonly int      $flags,
        public readonly int      $columnCount,
        public readonly string   $columnsBitmap,
        public readonly string   $columnsBitmapAfter,
        public readonly int      $count,
        public readonly array    $rows,
    ) {}

    public function jsonSerialize(): mixed
    {
        $vars = get_object_vars($this);
        $vars['action'] = static::ACTION;
        unset($vars['columnsBitmap']);
        unset($vars['columnsBitmapAfter']);
        return $vars;
    }
}
