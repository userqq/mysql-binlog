<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;
use UserQQ\MySQL\Binlog\Protocol\Event\RowEvent;

class WriteRows implements Event, RowEvent
{
    public function __construct(
        public readonly Header    $header,
        public readonly int       $tableId,
        public readonly TableMap  $tableMap,
        public readonly int       $flags,
        public readonly int       $columnCount,
        public readonly string    $columnsBitmap,
        public readonly int       $count,
        public readonly array     $rows,
    ) {}

    public function jsonSerialize(): mixed
    {
        $vars = get_object_vars($this);
        unset($vars['columnsBitmap']);
        return $vars;
    }
}
