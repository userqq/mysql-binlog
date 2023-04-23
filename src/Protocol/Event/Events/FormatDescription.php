<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event\Events;

use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

final class FormatDescription implements Event
{
    public function __construct(
        public readonly Header $header,
        public readonly int    $formatVersion,
        public readonly string $serverVersion,
        public readonly int    $createTimestamp,
        public readonly int    $eventHeaderLength,
        public readonly string $postHeaderEventLengths,
        public readonly int    $checksumAlgorithmType,
    ) {}

    public function jsonSerialize(): mixed
    {
        $vars = get_object_vars($this);
        unset($vars['postHeaderEventLengths']);
        return $vars;
    }
}
