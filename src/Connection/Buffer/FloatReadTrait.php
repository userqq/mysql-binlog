<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use Countable;
use ValueError;

trait FloatReadTrait
{
    private const FLOAT_LENGTH = 4;
    private const DOUBLE_LENGTH = 8;

    public function readFloat(): float
    {
        return unpack('g', $this->read(static::FLOAT_LENGTH))[1];
    }

    public function readDouble(): float
    {
        return unpack('e', $this->read(static::DOUBLE_LENGTH))[1];
    }
}
