<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use ValueError;

trait FloatReadTrait
{
    public function readFloat(): float
    {
        return unpack('g', $this->read(4))[1];
    }

    public function readDouble(): float
    {
        return unpack('e', $this->read(8))[1];
    }
}
