<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use Countable;
use ValueError;

trait IntBeReadTrait
{
    /**
     * Read BE uint16_t
     */
    public function readInt16Be(): int
    {
        $value = (\ord($this->data[$this->offset]) << 8)
            | \ord($this->data[++$this->offset]);

        $value = \ord($this->data[$this->offset - 1]) & 0x80 ? ($value - 0x10000) : $value;
        ++$this->offset;

        return $value;
    }

    /**
     * Read BE uint24_t
     */
    public function readInt24Be(): int
    {
        $value = (\ord($this->data[$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 8)
            | \ord($this->data[++$this->offset]);

        $value = \ord($this->data[$this->offset - 2]) & 0x80 ? ($value - 0x1000000) : $value;
        ++$this->offset;

        return $value;
    }

    /**
     * Read BE uint32_t
     */
    public function readInt32Be(): int
    {
        $value = (\ord($this->data[$this->offset]) << 24)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 8)
            | \ord($this->data[++$this->offset]);

        ++$this->offset;

        return \ord($this->data[$this->offset - 4]) & 0x80 ? ($value - 0x100000000) : $value;
    }

    /**
     * Read BE uint40_t
     */
    public function readInt40Be(): int
    {
        $value = (\ord($this->data[$this->offset]) << 32)
            | (\ord($this->data[++$this->offset]) << 24)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 8)
            | \ord($this->data[++$this->offset]);

        $value = (\ord($this->data[$this->offset - 4]) & 0x80) ? ($value - 0x10000000000) : $value;
        ++$this->offset;

        return $value;
    }
}
