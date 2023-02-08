<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use Countable;
use ValueError;

trait IntLeReadTrait
{
    /**
     * Read LE int8_t
     */
    public function readInt8(): int
    {
        $value = \ord($this->data[$this->offset]);
        ++$this->offset;

        return $value & 0x80 ? ($value - 0x100) : $value;
    }

    /**
     * Read LE int16_t
     */
    public function readInt16(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8);

        $value = \ord($this->data[$this->offset]) & 0x80 ? ($value - 0x10000) : $value;
        ++$this->offset;

        return $value;
    }

    /**
     * Read LE int24_t
     */
    public function readInt24(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16);

        $value = \ord($this->data[$this->offset]) & 0x80 ? ($value - 0x1000000) : $value;
        ++$this->offset;

        return $value;
    }

    /**
     * Read LE int32_t
     */
    public function readInt32(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 24);

        $value = \ord($this->data[$this->offset]) & 0x80 ? ($value - 0x100000000) : $value;
        ++$this->offset;

        return $value;
    }

    /**
     * Read LE int64_t
     */
    public function readInt64(): string
    {
        $value = gmp_import($str = substr($this->data, $this->offset, 8), 1, GMP_LSW_FIRST | GMP_LITTLE_ENDIAN);
        $value = \ord($this->data[$this->offset + 7]) & 0x80
            ? $value - gmp_init('0x10000000000000000')
            : $value;
        $this->offset += 8;

        return gmp_strval($value);
    }
}
