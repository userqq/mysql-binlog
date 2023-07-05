<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use ValueError;

trait UIntLeReadTrait
{
    /**
     * Read LE uint8_t
     */
    public function readUInt8(): int
    {
        $value = \ord($this->data[$this->offset]);
        ++$this->offset;

        return $value;
    }

    /**
     * Read LE uint16_t
     */
    public function readUInt16(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8);

        ++$this->offset;

        return $value;
    }

    /**
     * Read LE uint24_t
     */
    public function readUInt24(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16);

        ++$this->offset;

        return $value;
    }

    /**
     * Read LE uint32_t
     */
    public function readUInt32(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 24);

        ++$this->offset;

        return $value;
    }

    /**
     * Read LE uint40_t
     */
    public function readUInt40(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 24)
            | (\ord($this->data[++$this->offset]) << 32);

        ++$this->offset;

        return $value;
    }

    /**
     * Read LE uint48_t
     */
    public function readUInt48(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 24)
            | (\ord($this->data[++$this->offset]) << 32)
            | (\ord($this->data[++$this->offset]) << 40);

        ++$this->offset;

        return $value;
    }

    /**
     * Read LE uint56_t
     */
    public function readUInt56(): int
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 24)
            | (\ord($this->data[++$this->offset]) << 32)
            | (\ord($this->data[++$this->offset]) << 40)
            | (\ord($this->data[++$this->offset]) << 48);

        ++$this->offset;

        return $value;
    }


    /**
     * Read LE uint64_t
     */
    public function readUInt64(): int|string
    {
        $value = \ord($this->data[$this->offset])
            | (\ord($this->data[++$this->offset]) << 8)
            | (\ord($this->data[++$this->offset]) << 16)
            | (\ord($this->data[++$this->offset]) << 24)
            | (\ord($this->data[++$this->offset]) << 32)
            | (\ord($this->data[++$this->offset]) << 40)
            | (\ord($this->data[++$this->offset]) << 48)
            | (\ord($this->data[++$this->offset]) << 56);

        ++$this->offset;

        // if value greater than signed int64, than we should handle this with GMP and return string
        if ($value < 0) {
            $this->offset -= 8;
            /** @psalm-suppress UndefinedConstant */
            $value = gmp_strval(gmp_import(\substr($this->data, $this->offset, 8), 1, GMP_LSW_FIRST | GMP_LITTLE_ENDIAN));
            $this->offset += 8;
        }

        return $value;
    }
}
