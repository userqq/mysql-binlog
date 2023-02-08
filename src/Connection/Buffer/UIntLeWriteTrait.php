<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use Countable;
use ValueError;

trait UIntLeWriteTrait
{
    /**
     * Write LE uint8_t
     */
    public function writeUInt8(int $value): static
    {
        $this->data .= \chr($value >> 0);
        $this->length += 1;

        return $this;
    }

    /**
     * Write LE uint16_t
     */
    public function writeUint16($value): static
    {
        $this->data .= \chr($value >> 0) . \chr($value >> 8);
        $this->length += 2;

        return $this;
    }

    /**
     * Write LE uint24_t
     */
    public function writeUint24($value): static
    {
        $this->data .= \chr($value >> 0) . \chr($value >> 8) . \chr($value >> 16);
        $this->length += 3;

        return $this;
    }

    /**
     * Write LE uint32_t
     */
    public function writeUInt32(int $value): static
    {
        $this->data .= \chr($value >> 0) . \chr($value >> 8) . \chr($value >> 16) . \chr($value >> 24);
        $this->length += 4;

        return $this;
    }
}
