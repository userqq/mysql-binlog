<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection;

use UnexpectedValueException;

final class Buffer
{
    use Buffer\DateTimeReadTrait;
    use Buffer\FloatReadTrait;
    use Buffer\DecimalReadTrait;
    use Buffer\IntBeReadTrait;
    use Buffer\IntLeReadTrait;
    use Buffer\UIntLeReadTrait;
    use Buffer\UIntLeWriteTrait;

    public const UNSIGNED_CHAR_COLUMN  = 251;
    public const UNSIGNED_SHORT_COLUMN = 252;
    public const UNSIGNED_INT24_COLUMN = 253;
    public const UNSIGNED_INT64_COLUMN = 254;

    private int $offset = 0;
    private int $length;

    public function __construct(
        private string $data = '',
                int $length = null,
    ) {
        $this->length = $length ??= \strlen($data);
    }

    public function readCodedBinary(): ?int
    {
        $size = \ord($this->data[$this->offset]);
        ++$this->offset;

        if ($size === self::UNSIGNED_CHAR_COLUMN) {
            return null;
        }

        if ($size < self::UNSIGNED_CHAR_COLUMN) {
            return $size;
        }

        if ($size === self::UNSIGNED_SHORT_COLUMN) {
            return $this->readUInt16();
        }

        if ($size === self::UNSIGNED_INT24_COLUMN) {
            return $this->readUInt24();
        }

        if ($size === self::UNSIGNED_INT64_COLUMN) {
            return $this->readUInt64();
        }

        throw new UnexpectedValueException(\sprintf('Not expected %dbit', $size));
    }

    public function readUIntBySize(int $size): int
    {
        switch ($size) {
            case 1:
                return $this->readUInt8();
                break;
            case 2:
                return $this->readUInt16();
                break;
            case 3:
                return $this->readUInt24();
                break;
            case 4:
                return $this->readUInt32();
                break;
            case 5:
                return $this->readUInt40();
                break;
            case 6:
                return $this->readUInt48();
                break;
            case 7:
                return $this->readUInt56();
                break;
            default:
                throw new UnexpectedValueException(\sprintf('Not expected %dbit', $size));
        }
    }

    public function readIntBeBySize(int $size): int
    {
        switch ($size) {
            case 1:
                return $this->readUInt8();
                break;
            case 2:
                return $this->readInt16Be();
                break;
            case 3:
                return $this->readInt24Be();
                break;
            case 4:
                return $this->readInt32Be();
                break;
            case 5:
                return $this->readInt40Be();
                break;
            default:
                throw new UnexpectedValueException(\sprintf('Not expected %dbit', $size));
        }
    }

    public function read(?int $length = null): string
    {
        if (0 === $length) {
            return '';
        }

        if (null === $length) {
            $return = \substr($this->data, $this->offset);
            $this->offset = $this->length;
        } else {
            $return = \substr($this->data, $this->offset, $length);
            $this->offset += $length;
        }

        return $return;
    }

    public function readUntill(string $needle): string
    {
        $position = strpos($this->data, $needle, $this->offset);
        $data = \substr($this->data, $this->offset, $position ? ($position - 1) : null);

        $this->offset = $position ? ($position + 1) : $this->length;

        return $data;
    }

    public function readVariableLengthString(): string
    {
        return $this->read($this->readCodedBinary());
    }

    public function readLengthString(int $size): string
    {
        return $this->read($this->readUIntBySize($size));
    }

    public function readBit(int $bytes, int $bits): string
    {
        $res = '';
        for ($byte = 0; $byte < $bytes; ++$byte) {
            $current_byte = '';
            $data = $this->readUInt8();
            if (0 === $byte) {
                if (1 === $bytes) {
                    $end = $bits;
                } else {
                    $end = $bits % 8;
                    if (0 === $end) {
                        $end = 8;
                    }
                }
            } else {
                $end = 8;
            }

            for ($bit = 0; $bit < $end; ++$bit) {
                if ($data & (1 << $bit)) {
                    $current_byte .= '1';
                } else {
                    $current_byte .= '0';
                }
            }

            $res .= \strrev($current_byte);
        }

        return $res;
    }

    public function slice(int $length): static
    {
        return new static($this->read($length), $length);
    }

    public function skip(int $length): static
    {
        $this->offset += $length;

        return $this;
    }

    public function write(string $value, int $repeat = 1): static
    {
        $this->data .= str_repeat($value, $repeat);
        $this->length += \strlen($value) * $repeat;

        return $this;
    }

    public function append(Buffer $buffer): static
    {
        $this->data .= $buffer->data;
        $this->length = \strlen($this->data);

        return $this;
    }

    public function rewind(int $stepBack = 0): static
    {
        $this->offset = (0 === $stepBack) ? 0 : ($this->offset - $stepBack);

        return $this;
    }

    public function getLength(): int
    {
        return $this->length;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }

    public function getLeft(): int
    {
        return $this->length - $this->offset;
    }

    public function __toString(): string
    {
        return $this->data;
    }
}
