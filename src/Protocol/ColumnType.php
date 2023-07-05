<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol;

use JsonSerializable;

enum ColumnType: int implements JsonSerializable
{
    case DECIMAL     = 0x00;
    case TINY        = 0x01;
    case SHORT       = 0x02;
    case LONG        = 0x03;
    case FLOAT       = 0x04;
    case DOUBLE      = 0x05;
    case NULL        = 0x06;
    case TIMESTAMP   = 0x07;
    case LONGLONG    = 0x08;
    case INT24       = 0x09;
    case DATE        = 0x0a;
    case TIME        = 0x0b;
    case DATETIME    = 0x0c;
    case YEAR        = 0x0d;
    case NEWDATE     = 0x0e;
    case VARCHAR     = 0x0f;
    case BIT         = 0x10;
    case TIMESTAMP2  = 0x11;
    case DATETIME2   = 0x12;
    case TIME2       = 0x13;
    case JSON        = 0xf5;
    case NEWDECIMAL  = 0xf6;
    case ENUM        = 0xf7;
    case SET         = 0xf8;
    case TINY_BLOB   = 0xf9;
    case MEDIUM_BLOB = 0xfa;
    case LONG_BLOB   = 0xfb;
    case BLOB        = 0xfc;
    case VAR_STRING  = 0xfd;
    case STRING      = 0xfe;
    case GEOMETRY    = 0xff;

    private const CASES = [
        0x00 => 'DECIMAL',
        0x01 => 'TINY',
        0x02 => 'SHORT',
        0x03 => 'LONG',
        0x04 => 'FLOAT',
        0x05 => 'DOUBLE',
        0x06 => 'NULL',
        0x07 => 'TIMESTAMP',
        0x08 => 'LONGLONG',
        0x09 => 'INT24',
        0x0a => 'DATE',
        0x0b => 'TIME',
        0x0c => 'DATETIME',
        0x0d => 'YEAR',
        0x0e => 'NEWDATE',
        0x0f => 'VARCHAR',
        0x10 => 'BIT',
        0x11 => 'TIMESTAMP2',
        0x12 => 'DATETIME2',
        0x13 => 'TIME2',
        0xf5 => 'JSON',
        0xf6 => 'NEWDECIMAL',
        0xf7 => 'ENUM',
        0xf8 => 'SET',
        0xf9 => 'TINY_BLOB',
        0xfa => 'MEDIUM_BLOB',
        0xfb => 'LONG_BLOB',
        0xfc => 'BLOB',
        0xfd => 'VAR_STRING',
        0xfe => 'STRING',
        0xff => 'GEOMETRY',
    ];

    public static function fromString(string $name): static
    {
        return static::tryFromString($name)
            ?? throw new \InvalidArgumentException(\sprintf('Unknown column type: %s', $name));
    }

    public static function tryFromString(string $name): ?static
    {
        return (false !== $found = array_search($name, static::CASES, true))
            ? static::from($found)
            : null;
    }

    public function toString(): string
    {
        return self::CASES[$this->value];
    }

    public function jsonSerialize(): mixed
    {
        return $this->toString();
    }
}
