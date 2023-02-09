<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol;

use JsonSerializable;

enum OptionalMetadataType: int
{
    case SIGNEDNESS                   = 1;
    case DEFAULT_CHARSET              = 2;
    case COLUMN_CHARSET               = 3;
    case COLUMN_NAME                  = 4;
    case SET_STR_VALUE                = 5;
    case ENUM_STR_VALUE               = 6;
    case GEOMETRY_TYPE                = 7;
    case SIMPLE_PRIMARY_KEY           = 8;
    case PRIMARY_KEY_WITH_PREFIX      = 9;
    case ENUM_AND_SET_DEFAULT_CHARSET = 10;
    case ENUM_AND_SET_COLUMN_CHARSET  = 11;

    private const CASES = [
        1 => 'SIGNEDNESS',
        2 => 'DEFAULT_CHARSET',
        3 => 'COLUMN_CHARSET',
        4 => 'COLUMN_NAME',
        5 => 'SET_STR_VALUE',
        6 => 'ENUM_STR_VALUE',
        7 => 'GEOMETRY_TYPE',
        8 => 'SIMPLE_PRIMARY_KEY',
        9 => 'PRIMARY_KEY_WITH_PREFIX',
        10 => 'ENUM_AND_SET_DEFAULT_CHARSET',
        11 => 'ENUM_AND_SET_COLUMN_CHARSET',
    ];

    public static function fromString(string $name): static
    {
        return static::tryFromString($name)
            ?? throw new \InvalidArgumentException(sprintf('Unknown optional metadata type: %s', $name));
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
