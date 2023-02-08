<?php

namespace UserQQ\MySQL\Binlog\Protocol;

use JsonSerializable;

enum OptionalMetadataType: int
{
    case SIGNEDNESS                   = 1;  // MIN    Data contains a bitmap indicating which integer columns are signed
    case DEFAULT_CHARSET              = 2;  // MIN    Character set of string columns, used if most columns have the same result. Columns with other character sets will follow as pair (column_index, collation number).
    case COLUMN_CHARSET               = 3;  // MIN    Character set of columns, used if columns have different character sets. Returned as a sequence of collation numbers.
    case COLUMN_NAME                  = 4;  // FULL    List of Column names, the first byte specifies the length of the column name
    case SET_STR_VALUE                = 5;  // FULL    List of set values: First byte is the number of different values, followed by length/value pairs.
    case ENUM_STR_VALUE               = 6;  // FULL    Same as SET_STR_VALUE. Since ENUM values might have up to 0xFFFF members, the number of values is a length encoded integer.
    case GEOMETRY_TYPE                = 7;  // FULL    A sequence of bytes repesenting the type of GEOMETRY columns: 0 = GEOMETRY, 1 = POINT, 2 = LINESTRING, 3 = POLYGON, 4=MULTIPOINT, 5 = MULTILINESTRING, 6 = MULTIPOLYGON, 7 = GEOMETRYCOLLECTION
    case SIMPLE_PRIMARY_KEY           = 8;  // FULL    A sequence of length encoded column indexes.
    case PRIMARY_KEY_WITH_PREFIX      = 9;  // FULL    A sequence of length encoded column indexes and prefix lengths.
    case ENUM_AND_SET_DEFAULT_CHARSET = 10; // FULL    The default character set number used for ENUM and SET columns
    case ENUM_AND_SET_COLUMN_CHARSET  = 11; // FULL    Character set of ENUM and SET columns, used if these columns have different character sets. Returned as a sequence of collation numbers.

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
        return $found = array_search($name, static::CASES, true)
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
