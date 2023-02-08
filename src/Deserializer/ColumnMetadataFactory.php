<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Deserializer;

use UserQQ\MySQL\Binlog\Connection\Buffer;
use UserQQ\MySQL\Binlog\Protocol\Collation;
use UserQQ\MySQL\Binlog\Protocol\ColumnType;
use UserQQ\MySQL\Binlog\Protocol\OptionalMetadataType;
use UserQQ\MySQL\Binlog\Protocol\Event\Events;
use UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap\Column;
use UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap\Meta;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

class ColumnMetadataFactory
{
    public function readColumns(Buffer $buffer, int $columnCount): array
    {
        $typesData = $buffer->read($columnCount);
        $buffer->readCodedBinary();

        $columns = [];
        for ($i = 0; $i < $columnCount; ++$i) {
            switch ($type = ColumnType::from(\ord($typesData[$i]))) {
                case ColumnType::DOUBLE:
                case ColumnType::FLOAT:
                    $columns[$i] = new Meta\SizedMeta($type, $buffer->readUInt8());
                    break;

                case ColumnType::TIMESTAMP2:
                case ColumnType::DATETIME2:
                case ColumnType::TIME2:
                    $columns[$i] = new Meta\TimeMeta($type, $buffer->readUInt8());
                    break;

                case ColumnType::VARCHAR:
                    $columns[$i] = new Meta\TextMeta($type, $buffer->readUInt16());
                    break;

                case ColumnType::VAR_STRING:
                case ColumnType::STRING:
                    $metadata = ($buffer->readUInt8() << 8) + $buffer->readUInt8();
                    $realType = $metadata >> 8;
                    if ($realType === ColumnType::SET->value || $realType === ColumnType::ENUM->value) {
                        $type = ColumnType::from($columnType = $realType);
                        $columns[$i] = new Meta\SizedMeta($type, $metadata & 0x00ff);
                    } else {
                        $columns[$i] = new Meta\TextMeta($type, ((($metadata >> 4) & 0x300) ^ 0x300) + ($metadata & 0x00ff));
                    }
                    break;

                case ColumnType::BLOB:
                case ColumnType::GEOMETRY:
                case ColumnType::JSON:
                    $columns[$i] = new Meta\BlobMeta($type, $buffer->readUInt8());
                    break;

                case ColumnType::NEWDECIMAL:
                    $columns[$i] = new Meta\DecimalMeta($type, $buffer->readUInt8(), $buffer->readUInt8());
                    break;

                case ColumnType::BIT:
                    $bits = $buffer->readUInt8();
                    $bytes = $buffer->readUInt8();

                    $bits = ($bytes * 8) + $bits;
                    $bytes = (int)(($bits + 7) / 8);
                    $columns[$i] = new Meta\BitMeta($type, $bytes, $bits);
                    break;

                default:
                    $columns[$i] = new Meta\CommonMeta($type);
                    break;
            }
        }

        return $columns;
    }

    /**
     * See https://github.com/kogel-net/Kogel.Subscribe/blob/ce494592665d695a3cef09936e997e621228a76e/Kogel.Slave.Mysql/Events/TableMapEvent.cs
     */
    public function readOptionalMetadata(Buffer $buffer, Header $header, int $columnCount, array $columns): array
    {
        $metadata = [];
        while ($header->payloadSize > $buffer->getOffset()) {
            $type = OptionalMetadataType::from($buffer->readUint8());
            $length = $buffer->readCodedBinary();
            $sub = $buffer->slice($length);

            switch ($type) {
                case OptionalMetadataType::SIGNEDNESS:
                    $metadata[$type->value] = $sub->read(($columnCount + 7) >> 3);
                    break;

                /**
                 * https://github.com/MariaDB/server/blob/b1856aff37557e82b0e53ddbd89fc41f86df07e6/sql/share/charsets/Index.xml
                 */
                case OptionalMetadataType::DEFAULT_CHARSET:
                case OptionalMetadataType::ENUM_AND_SET_DEFAULT_CHARSET:
                    $metadata[$type->value] = ['defaultCharsetCollation' => Collation::from($sub->readCodedBinary())];
                    while ($sub->getLeft()) {
                        $metadata[$type->value]['charsetCollations'][$sub->readCodedBinary()] = Collation::from($sub->readCodedBinary());
                    }
                    break;

                case OptionalMetadataType::COLUMN_NAME:
                    $metadata[$type->value] = [];
                    while ($sub->getLeft()) {
                        $metadata[$type->value][] = $sub->readVariableLengthString();
                    }
                    break;

                case OptionalMetadataType::ENUM_STR_VALUE:
                    $metadata[$type->value] = [];
                    for ($j = 0; $sub->getLeft(); ++$j) {
                        $valuesCount = $sub->readCodedBinary();
                        for ($i = 0; $i < $valuesCount; ++$i) {
                            $metadata[$type->value][$j][$i] = $sub->readVariableLengthString();
                        }
                    }
                    break;

                case OptionalMetadataType::SIMPLE_PRIMARY_KEY:
                    $metadata[$type->value] = [];
                    while ($sub->getLeft()) {
                        $metadata[$type->value][] = $sub->readCodedBinary();
                    }
                    break;

                default:
                    throw new \UnexpectedValueException(sprintf('Unknown optional medatada type %d', $type->value));
            }
        }

        if (!isset($metadata[OptionalMetadataType::COLUMN_NAME->value])) {
            throw new \RuntimeExeption('Columns names was not found in TABLE_MAP event, please make sure binlog_row_metadata=FULL option is set');
        }

        $integerColumn = 0;
        $enumColumn = 0;

        foreach ($columns as $i => $column) {
            switch ($column->type) {
                case ColumnType::TINY:
                case ColumnType::SHORT:
                case ColumnType::INT24:
                case ColumnType::LONG:
                case ColumnType::LONGLONG:
                    $bitmap = $metadata[OptionalMetadataType::SIGNEDNESS->value];
                    $columns[$i] = new Column\IntegerColumn(
                        $i,
                        $column,
                        $metadata[OptionalMetadataType::COLUMN_NAME->value][$i],
                        !(\ord($bitmap[$integerColumn >> 3]) & (1 << (7 - ($integerColumn & 0x07))))
                    );
                    ++$integerColumn;
                    break;
                case ColumnType::FLOAT:
                case ColumnType::DOUBLE:
                    $columns[$i] = new Column\FloatColumn(
                        $i,
                        $column,
                        $metadata[OptionalMetadataType::COLUMN_NAME->value][$i],
                    );
                    break;
                    break;

                case ColumnType::VARCHAR:
                case ColumnType::STRING:
                    // TODO: check COLUMN_CHARSET, then DEFAULT_CHARSET
                    // TODO: DEFAULT_CHARSET['charsetCollations'] ?!
                    $columns[$i] = new Column\TextColumn(
                        $i,
                        $column,
                        $metadata[OptionalMetadataType::COLUMN_NAME->value][$i],
                        $metadata[OptionalMetadataType::DEFAULT_CHARSET->value]['defaultCharsetCollation'],
                    );
                    break;


                case ColumnType::DATE:
                case ColumnType::DATETIME2:
                case ColumnType::TIMESTAMP2:
                    $columns[$i] = new Column\TimeColumn(
                        $i,
                        $column,
                        $metadata[OptionalMetadataType::COLUMN_NAME->value][$i],
                    );
                    break;

                case ColumnType::BLOB:
                    $columns[$i] = new Column\BlobColumn(
                        $i,
                        $column,
                        $metadata[OptionalMetadataType::COLUMN_NAME->value][$i],
                    );
                    break;

                case ColumnType::ENUM:
                    $columns[$i] = new Column\EnumColumn(
                        $i,
                        $column,
                        $metadata[OptionalMetadataType::COLUMN_NAME->value][$i],
                        $metadata[OptionalMetadataType::ENUM_AND_SET_DEFAULT_CHARSET->value]['defaultCharsetCollation'],
                        $metadata[OptionalMetadataType::ENUM_STR_VALUE->value][$enumColumn],
                    );
                    ++$enumColumn;
                    break;

                default:
                    throw new \UnexpectedValueException(sprintf('Unknown column type %s', var_export($column->type, true)));
            }
        }

        // TODO:! PRIMARY_KEY_WITH_PREFIX
        // TODO:! use names of collations instead of ids
        return [
            $columns,
            array_map(
                fn (int $columnIndex) => $columns[$columnIndex],
                $metadata[OptionalMetadataType::SIMPLE_PRIMARY_KEY->value] ?? []
            ) ?: null,
        ];
    }
}
