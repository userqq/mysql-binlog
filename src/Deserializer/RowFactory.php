<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Deserializer;

use UnexpectedValueException;
use UserQQ\MySQL\Binlog\Connection\Buffer;
use UserQQ\MySQL\Binlog\Protocol\ColumnType;
use UserQQ\MySQL\Binlog\Protocol\Event\Events\TableMap;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;

class RowFactory
{
    private array $tableMaps = [];

    public function addTableMap(TableMap $tableMap): void
    {
        $this->tableMaps[$tableMap->tableId] = $tableMap;
    }

    public function readRows(Buffer $buffer, Header $header, int $tableId, int $columnCount, string $columnsBitmap, ?string $columnsBitmapAfter = null): array
    {
        $columns = $this->tableMaps[$tableId]->columns;

        $nullBitmapLength = 0;
        for ($i = 0; $i < $columnCount; ++$i) {
            $nullBitmapLength += (\ord($columnsBitmap[$i >> 3]) & (1 << ($i & 0x07))) ? 1 : 0;
        }
        $nullBitmapLength = ($nullBitmapLength + 7) >> 3;

        if ($columnsBitmapAfter) {
            $nullBitmapLengthAfter = 0;
            for ($i = 0; $i < $columnCount; ++$i) {
                $nullBitmapLengthAfter += (\ord($columnsBitmapAfter[$i >> 3]) & (1 << ($i & 0x07))) ? 1 : 0;
            }
            $nullBitmapLengthAfter = ($nullBitmapLengthAfter + 7) >> 3;
        }

        $rows = [];
        for ($j = 0; $header->payloadSize > $buffer->getOffset(); ++$j) {
            /** @psalm-suppress EmptyArrayAccess */
            $row = &$rows[$j];
            $bitmap = $columnsBitmap;
            $nullBitmapLengthCurrent = $nullBitmapLength;
            if ($columnsBitmapAfter) {
                $row = &$rows[$j]['before'];
            }

            repeat: {
                $nullBitmap = $buffer->read($nullBitmapLengthCurrent);

                $nullIndex = 0;
                for ($i = 0; $i < $columnCount; ++$i) {
                    $column = $columns[$i];

                    if (!(\ord($bitmap[$nullIndex >> 3]) & (1 << ($nullIndex & 0x07)))) {
                        $row[$column->name] = null;
                        continue;
                    }

                    // $row = array_combine($this->tableMaps[$tableId]->optionalMetadata['COLUMN_NAME'], $row);
                    if ((\ord($nullBitmap[$nullIndex >> 3]) & (1 << ($nullIndex & 0x07)))) {
                        $row[$column->name] = null;
                    } else {
                        switch ($column->meta->type) {
                            case ColumnType::TINY:
                                $row[$column->name] = $column->isSigned ? $buffer->readInt8() : $buffer->readUInt8();
                                break;
                            case ColumnType::SHORT:
                                $row[$column->name] = $column->isSigned ? $buffer->readInt16() : $buffer->readUInt16();
                                break;
                            case ColumnType::INT24:
                                $row[$column->name] = $column->isSigned ? $buffer->readInt24() : $buffer->readUInt24();
                                break;
                            case ColumnType::LONG:
                                $row[$column->name] = $column->isSigned ? $buffer->readInt32() : $buffer->readUInt32();
                                break;

                            case ColumnType::LONGLONG:
                                $row[$column->name] = $column->isSigned ? $buffer->readInt64() : $buffer->readUInt64();
                                break;

                            case ColumnType::FLOAT:
                                $row[$column->name] = round($buffer->readFloat(), $column->meta->size);
                                break;

                            case ColumnType::DOUBLE:
                                $row[$column->name] = $buffer->readDouble();
                                break;

                            case ColumnType::BIT:
                                $row[$column->name] = $buffer->readBit($column->meta->bytes, $column->meta->bits);
                                break;

                            case ColumnType::VARCHAR:
                            case ColumnType::STRING:
                                $row[$column->name] = $buffer->readLengthString($column->meta->maxLength > 255 ? 2 : 1);
                                break;

                            case ColumnType::BLOB:
                                $row[$column->name] = $buffer->readLengthString($column->meta->length);
                                break;

                            case ColumnType::DATE:
                                $row[$column->name] = $buffer->readDate();
                                break;

                            case ColumnType::DATETIME2:
                                $row[$column->name] = $buffer->readDateTime2($column->meta->fsp);
                                break;

                            case ColumnType::TIMESTAMP2:
                                $row[$column->name] = $buffer->readTimestamp2($column->meta->fsp);
                                break;

                            case ColumnType::ENUM:
                                $row[$column->name] = $column->values[$buffer->readUIntBySize($column->meta->size) - 1]
                                    ?? '';
                                break;

                            default:
                                throw new UnexpectedValueException(sprintf('Got column with unexpected data type %s', var_export($column, true)));
                                exit();
                        }
                    }

                    ++$nullIndex;
                }

                if ($columnsBitmapAfter && 1 === count($rows[$j])) {
                    $row = &$rows[$j]['after'];
                    $bitmap = $columnsBitmapAfter;
                    /** @psalm-suppress PossiblyUndefinedVariable */
                    $nullBitmapLengthCurrent = $nullBitmapLengthAfter;
                    goto repeat;
                }
            }
        }

        return $rows;
    }
}
