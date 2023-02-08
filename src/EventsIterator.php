<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog;

use Iterator;
use IteratorAggregate;
use SysvMessageQueue;
use Amp\DeferredCancellation;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Revolt\EventLoop;
use UserQQ\MySQL\Binlog\Connection\Buffer;
use UserQQ\MySQL\Binlog\Connection\Connection;
use UserQQ\MySQL\Binlog\Connection\Packet;
use UserQQ\MySQL\Binlog\Deserializer\ColumnMetadataFactory;
use UserQQ\MySQL\Binlog\Deserializer\RowFactory;
use UserQQ\MySQL\Binlog\Protocol\ColumnType;
use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Events;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;
use UserQQ\MySQL\Binlog\Protocol\Event\RowEvent;
use UserQQ\MySQL\Binlog\Protocol\Event\Type;

class EventsIterator implements IteratorAggregate
{
    private readonly Connection                $connection;
    private readonly ColumnMetadataFactory     $columnsMetadataFactory;
    private readonly RowFactory                $rowFactory;
    private readonly StatisticsCollector       $statisticsCollector;

    private          ?Events\FormatDescription $formatDescription = null;

    private          array                     $tableMaps = [];

    private          BinlogPosition            $position;

    private readonly bool                      $include;
    private readonly bool                      $exclude;
    private readonly bool                      $check;

    public function __construct(
        private readonly Config               $config       = new Config,
        private readonly LoggerInterface      $logger       = new NullLogger,
        private readonly DeferredCancellation $cancellation,
    ) {
        $this->connection = new Connection($config, $logger);
        $this->columnsMetadataFactory = new ColumnMetadataFactory();
        $this->rowFactory = new RowFactory();

        $this->statisticsCollector = new StatisticsCollector($logger);

        $this->position = new BinlogPosition($this->connection->getBinlogFile(), $this->connection->getBinlogPosition());

        if (null !== $this->config->statisticsInterval) {
            EventLoop::unreference(EventLoop::repeat($this->config->statisticsInterval, $this->statisticsCollector->flush(...)));
        }

        $this->include = ($config->tables && count($config->tables))
            || ($config->databases && count ($config->databases));
        $this->exclude = ($config->excludeTables && count($config->excludeTables))
            || ($config->excludeDatabases && count ($config->excludeDatabases));
        $this->check = $this->include || $this->exclude;
    }

    public function getIterator(): Iterator
    {
        foreach ($this->connection as $buffer) {
            if (null !== $event = $this->parse($buffer)) {
                yield $this->position => $event;
            }

            if ($this->cancellation->isCancelled()) {
                break;
            }
        }

        $this->logger->info(sprintf('End events queue', $signal));
    }

    private function check(Events\TableMap $tableMap): bool
    {
        if (!$this->check) {
            return false;
        }

        if ($this->include) {
            if (null !== $this->config->databases && !in_array($tableMap->schema, $this->config->databases, true)) {
                return true;
            }

            if (null !== $this->config->tables && !in_array("{$tableMap->schema}.{$tableMap->table}", $this->config->tables, true)) {
                return true;
            }
        }

        if ($this->exclude) {
            if (null !== $this->config->excludeDatabases && in_array($tableMap->schema, $this->config->excludeDatabases, true)) {
                return true;
            }

            if (null !== $this->config->excludeTables && in_array("{$tableMap->schema}.{$tableMap->table}", $this->config->excludeTables, true)) {
                return true;
            }
        }

        return false;
    }

    private function parse(Buffer $buffer): ?Event
    {
        $header = $this->readEventHeader($buffer);

        if (null === $this->formatDescription && $header->type !== Type::FORMAT_DESCRIPTION_EVENT) {
            throw new \UnexpectedValueException(sprintf('Expected to got FORMAT_DESCRIPTION_EVENT first, but got %s', var_export($header->type, true)));
        }

        if ($header->type === Type::FORMAT_DESCRIPTION_EVENT) {
            $this->formatDescription = $this->readFormatDescriptionEvent($buffer, $header);
            $this->logger->info(sprintf(
                '[EVENT][FORMAT_DESCRIPTION] server: %s, version: %d, checksum: %d',
                $this->formatDescription->serverVersion,
                $this->formatDescription->formatVersion,
                $this->formatDescription->checksumAlgorithmType,
            ));

            return null;
        }

        if ($header->type === Type::HEARTBEAT_EVENT) {
            $this->logger->debug('[EVENT][HEARTBEAT]');
            return null;
        }

        switch ($header->type) {
            case Type::TABLE_MAP_EVENT:
                $event = $this->readTableMapEvent($buffer, $header);
                $this->tableMaps[$event->tableId] = $event;
                $this->rowFactory->addTableMap($event);
                return null;
                break;
            case Type::ROTATE_EVENT:
                $event = $this->readRotateEvent($buffer, $header);
                if ($this->position->filename !== $event->filename || $this->position->position !== $event->position) {
                    $this->logger->info(sprintf('[EVENT][ROTATE] %s:%d', $event->filename, $event->position));
                }

                $this->position = new BinlogPosition($event->filename, $event->position);
                return null;
                break;
        }

        if (
            ($this->config->binlogPosition && $this->config->binlogFile)
            && ($this->config->binlogFile === $this->position->filename)
            && ($this->config->binlogPosition > $this->position->position)
        ) {
            $this->position = new BinlogPosition($this->position->filename, $header->nextPosition);
            $this->statisticsCollector->pushHeader($header);
            return null;
        }

        switch ($header->type) {
            case Type::UPDATE_ROWS_EVENTv1:
                $event = $this->readUpdateRowsEventV1($buffer, $header);
                break;
            case Type::UPDATE_ROWS_EVENTv2:
                $event = $this->readUpdateRowsEventV2($buffer, $header);
                break;
            case Type::WRITE_ROWS_EVENTv1:
                $event = $this->readWriteRowsEventV1($buffer, $header);
                break;
            case Type::WRITE_ROWS_EVENTv2:
                $event = $this->readWriteRowsEventV2($buffer, $header);
                break;
            case Type::DELETE_ROWS_EVENTv1:
                $event = $this->readDeleteRowsEventV1($buffer, $header);
                break;
            case Type::DELETE_ROWS_EVENTv2:
                $event = $this->readDeleteRowsEventV2($buffer, $header);
                break;

            case Type::QUERY_EVENT:
                $event ??= $this->readQueryEvent($buffer, $header);
                // nobreak;
            case Type::XID_EVENT:
                $event ??= $this->readXidEvent($buffer, $header);
                // nobreak;
            default:
                assert($buffer->getLeft() === $header->checksumSize);
                assert($header->checksumSize === 0 || strrev($buffer->read()) === hash('crc32b', substr((string) $buffer, 1, -1 * $header->checksumSize), true));

                $this->statisticsCollector->pushEvent($event);
                $this->position = new BinlogPosition($this->position->filename, $header->nextPosition);
                return null;
        }

        if (null === $event) {
            return null;
        }

        assert($buffer->getLeft() === $header->checksumSize);
        assert($header->checksumSize === 0 || strrev($buffer->read()) === hash('crc32b', substr((string) $buffer, 1, -1 * $header->checksumSize), true));

        $this->statisticsCollector->pushRowEvent($event);
        $this->position = new BinlogPosition($this->position->filename, $header->nextPosition);

        return $event;
    }

    private function readEventHeader(Buffer $buffer): Header
    {
        /* TODO:! Check binlog version */
        return new Header(
            $this->position,
            $buffer->readInt32(),              // uint<4> Timestamp (creation time)
            Type::from($buffer->readUInt8()),  // uint<1> Event Type (type_code)
            $buffer->readInt32(),              // uint<4> Server_id (server which created the event)
            $eventSize = $buffer->readInt32(), // uint<4> Event Length (header + data)
            $buffer->readInt32(),              // uint<4> Next Event position
            $buffer->readUInt16(),             // uint<2> Event flags
            $checksumSize = ($this->formatDescription?->checksumAlgorithmType > 0) ? 4 : 0,
            ($eventSize + 1 /* Packet type */) - $checksumSize,
        );
    }

    private function readTableMapEvent(Buffer $buffer, Header $header): Events\TableMap
    {
        $tableId = $buffer->readUInt48(); // uint<6> The table ID.
        $reserved = $buffer->readUInt16(); // uint<2> Reserved for future use.
        $schema = $buffer->read($buffer->readUInt8()); // uint<1> Database name length. // string<NUL> The database name (null-terminated).
        $table = $buffer->read($buffer->skip(1)->readUInt8()); // uint<1> Table name length. // string<NUL> The table name (null-terminated).
        $columnCount = $buffer->skip(1)->readCodedBinary(); // int<lenenc> The number of columns in the table.
        $columns = $this->columnsMetadataFactory->readColumns($buffer, $columnCount);
        $nullableBitField = $buffer->read(($columnCount + 7) >> 3); // byte<n> Bit-field indicating whether each column can be NULL, one bit per column.
        [$columns, $primaryKeyColumns] = $this->columnsMetadataFactory->readOptionalMetadata($buffer, $header, $columnCount, $columns);

        return new Events\TableMap(
            $header,
            $tableId,
            $reserved,
            $schema,
            $table,
            $columnCount,
            $columns,
            $nullableBitField,
            $primaryKeyColumns,
        );
    }

    private function readRotateEvent(Buffer $buffer, Header $header): Events\Rotate
    {
        return new Events\Rotate(
            $header,
            (int) $buffer->readUInt64(),
            $buffer->read($header->payloadSize - $buffer->getOffset())
        );
    }

    private function readXidEvent(Buffer $buffer, Header $header): Events\Xid
    {
        return new Events\Xid(
            $header,
            (string) $buffer->readUInt64(),
        );
    }

    private function readQueryEvent(Buffer $buffer, Header $header): Events\Query
    {
        return new Events\Query(
            $header,
            $buffer->readUInt32(),
            $buffer->readUInt32(),
            $schemaLength = $buffer->readUInt8(),
            $buffer->readUInt16(),
            $statusVarsLength = $buffer->readUInt16(),
            $buffer->skip($statusVarsLength)->read($schemaLength), /* TODO!: parse status vars */
            $buffer->read($header->payloadSize - $buffer->getOffset()),
        );
    }

    private function readFormatDescriptionEvent(Buffer $buffer, Header $header): Events\FormatDescription
    {
        return new Events\FormatDescription(
            $header,
            $buffer->readUInt16(),                // uint<2> The binary log format version. This is 4 in MariaDB 10 and up.
            trim($buffer->read(50), "\x0"),       // string<50> The MariaDB server version (example: 10.2.1-debug-log), padded with 0x00 bytes on the right.
            $buffer->readUInt32(),                // uint<4> Timestamp in seconds when this event was created (this is the moment when the binary log was created). This value is redundant; the same value occurs in the timestamp header field.
            $headerLength = $buffer->readUInt8(), // uint<1> The header length. This length - 19 gives the size of the extra headers field at the end of the header for other events.
            $buffer->read(                        // byte<n> Variable-sized. An array that indicates the post-header lengths for all event types.
                $header->eventSize - $headerLength - (2 + 50 + 4 + 1) - 1 - 4
            ),
                                                  // There is one byte per event type that the server knows about.
                                                  // The value 'n' comes from the following formula:
                                                  // n = event_size - header length - offset (2 + 50 + 4 + 1) - checksum_algo - checksum
            $buffer->readUInt8(),                 // uint<1> Checksum Algorithm Type
            // uint<4> CRC32 4 bytes (value matters only if checksum algo is CRC32)
        );
    }

    private function readUpdateRowsEventV1(Buffer $buffer, Header $header): ?Events\UpdateRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        return new Events\UpdateRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            $columnsBitmapAfter = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap, $columnsBitmapAfter)),
            $rows,
        );
    }

    private function readUpdateRowsEventV2(Buffer $buffer, Header $header): ?Events\UpdateRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        return new Events\UpdateRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            $columnsBitmapAfter = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap, $columnsBitmapAfter)),
            $rows,
        );
    }

    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_write_rows_v2
    // https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
    private function readWriteRowsEventV1(Buffer $buffer, Header $header): ?Events\WriteRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        return new Events\WriteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }

    private function readWriteRowsEventV2(Buffer $buffer, Header $header): ?Events\WriteRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        return new Events\WriteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }

    private function readDeleteRowsEventV1(Buffer $buffer, Header $header): ?Events\DeleteRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        return new Events\DeleteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }

    private function readDeleteRowsEventV2(Buffer $buffer, Header $header): ?Events\DeleteRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        return new Events\DeleteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }
}
