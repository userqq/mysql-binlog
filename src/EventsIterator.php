<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog;

use Iterator;
use IteratorAggregate;
use SysvMessageQueue;
use RuntimeException;
use Amp\Cancellation;
use Amp\CancelledException;
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

/**
 * @template-implements IteratorAggregate<Events\WriteRows|Events\UpdateRows|Events\DeleteRows>
 */
final class EventsIterator implements IteratorAggregate
{
    private readonly Connection                $connection;
    private readonly ColumnMetadataFactory     $columnsMetadataFactory;
    private readonly RowFactory                $rowFactory;
    private readonly StatisticsCollector       $statisticsCollector;

    private          ?Events\FormatDescription $formatDescription = null;

    private          array                     $tableMaps = [];

    private          BinlogPosition            $position;
    private          int                       $nextOffset;

    private readonly bool                      $include;
    private readonly bool                      $exclude;
    private readonly bool                      $check;

    public function __construct(
        private readonly Config          $config       = new Config,
        private readonly LoggerInterface $logger       = new NullLogger,
        private readonly ?Cancellation   $cancellation = null,
    ) {
        $this->connection = new Connection($config, $logger, $cancellation);
        $this->columnsMetadataFactory = new ColumnMetadataFactory();
        $this->rowFactory = new RowFactory();

        $this->statisticsCollector = new StatisticsCollector($logger);

        $this->position = new BinlogPosition($this->connection->getBinlogFile(), $this->nextOffset = $this->connection->getBinlogPosition());

        if (null !== $this->config->statisticsInterval) {
            EventLoop::unreference(EventLoop::repeat($this->config->statisticsInterval, $this->statisticsCollector->flush(...)));
        }

        $this->include = (null !== $config->tables && \count($config->tables))
            || (null !== $config->databases && \count($config->databases));
        $this->exclude = (null !== $config->excludeTables && \count($config->excludeTables))
            || (null !== $config->excludeDatabases && \count($config->excludeDatabases));
        $this->check = $this->include || $this->exclude;
    }

    public function getIterator(): Iterator
    {
        try {
            foreach ($this->connection as $buffer) {
                if (null !== $event = $this->parse($buffer)) {
                    yield $this->position => $event;
                }

                $this->cancellation?->throwIfRequested();
            }
        } catch (CancelledException) {
            $this->connection->close();
        }

        $this->logger->info('End events queue');
    }

    public function getPosition(): BinlogPosition
    {
        if (
            ($this->config->binlogPosition && $this->config->binlogFile)
            && ($this->config->binlogFile === $this->position->filename)
            && ($this->config->binlogPosition > $this->position->position)
        ) {
            return new BinlogPosition($this->config->binlogFile, $this->config->binlogPosition);
        }

        return $this->position;
    }

    private function check(Events\TableMap $tableMap): bool
    {
        if (!$this->check) {
            return false;
        }

        if ($this->include) {
            if (null !== $this->config->databases && !\in_array($tableMap->schema, $this->config->databases, true)) {
                return true;
            }

            if (null !== $this->config->tables && !\in_array("{$tableMap->schema}.{$tableMap->table}", $this->config->tables, true)) {
                return true;
            }
        }

        if ($this->exclude) {
            if (null !== $this->config->excludeDatabases && \in_array($tableMap->schema, $this->config->excludeDatabases, true)) {
                return true;
            }

            if (null !== $this->config->excludeTables && \in_array("{$tableMap->schema}.{$tableMap->table}", $this->config->excludeTables, true)) {
                return true;
            }
        }

        return false;
    }

    private function parse(Buffer $buffer): ?Event
    {
        $header = $this->readEventHeader($buffer);
        $event = null;

        if (null === $this->formatDescription && $header->type !== Type::FORMAT_DESCRIPTION_EVENT) {
            throw new \UnexpectedValueException(\sprintf('Expected to got FORMAT_DESCRIPTION_EVENT first, but got %s', var_export($header->type, true)));
        }

        if ($header->type === Type::FORMAT_DESCRIPTION_EVENT) {
            $this->formatDescription = $this->readFormatDescriptionEvent($buffer, $header);
            $this->logger->info(\sprintf(
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

        if ($header->type === Type::ROTATE_EVENT) {
            $event = $this->readRotateEvent($buffer, $header);
            if ($this->position->filename !== $event->filename || $this->position->position !== $event->position) {
                $this->logger->info(\sprintf('[EVENT][ROTATE] %s:%d', $event->filename, $event->position));
            }
            $this->tableMaps = [];
            $this->rowFactory->dropTableMaps();
            $this->statisticsCollector->pushEvent($event);
            $this->position = new BinlogPosition($event->filename, $this->nextOffset = $event->position);
            return null;
        }

        assert(
            $header->nextPosition->position === $header->nextPositionShort->position
                || ($header->nextPosition->position % 4294967296) === $header->nextPositionShort->position,
            sprintf(
                'Next event position %s is not equal to event header value %s',
                json_encode($header->nextPosition),
                json_encode($header->nextPositionShort),
            ),
        );

        if (
            !(
                $header->nextPosition->position === $header->nextPositionShort->position
                    || ($header->nextPosition->position % 4294967296) === $header->nextPositionShort->position
            )
        ) {
            throw new RuntimeException(sprintf(
                'Next event position missmatch, expected to have %s (or with equal offset) but got %s',
                json_encode($header->nextPosition),
                json_encode($header->nextPositionShort),
            ));
        }

        if ($header->type === Type::TABLE_MAP_EVENT) {
            $event = $this->readTableMapEvent($buffer, $header);
            $this->tableMaps[$event->tableId] = $event;
            $this->rowFactory->addTableMap($event);
            $this->statisticsCollector->pushEvent($event);
            $this->position = $header->nextPosition;
            return null;
        }

        if (
            ($this->config->binlogPosition && $this->config->binlogFile)
            && ($this->config->binlogFile === $this->position->filename)
            && ($this->config->binlogPosition > $this->position->position)
        ) {
            $this->position = $header->nextPosition;
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
            default:
                switch ($header->type) {
                    case Type::QUERY_EVENT:
                        $event = $this->readQueryEvent($buffer, $header);
                        break;
                    case Type::XID_EVENT:
                        $event = $this->readXidEvent($buffer, $header);
                        break;
                    case Type::USER_VAR_EVENT:
                    case Type::STOP_EVENT:
                    case Type::PREVIOUS_GTIDS_EVENT:
                    case Type::ANONYMOUS_GTID_EVENT:
                        $this->position = $header->nextPosition;
                        return null;
                        break;
                    default:
                        /** @psalm-suppress TypeDoesNotContainType */
                        \assert(null !== $event);
                }

                \assert($buffer->getLeft() === $header->checksumSize);
                \assert($header->checksumSize === 0 || \strrev($buffer->read()) === \hash('crc32b', \substr((string) $buffer, 1, -1 * $header->checksumSize), true));

                $this->statisticsCollector->pushEvent($event);
                $this->position = $header->nextPosition;
                return null;
        }

        if (null === $event) {
            return null;
        }

        \assert($buffer->getLeft() === $header->checksumSize);
        \assert($header->checksumSize === 0 || \strrev($buffer->read()) === \hash('crc32b', \substr((string) $buffer, 1, -1 * $header->checksumSize), true));

        /** @psalm-suppress PossiblyNullArgument */
        $this->statisticsCollector->pushRowEvent($event);
        $this->position = $header->nextPosition;

        return $event;
    }

    /**
     * Check binlog version
     */
    private function readEventHeader(Buffer $buffer): Header
    {
        return new Header(
            $this->position,
            $buffer->readUInt32(),
            $type = Type::from($buffer->readUInt8()),
            $buffer->readUInt32(),
            $eventSize = $buffer->readUInt32(),
            new BinlogPosition($this->position->filename, $buffer->readUInt32()),
            new BinlogPosition($this->position->filename, $type === Type::HEARTBEAT_EVENT ? $this->nextOffset : $this->nextOffset += $eventSize),
            $buffer->readUInt16(),
            $checksumSize = ($this->formatDescription?->checksumAlgorithmType > 0) ? 4 : 0,
            ($eventSize + 1 /* Packet type header*/) - $checksumSize,
        );
    }

    private function readTableMapEvent(Buffer $buffer, Header $header): Events\TableMap
    {
        $tableId = $buffer->readUInt48();
        $reserved = $buffer->readUInt16();
        $schema = $buffer->read($buffer->readUInt8());
        $table = $buffer->read($buffer->skip(1)->readUInt8());
        $columnCount = $buffer->skip(1)->readCodedBinary();
        /** @psalm-suppress PossiblyNullArgument */
        $columns = $this->columnsMetadataFactory->readColumns($buffer, $columnCount);
        $nullableBitField = $buffer->read(($columnCount + 7) >> 3);
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
            $buffer->skip($statusVarsLength)->read($schemaLength),
            $buffer->read($header->payloadSize - $buffer->getOffset()),
        );
    }

    private function readFormatDescriptionEvent(Buffer $buffer, Header $header): Events\FormatDescription
    {
        return new Events\FormatDescription(
            $header,
            $buffer->readUInt16(),
            trim($buffer->read(50), "\x0"),
            $buffer->readUInt32(),
            $headerLength = $buffer->readUInt8(),
            $buffer->read($header->eventSize - $headerLength - (2 + 50 + 4 + 1) - 1 - 4),
            $buffer->readUInt8(),
        );
    }

    private function readUpdateRowsEventV1(Buffer $buffer, Header $header): ?Events\UpdateRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\UpdateRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            $columnsBitmapAfter = $buffer->read(($columnCount + 7) >> 3),
            \count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap, $columnsBitmapAfter)),
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

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\UpdateRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            $columnsBitmapAfter = $buffer->read(($columnCount + 7) >> 3),
            \count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap, $columnsBitmapAfter)),
            $rows,
        );
    }

    /**
     * TODO!: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_write_rows_v2
     * TODO!: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
     */
    private function readWriteRowsEventV1(Buffer $buffer, Header $header): ?Events\WriteRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        if ($this->check($tableMap)) {
            return null;
        }

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\WriteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            \count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
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

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\WriteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            \count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
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

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\DeleteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            \count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
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

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\DeleteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            \count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }
}
