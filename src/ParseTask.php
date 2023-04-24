<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog;

use Amp\Parallel\Worker\Task;
use Amp\Sync\Channel;

use Iterator;
use IteratorAggregate;
use SysvMessageQueue;
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
 * @template-implements Task<string, never, never>
 */
final class ParseTask implements Task
{
    public function __construct(
        private readonly Header $header,
        private readonly Buffer $buffer,
        private readonly array $tableMaps,
    ) {
    }

    private ?RowFactory $rowFactory = null;

    public function run(Channel $channel, Cancellation $cancellation): mixed
    {
        $this->rowFactory ??= new RowFactory();
        foreach ($this->tableMaps as $tableMap) {
            $this->rowFactory->addTableMap($tableMap);
        }

        switch ($this->header->type) {
            case Type::UPDATE_ROWS_EVENTv1:
                $event = $this->readUpdateRowsEventV1($this->buffer, $this->header);
                break;
            case Type::UPDATE_ROWS_EVENTv2:
                $event = $this->readUpdateRowsEventV2($this->buffer, $this->header);
                break;
            case Type::WRITE_ROWS_EVENTv1:
                $event = $this->readWriteRowsEventV1($this->buffer, $this->header);
                break;
            case Type::WRITE_ROWS_EVENTv2:
                $event = $this->readWriteRowsEventV2($this->buffer, $this->header);
                break;
            case Type::DELETE_ROWS_EVENTv1:
                $event = $this->readDeleteRowsEventV1($this->buffer, $this->header);
                break;
            case Type::DELETE_ROWS_EVENTv2:
                $event = $this->readDeleteRowsEventV2($this->buffer, $this->header);
                break;
        }
        
        // return $header->type;

        return $event;

        try {
            return $this->parse($this->buffer);
        } catch (\Throwable $t) {
            return $t;
        }
    }

    private function readUpdateRowsEventV1(Buffer $buffer, Header $header): ?Events\UpdateRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        // if ($this->check($tableMap)) {
            // return null;
        // }

        /** @psalm-suppress PossiblyNullArgument */
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

        // if ($this->check($tableMap)) {
            // return null;
        // }

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\UpdateRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            $columnsBitmapAfter = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap, $columnsBitmapAfter)),
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

        // if ($this->check($tableMap)) {
            // return null;
        // }

        /** @psalm-suppress PossiblyNullArgument */
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

        // if ($this->check($tableMap)) {
            // return null;
        // }

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\WriteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }

    private function readDeleteRowsEventV1(Buffer $buffer, Header $header): ?Events\DeleteRows
    {
        $tableId = $buffer->readUInt48();
        $tableMap = $this->tableMaps[$tableId];

        // if ($this->check($tableMap)) {
            // return null;
        // }

        /** @psalm-suppress PossiblyNullArgument */
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

        // if ($this->check($tableMap)) {
            // return null;
        // }

        /** @psalm-suppress PossiblyNullArgument */
        return new Events\DeleteRows(
            $header,
            $tableId,
            $tableMap,
            $buffer->readUint16(),
            $columnCount = $buffer->skip((int) ($buffer->readUInt16() / 8))->readCodedBinary(),
            $columnsBitmap = $buffer->read(($columnCount + 7) >> 3),
            count($rows = $this->rowFactory->readRows($buffer, $header, $tableId, $columnCount, $columnsBitmap)),
            $rows,
        );
    }
}


