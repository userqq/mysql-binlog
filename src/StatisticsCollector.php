<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog;

use Psr\Log\LoggerInterface;
use UserQQ\MySQL\Binlog\Protocol\Event\Event;
use UserQQ\MySQL\Binlog\Protocol\Event\Events;
use UserQQ\MySQL\Binlog\Protocol\Event\Header;
use UserQQ\MySQL\Binlog\Protocol\Event\RowEvent;

final class StatisticsCollector
{
    private const LOG_FORMAT = '| %\' 6d %\' 9s %\' 10s %\' 8.2fms (%\' 10s)  %s';

    private int   $events;
    private int   $bytes;
    private int   $rows;
    private array $tables;

    private float $start;

    public function __construct(
        private readonly LoggerInterface $logger,
    ) {
        $this->reset();
    }

    private function reset(): void
    {
        $this->events = 0;
        $this->bytes = 0;
        $this->rows = 0;
        $this->tables = [];

        $this->start = microtime(true);
    }

    private function formatBytes(int $bytes, int $precision = 2): string
    {
        $base = log($bytes, 1024);
        return 0 === $bytes
            ? '0.00b '
            : number_format(round(pow(1024, $base - floor($base)), $precision), 2) . ['b ', 'KB', 'MB', 'GB', 'TB'][(int) floor($base)];
    }

    public function flush(): void
    {
        if ($this->bytes) {
            $this->logger->notice(\sprintf(
                static::LOG_FORMAT,
                $this->events,
                $this->formatBytes($this->bytes),
                number_format($this->rows),
                (microtime(true) - $this->start) * 1000,
                $this->formatBytes(memory_get_usage()),
                json_encode($this->tables),
            ));
        }

        $this->reset();
    }

    public function pushHeader(Header $header): void
    {
        $this->bytes += ($header->payloadSize + $header->checksumSize);
    }

    /**
     * @param Events\TableMap|Events\Rotate|Events\Xid|Events\Query|Events\FormatDescription|Events\UpdateRows|Events\WriteRows|Events\DeleteRows $event
     */
    public function pushEvent(Event $event): void
    {
        ++$this->events;

        $this->pushHeader($event->header);
    }

    /**
     * @param Events\UpdateRows|Events\WriteRows|Events\DeleteRows $event
     */
    public function pushRowEvent(RowEvent $event): void
    {
        $this->rows += $event->count;

        $this->tables[$event->tableMap->table] ??= 0;
        $this->tables[$event->tableMap->table] += $event->count;

        $this->pushEvent($event);
    }
}
