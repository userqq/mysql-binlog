<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection;

use Iterator;
use IteratorAggregate;
use Amp\Cancellation;
use Amp\Future;
use Amp\ByteStream\BufferedReader;
use Amp\Socket\Socket;
use Psr\Log\LoggerInterface;
use UserQQ\MySQL\Binlog\Config;
use UserQQ\MySQL\Binlog\Protocol\ColumnType;

use function Amp\async;
use function Amp\Socket\connect;

/**
 * @template-implements IteratorAggregate<Buffer>
 */
final class Connection implements IteratorAggregate
{
    private const SOCKET_BUFFER_SIZE = 65536;

    private const COM_REGISTER_SLAVE = 0x15;
    private const COM_BINLOG_DUMP    = 0x12;

    private const BINLOG_HEADER_SIZE = 4;

    private const MAX_PACKET_SIZE    = 0xffffff;

    private readonly Socket         $socket;
    private readonly BufferedReader $reader;
    private          ServerInfo     $serverInfo;
    private          int            $seqId = -1;
    private readonly string         $binlogFile;
    private readonly int            $binlogPosition;
    private readonly ?string        $cancellationId;

    public function __construct(
        private readonly Config          $config,
        private readonly LoggerInterface $logger,
        private readonly ?Cancellation   $cancellation = null,
    ) {
        $this->socket = connect(sprintf('tcp://%s:%d', $this->config->host, $this->config->port), cancellation: $cancellation);

        if (method_exists($this->socket, 'setChunkSize')) {
            $this->socket->setChunkSize(65536);
        }

        $this->reader = new BufferedReader($this->socket);
        $this->logger->debug('Connection established, starting handshake phase');

        $this->handleHandshake($this->readPacket());
        $this->logger->info(sprintf(
            'Connected to %s, protocol version: %d, connection id: %d, auth plugin: %s',
            $this->serverInfo->serverVersion,
            $this->serverInfo->protocolVersion,
            $this->serverInfo->connectionId,
            $this->serverInfo->authPluginName ?? 'NONE',
        ));

        $this->execute(sprintf('SET NAMES "%s" COLLATE "%s"', $this->config->collation->getCharset(), $this->config->collation->toString()));
        $this->validateServerConfiguration();
        $this->logger->info(sprintf('Master status is %s', json_encode($this->execute('SHOW MASTER STATUS'))));

        $this->binlogFile = $this->selectBinlogFile();
        $this->binlogPosition = $this->selectBinlogPosition();
        $this->logger->info(sprintf('Selected binlog file is %s:%d', $this->binlogFile, $this->binlogPosition));

        if ('NONE' !== $this->query('SELECT @@global.binlog_checksum AS value')[0]) {
            $this->execute('SET @master_binlog_checksum = @@global.binlog_checksum');
        }

        $this->execute(sprintf('SET @master_heartbeat_period = %f', $this->config->heartbeatPeriod * 1000000000));
    }

    public function getBinlogFile(): string
    {
        return $this->binlogFile;
    }

    public function getBinlogPosition(): int
    {
        return $this->binlogPosition;
    }

    private function validateServerConfiguration(): void
    {
        if ('ROW' !== ($this->query('SELECT @@global.binlog_format AS value')[0]['value'])) {
            throw new \RuntimeException('Expected to have binlog_format=ROW');
        }

        if ('FULL' !== ($this->query('SELECT @@global.binlog_row_image AS value')[0]['value'])) {
            throw new \RuntimeException('Expected to have binlog_row_image=FULL');
        }

        if ('FULL' !== ($this->query('SELECT @@global.binlog_row_metadata AS value')[0]['value'])) {
            throw new \RuntimeException('Expected to have binlog_row_metadata=FULL');
        }
    }

    private function selectBinlogFile(): string
    {
        $binlogFiles = array_column($this->query('SHOW BINARY LOGS'), 'File_size', 'Log_name');
        ksort($binlogFiles, SORT_NATURAL);

        if ($this->config->binlogFile) {
            if (!isset($binlogFiles[$this->config->binlogFile])) {
                throw new \UnexpectedValueException(sprintf(
                    'Binlog file %s is not found on server %s:%s',
                    $this->config->binlogFile,
                    $this->config->host,
                    $this->config->port,
                ));
            }
            return $this->config->binlogFile;
        } elseif (!count($binlogFiles)) {
            throw new \UnexpectedValueException(sprintf(
                'No binlog files were found on server %s:%s',
                $this->config->host,
                $this->config->port,
            ));
        } else {
            return array_key_first($binlogFiles);
        }
    }

    private function selectBinlogPosition(): int
    {
        $binlogPosition = $this->config->binlogPosition ?? static::BINLOG_HEADER_SIZE;

        $result = $this->execute(sprintf(
            'SHOW BINLOG EVENTS IN "%s" FROM %s LIMIT 1',
            addcslashes($this->binlogFile, '"'),
            $binlogPosition,
        ));

        $result[0]['Pos']
            ?? throw new \RuntimeException(sprintf('No events found in %s:%d', $this->binlogFile, $binlogPosition));

        return static::BINLOG_HEADER_SIZE;
    }

    private function registerSlave(): void
    {
        $this->seqId = -1;
        $payload = (new Buffer)
            ->writeUint8(static::COM_REGISTER_SLAVE)
            ->writeUInt32($this->config->slaveId)
            ->writeUint8(9)
            ->write('localhost')
            ->writeUint8(strlen($this->config->user))
            ->write($this->config->user)
            ->writeUint8(strlen($this->config->password))
            ->write($this->config->password)
            ->write($this->config->password)
            ->writeUInt16($this->config->port)
            ->writeUInt32(0)
            ->writeUInt32(0);

        $this->sendPacket($payload);

        if (Packet::OK !== ($response = $this->read())[0]) {
            throw new \RuntimeException('Unable to register slave');
        }

        $this->logger->info('Registered as slave successfully');
    }

    private function dumpBinlog(): void
    {
        $this->seqId = -1;

        $payload = (new Buffer)
            ->writeUint8(static::COM_BINLOG_DUMP)
            ->writeUint32($this->binlogPosition)
            ->writeUInt16(0)
            ->writeUInt32($this->config->slaveId)
            ->write($this->binlogFile);

        $this->sendPacket($payload);

        if (Packet::OK !== ($response = $this->read())[0]) {
            throw new \RuntimeException('Unable to register slave');
        }

        $this->logger->info(sprintf('Dumping binlog starting from %s:%d', $this->binlogFile, $this->binlogPosition));
    }

    public function getIterator(): Iterator
    {
        $this->registerSlave();
        $this->dumpBinlog();

        while (true) {
            if (Packet::EOF === ([$packet, $buffer] = $this->read())[1]) {
                continue;
            }

            yield $buffer;
        }
    }

    private function execute(string $query): bool|int|array
    {
        $this->seqId = -1;
        $this->sendPacket((new Buffer("\x03"))->write($query));

        [$packet, $buffer] = $response = $this->read(true);
        switch ($packet) {
            case Packet::OK:
                $affected = $response[1]->readCodedBinary();
                $insertId = $response[1]->readCodedBinary();
                $statusFlags = 0;
                $warnings = 0;
                if ($this->serverInfo->serverCapabilities & (Capability::PROTOCOL_41->value | Capability::TRANSACTIONS->value)) {
                    $statusFlags = $response[1]->readUint16();
                    $warnings = $response[1]->readUint16();
                }

                $this->logger->info(sprintf(
                    'Query OK "%s", affected: %d, insertId: %d, statusFlags: %d, warnings: %d',
                    $query,
                    $affected,
                    $insertId,
                    $statusFlags,
                    $warnings,
                ));
                return true;
                break;

            case Packet::EOF:
                $this->logger->warning(sprintf('Query ERR "%s"', $query));
                return false;
                break;
        }

        $columnCount = $buffer->readCodedBinary();
        $columnDefinitions = array_map(function (int $i) {
            [, $buffer] = $this->read(true);
            if ($this->serverInfo->serverCapabilities & Capability::PROTOCOL_41->value) {
                return [
                    'catalog' => $buffer->readVariableLengthString(),
                    'schema' => $buffer->readVariableLengthString(),
                    'table' => $buffer->readVariableLengthString(),
                    'originalTable' => $buffer->readVariableLengthString(),
                    'name' => $buffer->readVariableLengthString(),
                    'originalName' => $buffer->readVariableLengthString(),
                    'fixLength' => $fixLength = $buffer->readCodedBinary(),
                    'charset' => $buffer->readUint16(),
                    'length' => $buffer->readUint32(),
                    'type' => $buffer->readCodedBinary(),
                    'flags' => $buffer->readUint16(),
                    'decimals' => $buffer->getLeft() ? $buffer->readUint8() : null,
                    'skip' => $buffer->getLeft() ? $buffer->read($fixLength) : null,
                    'defaults' => $buffer->getLeft() ? $buffer->read() : null,
                ];
            } else {
                throw new \Exception('Not implemented yet');
                // $column["table"] = MysqlDataType::decodeString($packet, $offset);
                // $column["name"] = MysqlDataType::decodeString($packet, $offset);
                // $columnLength = MysqlDataType::decodeUnsigned($packet, $offset);
                // $column["length"] = MysqlDataType::decodeIntByLength($packet, $columnLength, $offset);
                // $typeLength = MysqlDataType::decodeUnsigned($packet, $offset);
                // $column["type"] = MysqlDataType::from(MysqlDataType::decodeIntByLength($packet, $typeLength, $offset));
                // $flagLength = $this->capabilities & self::CLIENT_LONG_FLAG
                    // ? MysqlDataType::decodeUnsigned($packet, $offset)
                    // : MysqlDataType::decodeUnsigned8($packet, $offset);
                // if ($flagLength > 2) {
                    // $column["flags"] = MysqlDataType::decodeUnsigned16($packet, $offset);
                // } else {
                    // $column["flags"] = MysqlDataType::decodeUnsigned8($packet, $offset);
                // }
                // $column["decimals"] = MysqlDataType::decodeUnsigned8($packet, $offset);
            }
        }, range(0, $columnCount - 1));

        if (Packet::EOF !== $this->read()[0]) {
            $this->logger->warning(sprintf('Query ERR after columns read "%s"', $query));
        }

        $result = [];
        for ($j = 0; Packet::EOF !== ([, $buffer] = $this->read(true))[0]; ++$j) {
            for ($i = 0; $i < $columnCount; ++$i) {
                if (0xfb === $buffer->readUint8()) {
                    $result[$j][$columnDefinitions[$i]['name']] = null;
                } else {
                    $value = $buffer->rewind(1)->readVariableLengthString();
                    $result[$j][$columnDefinitions[$i]['name']] = match (ColumnType::tryFrom($columnDefinitions[$i]['type'])) {
                        ColumnType::INT24,
                        ColumnType::SHORT,
                        ColumnType::TINY,
                        ColumnType::LONG,
                        ColumnType::YEAR => is_numeric($value) ? ((int) $value) : $value,
                        ColumnType::LONGLONG => (is_numeric($value) || $columnDefinitions[$i]['flags'] & 0x20) ? $value : ((int) $value),
                        ColumnType::DOUBLE,
                        ColumnType::FLOAT => is_numeric($value) ? ((float) $value) : $value,
                        default => $value,
                    };
                }
            }
        }

        $this->logger->debug(sprintf('Query OK "%s", resulted %d row(s)', $query, count($result)));

        return $result;
    }

    private function query(string $query): array
    {
        return !is_array($result = $this->execute($query))
            ? []
            : $result;
    }

    private function handleHandshake(Buffer $buffer): void
    {
        /** @psalm-suppress TooFewArguments */
        $this->serverInfo = new ServerInfo(
            $buffer->readUInt8(),
            $buffer->readUntill("\0"),
            $buffer->readUint32(),
            ...(function () use ($buffer) {
                $ret = [$buffer->read(8), $serverCapabilities = $buffer->skip(1)->readUint16()];

                if ($buffer->getLeft() > 0) {
                    $ret[] = $buffer->readUInt8();
                    $ret[] = $buffer->readUInt16();

                    $ret[1] = $serverCapabilities += $buffer->readUInt16() << 16;

                    $authPluginDataLen = Capability::PLUGIN_AUTH->in($serverCapabilities) ? $buffer->readUInt8() : (int) !(bool) $buffer->skip(1);
                    if (Capability::SECURE_CONNECTION->in($serverCapabilities)) {
                        $ret[0] .= $buffer->skip(10)->read(max(13, $authPluginDataLen - 8));
                        if (Capability::PLUGIN_AUTH->in($serverCapabilities)) {
                            $ret[] = $buffer->readUntill("\0");
                        }
                    }
                }
                return $ret + [null, null, null, null, null];
            })()
        );

        $capabilities = 0
            | Capability::LONG_PASSWORD->value
            | Capability::LONG_FLAG->value
            | Capability::PROTOCOL_41->value
            | Capability::NO_SCHEMA->value
            | Capability::TRANSACTIONS->value
            | Capability::SECURE_CONNECTION->value;

        $capabilities &= $this->serverInfo->serverCapabilities;

        $auth = (function (): string {
            if ('' === $this->config->password) {
                return '';
            }

            $hash = sha1($this->config->password, true);
            return $hash ^ sha1(\substr($this->serverInfo->authPluginData, 0, 20) . sha1($hash, true), true);
        })();

        $payload = (new Buffer)
            ->writeUInt32($capabilities)
            ->writeUInt32(static::MAX_PACKET_SIZE)
            ->writeUint8(33)
            ->write("\0", 23)
            ->write($this->config->user . "\0")
            ->writeUint8(strlen($auth))
            ->write($auth);

        $this->sendPacket($payload);

        if (Packet::OK !== ($response = $this->read())[0]) {
            throw new \RuntimeException('Handshake failed');
        }
    }

    private function readPacket(): Buffer
    {
        $data = '';
        $header = $this->reader->readLength(4, $this?->cancellation);
        /** @var int<1, max> $length */
        $length = \ord($header[0]) | (\ord($header[1]) << 8) | (\ord($header[2]) << 16);
        $this->seqId = \ord($header[3]);

        read: {
            $data .= $this->reader->readLength($length, $this?->cancellation);

            if (static::MAX_PACKET_SIZE === $length) {
                $header = $this->reader->readLength(4, $this?->cancellation);
                $length = \ord($header[0]) | (\ord($header[1]) << 8) | (\ord($header[2]) << 16);
                if (\ord($header[3]) !== ++$this->seqId) {
                    throw new \Exception('Got packets out of order');
                }

                goto read;
            }
        }

        return new Buffer($data);
    }

    public function read(bool $allowUnknownPacketTypes = false): array
    {
        $buffer = $this->readPacket();

        $packet = null;
        switch ($packet = Packet::tryFrom($buffer->readUint8())) {
            case Packet::OK:
            case Packet::EOF:
                return [$packet, $buffer];
                break;

            case Packet::ERR:
                throw new \Exception(code: $buffer->readUint16(), message: $buffer->read());
                break;

            default:
                if ($allowUnknownPacketTypes) {
                    return [$packet, $buffer->rewind()];
                }
                throw new \UnexpectedValueException(sprintf('Unknown type of packet "%s"', var_export($packet, true)));
                break;
        }

        return [$packet, $buffer];
    }

    private function sendPacket(Buffer $data): void
    {
        if ($data->getLength() > static::MAX_PACKET_SIZE) {
            throw new \Exception('Sending large packets is not implemented');
        }

        $this->socket->write(
            (string) (new Buffer)
                ->writeUint24($data->getLength())
                ->writeUint8(++$this->seqId)
                ->append($data)
        );
    }

    public function close(): void
    {
        if (!$this->socket->isClosed()) {
            try {
                $this->sendPacket(new Buffer("\x01"));
            } finally {
                $this->socket->close();
            }
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}
