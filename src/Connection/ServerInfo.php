<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection;

final class ServerInfo
{
    public function __construct(
        public readonly int     $protocolVersion,
        public readonly string  $serverVersion,
        public readonly int     $connectionId,
        public readonly string  $authPluginData,
        public readonly int     $serverCapabilities,
        public readonly int     $charset,
        public readonly int     $statusFlags,
        public readonly ?string $authPluginName,
    ) {}
}
