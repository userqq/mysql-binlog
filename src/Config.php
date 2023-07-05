<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog;

use ValueError;
use Monolog\Level;
use UserQQ\MySQL\Binlog\Protocol\Collation;

final class Config
{
    private const DEFAULT_MYSQL_USER      = 'root';
    private const DEFAULT_MYSQL_PASSWORD  = '';
    private const DEFAULT_MYSQL_HOST      = '127.0.0.1';
    private const DEFAULT_MYSQL_PORT      = 3306;
    private const DEFAULT_MYSQL_SLAVE_ID  = 666;
    private const DEFAULT_MYSQL_COLLATION = Collation::UTF8MB4_GENERAL_CI;
    private const DEFAULT_LOG_LEVEL       = Level::Notice;

    public static function fromEnv(?Config $config = null): static
    {
        $config ??= new static();

        if (false !== $user = getenv('USER')) {
            $config = $config->withUser($user);
        }

        if (false !== $password = getenv('PASSWORD')) {
            $config = $config->withPassword($password);
        }

        if (false !== $host = getenv('HOST')) {
            $config = $config->withHost($host);
        }

        if (false !== $port = getenv('PORT')) {
            $config = $config->withPort((int) $port);
        }

        if (false !== $collation = getenv('COLLATION')) {
            $config = $config->withCollation($collation);
        }

        if (false !== $slaveId = getenv('SLAVE_ID')) {
            $config = $config->withSlaveId((int) $slaveId);
        }

        if (false !== $binlogFile = getenv('BINLOG_FILE')) {
            $config = $config->withBinlogFile($binlogFile);
        }

        if (false !== $binlogPosition = getenv('BINLOG_POSITION')) {
            $config = $config->withBinlogPosition((int) $binlogPosition);
        }

        if (false !== $tables = getenv('TABLES')) {
            $config = $config->withTables(array_map('trim', explode(',', $tables)));
        }

        if (false !== $excludeTables = getenv('EXCLUDE_TABLES')) {
            $config = $config->withExcludeTables(array_map('trim', explode(',', $excludeTables)));
        }

        if (false !== $databases = getenv('DATABASES')) {
            $config = $config->withDatabases(array_map('trim', explode(',', $databases)));
        }

        if (false !== $excludeDatabases = getenv('EXCLUDE_DATABASES')) {
            $config = $config->withExcludeDatabases(array_map('trim', explode(',', $excludeDatabases)));
        }

        if (false !== $heartbeatPeriod = getenv('HEARTBEAT_PERIOD')) {
            $config = $config->withHeartbeatPeriod((float) $heartbeatPeriod);
        }

        if (false !== $statisticsInterval = getenv('STATISTICS_INTERVAL')) {
            $config = $config->withStatisticsInterval((float) $statisticsInterval);
        }

        if (false !== $logLevel = getenv('LOG_LEVEL')) {
            $config = $config->withLogLevel($logLevel);
        }

        return $config;
    }

    public static function fromArgs(?Config $config = null): static
    {
        $config ??= new static();

        $options = getopt('', [
            'user:',
            'password:',
            'host:',
            'port:',
            'collation:',
            'slaveId:',
            'binlogFile:',
            'binlogPosition:',
            'tables:',
            'excludeTables:',
            'databases:',
            'excludeDatabases:',
            'heartbeatPeriod:',
            'statisticsInterval:',
            'logLevel:',
        ]);

        if (\array_key_exists('user', $options)) {
            $config = $config->withUser($options['user']);
        }

        if (\array_key_exists('password', $options)) {
            $config = $config->withPassword($options['password']);
        }

        if (\array_key_exists('host', $options)) {
            $config = $config->withHost($options['host']);
        }

        if (\array_key_exists('port', $options)) {
            $config = $config->withPort((int) $options['port']);
        }

        if (\array_key_exists('collation', $options)) {
            $config = $config->withCollation($options['collation']);
        }

        if (\array_key_exists('slaveId', $options)) {
            $config = $config->withSlaveId((int) $options['slaveId']);
        }

        if (\array_key_exists('binlogFile', $options)) {
            $config = $config->withBinlogFile($options['binlogFile']);
        }

        if (\array_key_exists('binlogPosition', $options)) {
            $config = $config->withBinlogPosition((int) $options['binlogPosition']);
        }

        if (\array_key_exists('tables', $options)) {
            $config = $config->withTables(array_map('trim', explode(',', $options['tables'])));
        }

        if (\array_key_exists('excludeTables', $options)) {
            $config = $config->withExcludeTables(array_map('trim', explode(',', $options['excludeTables'])));
        }

        if (\array_key_exists('databases', $options)) {
            $config = $config->withDatabases(array_map('trim', explode(',', $options['databases'])));
        }

        if (\array_key_exists('excludeDatabases', $options)) {
            $config = $config->withExcludeDatabases(array_map('trim', explode(',', $options['excludeDatabases'])));
        }

        if (\array_key_exists('heartbeatPeriod', $options)) {
            $config = $config->withHeartbeatPeriod((float) $options['heartbeatPeriod']);
        }

        if (\array_key_exists('statisticsInterval', $options)) {
            $config = $config->withStatisticsInterval((float) $options['statisticsInterval']);
        }

        if (\array_key_exists('logLevel', $options)) {
            $config = $config->withLogLevel($options['logLevel']);
        }

        return $config;
    }

    public static function build(): static
    {
        $config = new static();
        $config = static::fromEnv($config);
        $config = static::fromArgs($config);

        return $config;
    }

    public readonly Collation $collation;
    public readonly Level     $logLevel;

    public function __construct(
        public readonly string           $user               = self::DEFAULT_MYSQL_USER,
        public readonly string           $password           = self::DEFAULT_MYSQL_PASSWORD,
        public readonly string           $host               = self::DEFAULT_MYSQL_HOST,
        public readonly int              $port               = self::DEFAULT_MYSQL_PORT,
        public readonly int              $slaveId            = self::DEFAULT_MYSQL_SLAVE_ID,
        public readonly float            $heartbeatPeriod    = 1.0,
        public readonly ?float           $statisticsInterval = 1.0,
        public readonly ?string          $binlogFile         = null,
        public readonly ?int             $binlogPosition     = null,
        public readonly ?array           $tables             = null,
        public readonly ?array           $excludeTables      = null,
        public readonly ?array           $databases          = null,
        public readonly ?array           $excludeDatabases   = null,
                        string|Collation $collation          = self::DEFAULT_MYSQL_COLLATION,
                        string|Level     $logLevel           = self::DEFAULT_LOG_LEVEL,
    ) {
        $this->collation = !\is_string($collation)
            ? $collation
            : Collation::tryFromString($collation)
                ?? throw new ValueError('Unknown collation $collation');

        $this->logLevel = !\is_string($logLevel)
            ? $logLevel
            : Level::fromName($logLevel);

        $this->validate();
    }

    private function validate(): void
    {
        filter_var($this->user, FILTER_VALIDATE_REGEXP, ['options' => ['regexp' => '/.+/'], 'flags' => FILTER_NULL_ON_FAILURE])
            ?? throw new ValueError('Field "user" cannot be empty');

        filter_var($this->host, FILTER_VALIDATE_DOMAIN, ['flags' => FILTER_FLAG_HOSTNAME | FILTER_NULL_ON_FAILURE])
            ?? throw new ValueError(\sprintf('Field "host" should be valid domain or IP address, "%s" given', $this->host));

        filter_var($this->port, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'max_range' => 65535], 'flags' => FILTER_NULL_ON_FAILURE])
            ?? throw new ValueError(\sprintf('Field "port" should be a valid port number in range 1-65535, "%s" given', $this->port));

        filter_var($this->slaveId, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1], 'flags' => FILTER_NULL_ON_FAILURE])
            ?? throw new ValueError(\sprintf('Field "slaveId" should be a positive integer, "%s" given', $this->slaveId));

        null === $this->binlogFile
            ?: filter_var($this->binlogFile, FILTER_VALIDATE_REGEXP, ['options' => ['regexp' => '/^[a-zA-Z0-9\-]+\.\d+$/'], 'flags' => FILTER_NULL_ON_FAILURE])
                ?? throw new ValueError(\sprintf('Field "binlogFile" should be a valid filename, "%s" given', $this->binlogFile));

        null === $this->binlogPosition
            ?: filter_var($this->binlogPosition, FILTER_VALIDATE_INT, ['options' => ['min_range' => 4], 'flags' => FILTER_NULL_ON_FAILURE])
                ?? throw new ValueError(\sprintf('Field "binlogPosition" should be at least %d, "%s" given', 4, $this->binlogPosition));

        !(null !== $this->binlogPosition && null === $this->binlogFile)
                ?: throw new ValueError(\sprintf('Field "binlogPosition" is set to %d without "binlogFile"', $this->binlogPosition));

        null === $this->tables
            ?: !\count($errorNames = array_filter(
                $this->tables,
                fn ($name): bool => !\is_string($name)
                    || !filter_var($name, FILTER_VALIDATE_REGEXP, ['options' => [
                        'regexp' => '/^[\x{0001}-\x{007F}\x{0080}-\x{FFFF}]+\.[\x{0001}-\x{007F}\x{0080}-\x{FFFF}]+$/u'
                    ]])
            )) ?: throw new ValueError(\sprintf(
                'Only U+0001 .. U+007F and U+0080 .. U+FFFF characters in "{schema}.{table}" format are allowed in "tables", got %s',
                implode(', ', array_map(fn (mixed $name): string => var_export($name, true), $errorNames)),
            ));

        null === $this->excludeTables
            ?: !\count($errorNames = array_filter(
                $this->excludeTables,
                fn ($name): bool => !\is_string($name)
                    || !filter_var($name, FILTER_VALIDATE_REGEXP, ['options' => [
                        'regexp' => '/^[\x{0001}-\x{007F}\x{0080}-\x{FFFF}]+\.[\x{0001}-\x{007F}\x{0080}-\x{FFFF}]+$/u'
                    ]])
            )) ?: throw new ValueError(\sprintf(
                'Only U+0001 .. U+007F and U+0080 .. U+FFFF characters in "{schema}.{table}" format are allowed in "excludeTables", got %s',
                implode(', ', array_map(fn (mixed $name): string => var_export($name, true), $errorNames)),
            ));

        null === $this->databases
            ?: !\count($errorNames = array_filter(
                $this->databases,
                fn ($name): bool => !\is_string($name)
                    || !filter_var($name, FILTER_VALIDATE_REGEXP, ['options' => [
                        'regexp' => '/^[\x{0001}-\x{007F}\x{0080}-\x{FFFF}]+$/u'
                    ]])
            )) ?: throw new ValueError(\sprintf(
                'Only U+0001 .. U+007F and U+0080 .. U+FFFF characters in "{schema}" format are allowed in "databases", got %s',
                implode(', ', array_map(fn (mixed $name): string => var_export($name, true), $errorNames)),
            ));

        null === $this->excludeDatabases
            ?: !\count($errorNames = array_filter(
                $this->excludeDatabases,
                fn ($name): bool => !\is_string($name)
                    || !filter_var($name, FILTER_VALIDATE_REGEXP, ['options' => [
                        'regexp' => '/^[\x{0001}-\x{007F}\x{0080}-\x{FFFF}]+$/u'
                    ]])
            )) ?: throw new ValueError(\sprintf(
                'Only U+0001 .. U+007F and U+0080 .. U+FFFF characters in "{schema}" format are allowed in "excludeDatabases", got %s',
                implode(', ', array_map(fn (mixed $name): string => var_export($name, true), $errorNames)),
            ));

        null === $this->heartbeatPeriod
            ?: $result = filter_var($this->heartbeatPeriod, FILTER_VALIDATE_FLOAT, ['options' => ['min_range' => 0.001, 'max_range' => 4294967.0], 'flags' => FILTER_NULL_ON_FAILURE])
                ?? throw new ValueError(\sprintf('Field "heartbeatPeriod" should be a positive float in range %.3f-%.1f, "%s" given', 0.001, 4294967.0, $this->heartbeatPeriod));

        null === $this->statisticsInterval
            ?: $result = filter_var($this->statisticsInterval, FILTER_VALIDATE_FLOAT, ['options' => ['min_range' => 0.001, 'max_range' => 4294967.0], 'flags' => FILTER_NULL_ON_FAILURE])
                ?? throw new ValueError(\sprintf('Field "statisticsInterval" should be a positive float in range %.3f-%.1f, "%s" given', 0.001, 4294967.0, $this->statisticsInterval));
    }

    private function with(string $property, mixed $value): static
    {
        return new static(...[$property => $value] + get_object_vars($this));
    }

    public function withUser(string $user): static
    {
        return $this->with('user', $user);
    }

    public function withPassword(string $password): static
    {
        return $this->with('password', $password);
    }

    public function withHost(string $host): static
    {
        return $this->with('host', $host);
    }

    public function withPort(int $port): static
    {
        return $this->with('port', $port);
    }

    public function withCollation(string|Collation $collation): static
    {
        return $this->with('collation', $collation);
    }

    public function withSlaveId(?int $slaveId): static
    {
        return $this->with('slaveId', $slaveId);
    }

    public function withBinlogFile(?string $binlogFile): static
    {
        return $this->with('binlogFile', $binlogFile);
    }

    public function withBinlogPosition(?int $binlogPosition): static
    {
        return $this->with('binlogPosition', $binlogPosition);
    }

    public function withTables(?array $tables): static
    {
        return $this->with('tables', $tables);
    }

    public function withExcludeTables(?array $excludeTables): static
    {
        return $this->with('excludeTables', $excludeTables);
    }

    public function withDatabases(?array $databases): static
    {
        return $this->with('databases', $databases);
    }

    public function withExcludeDatabases(?array $excludeDatabases): static
    {
        return $this->with('excludeDatabases', $excludeDatabases);
    }

    public function withHeartbeatPeriod(float $heartbeatPeriod): static
    {
        return $this->with('heartbeatPeriod', $heartbeatPeriod);
    }

    public function withStatisticsInterval(?float $statisticsInterval): static
    {
        return $this->with('statisticsInterval', $statisticsInterval);
    }

    public function withLogLevel(string|Level $logLevel): static
    {
        return $this->with('logLevel', $logLevel);
    }
}
