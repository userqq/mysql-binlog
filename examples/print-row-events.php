#!/usr/local/bin/php
<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

error_reporting(E_ALL & ~E_DEPRECATED & ~E_USER_DEPRECATED);

use Amp\ByteStream;
use Amp\DeferredCancellation;
use Amp\Log\ConsoleFormatter;
use Amp\Log\StreamHandler;
use Monolog\ErrorHandler;
use Monolog\Logger;
use Monolog\Level;
use Revolt\EventLoop;
use UserQQ\MySQL\Binlog\Config;
use UserQQ\MySQL\Binlog\EventsIterator;
use function Amp\trapSignal;

$config = Config::build();

$cancellation = new DeferredCancellation();
$logger = new Logger('event-stream', [
    (new StreamHandler(ByteStream\getStderr(), $config->logLevel))
        ->setFormatter(new ConsoleFormatter(ConsoleFormatter::DEFAULT_FORMAT, 'H:i:s.u'))
]);

ErrorHandler::register($logger);
EventLoop::setErrorHandler(function (Throwable $t) use ($logger, $cancellation): void {
    $logger->emergency($t);
    $cancellation->cancel($t);
});

$eventStream = new EventsIterator($config, $logger, $cancellation->getCancellation());

$currentPosition = null;
EventLoop::queue(function () use ($eventStream, &$currentPosition): void {
    foreach ($eventStream as $position => $event) {
        echo json_encode($event, JSON_PRETTY_PRINT) . PHP_EOL;
        $currentPosition = $position;
    }
});

$signal = trapSignal([
    SIGHUP, SIGINT, SIGQUIT, SIGILL, SIGTRAP, SIGABRT, SIGBUS, SIGFPE, SIGUSR1, SIGSEGV, SIGUSR2, SIGPIPE, SIGALRM, SIGTERM, SIGSTKFLT, SIGCLD, SIGCONT, SIGTSTP, SIGTTIN, SIGTTOU, SIGURG, SIGXCPU, SIGXFSZ, SIGVTALRM, SIGPROF, SIGPOLL, SIGIO, SIGPWR, SIGSYS
], true, $cancellation->getCancellation());

$logger->notice(sprintf('Received signal %d, stopping event queue', $signal));
$logger->notice(sprintf('Stopped at position %s', json_encode($currentPosition)));
