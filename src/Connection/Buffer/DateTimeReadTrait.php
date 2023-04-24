<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use ValueError;

trait DateTimeReadTrait
{
    /** With jit enabled it is faster than date('Y-m-d H:i:s') */
    public function readTimestamp2(int $fsp): string
    {
        if (0 === $t = $this->readInt32Be()) {
            return '0000-00-00 00:00:00'
                . ($fsp > 0 ? sprintf('.%-03.3s', $this->readIntBeBySize(($fsp + 1) >> 1)) : '');
        }

        $second = $t % 60;
        $t = intdiv($t, 60);
        $minute = $t % 60;
        $t = intdiv($t, 60);
        $hour = $t % 24;
        $t = intdiv($t, 24);

        $a = intdiv(4 * $t + 102032, 146097) + 15;
        $b = $t + 2442113 + $a - intdiv($a, 4);
        $c = intdiv(20 * $b - 2442, 7305);
        $d = $b - 365 * $c - intdiv($c, 4);
        $e = intdiv($d * 1000, 30601);
        $f = ($d - $e * 30 - intdiv($e * 601, 1000));

        if ($e <= 13) {
            $c -= 4716;
            $e -= 1;
        } else {
            $c -= 4715;
            $e -= 13;
        }

        return \sprintf('%04d-%02d-%02d %02d:%02d:%02d', $c, $e, $f, $hour, $minute, $second)
            . ($fsp > 0 ? sprintf('.%-03.3s', $this->readIntBeBySize(($fsp + 1) >> 1)) : '');
    }

    public function readDate(): string
    {
        if (0 === $value = $this->readUInt24()) {
            return '0000-00-00';
        }

        return sprintf('%04d-%02d-%02d', ($value & ((1 << 15) - 1) << 9) >> 9, ($value & ((1 << 4) - 1) << 5) >> 5, ($value & ((1 << 5) - 1)));
    }

    public function readDateTime2(int $fsp): string
    {
        $yearMonth = (\ord($this->data[$this->offset + 2]) >> 6)
            + (\ord($this->data[$this->offset + 1]) << 2)
            + ((\ord($this->data[$this->offset]) & 0x7f) << 10);

        $value = sprintf(
            '%04d-%02d-%02d %02d:%02d:%02d',
            intdiv($yearMonth, 13),
            $yearMonth % 13,
            (\ord($this->data[$this->offset + 2]) & 0x3e) >> 1,
            ((\ord($this->data[$this->offset + 3]) & 0xf0) >> 4) + ((\ord($this->data[$this->offset + 2]) & 0x01) << 4),
            (\ord($this->data[$this->offset + 4]) >> 6) + ((\ord($this->data[$this->offset + 3]) & 0x0f) << 2),
            (\ord($this->data[$this->offset + 4]) & 0x3f),
        );

        $this->offset += 5;

        return $value . ($fsp > 0 ? sprintf('.%-03.3s', $this->readIntBeBySize(($fsp + 1) >> 1)) : '');
    }

    public function readTime2(int $fsp): string
    {
        $value = sprintf(
            '%02d:%02d:%02d',
            ((\ord($this->data[$this->offset + 1]) & 0xf0) >> 4) + ((\ord($this->data[$this->offset]) & 0x01) << 4),
            (\ord($this->data[$this->offset + 2]) >> 6) + ((\ord($this->data[$this->offset + 1]) & 0x0f) << 2),
            (\ord($this->data[$this->offset + 2]) & 0x3f),
        );

        $this->offset += 3;

        return $value . ($fsp > 0 ? sprintf('.%-03.3s', $this->readIntBeBySize(($fsp + 1) >> 1)) : '');
    }
}
