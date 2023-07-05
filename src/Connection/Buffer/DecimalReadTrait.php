<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection\Buffer;

use ValueError;

trait DecimalReadTrait
{
    public function readDecimal(int $precision, int $scale)
    {
        assert($precision > 0);
        assert($scale <= $precision);

        $digPerDec  = 9;
        $dig2bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

        $intg = $precision - $scale;
        $intg0 = intdiv($intg, $digPerDec);
        $frac0 = intdiv($scale, $digPerDec);
        $intg0x = $intg - ($intg0 * $digPerDec);
        $frac0x = $scale - ($frac0 * $digPerDec);

        $mask = (ord($this->data[$this->offset]) & 0x80) ? 0 : -1;
        $result = (ord($this->data[$this->offset]) & 0x80) ? '' : '-';

        $this->data[$this->offset] = chr(ord($this->data[$this->offset]) ^ 0x80);
        if ($dig2bytes[$intg0x]) {
            $result .= ($this->readIntBeBySize($dig2bytes[$intg0x]) ^ $mask);
        }

        for ($i = 0; $i < $intg0; ++$i) {
            $result .= sprintf('%09d', $this->readInt32Be() ^ $mask);
        }

        if ($scale > 0) {
            $result .= '.';

            for ($i = 0; $i < $frac0; ++$i) {
                $result .= sprintf('%09d', $this->readInt32Be() ^ $mask);
            }

            if ($dig2bytes[$frac0x] > 0) {
                $result .= sprintf("%0{$frac0x}d", $this->readIntBeBySize($dig2bytes[$frac0x]) ^ $mask);
            }
        }

        return sprintf("%.{$scale}F", $result);
    }
}
