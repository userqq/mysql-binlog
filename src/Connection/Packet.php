<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection;

enum Packet: int
{
    case OK                   = 0x00;
    case EXTRA_AUTH           = 0x01;
    case LOCAL_INFILE_REQUEST = 0xfb;
    case EOF                  = 0xfe;
    case ERR                  = 0xff;
}
