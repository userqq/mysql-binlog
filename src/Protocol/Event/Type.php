<?php declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Protocol\Event;

use JsonSerializable;

enum Type: int implements JsonSerializable
{
    case UNKNOWN_EVENT                         = 0x00;
    case START_EVENT_V3                        = 0x01;
    case QUERY_EVENT                           = 0x02;
    case STOP_EVENT                            = 0x03;
    case ROTATE_EVENT                          = 0x04;
    case INTVAR_EVENT                          = 0x05;
    case LOAD_EVENT                            = 0x06;
    case SLAVE_EVENT                           = 0x07;
    case CREATE_FILE_EVENT                     = 0x08;
    case APPEND_BLOCK_EVENT                    = 0x09;
    case EXEC_LOAD_EVENT                       = 0x0a;
    case DELETE_FILE_EVENT                     = 0x0b;
    case NEW_LOAD_EVENT                        = 0x0c;
    case RAND_EVENT                            = 0x0d;
    case USER_VAR_EVENT                        = 0x0e;
    case FORMAT_DESCRIPTION_EVENT              = 0x0f;
    case XID_EVENT                             = 0x10;
    case BEGIN_LOAD_QUERY_EVENT                = 0x11;
    case EXECUTE_LOAD_QUERY_EVENT              = 0x12;
    case TABLE_MAP_EVENT                       = 0x13;
    case WRITE_ROWS_EVENTv0                    = 0x14;
    case UPDATE_ROWS_EVENTv0                   = 0x15;
    case DELETE_ROWS_EVENTv0                   = 0x16;
    case WRITE_ROWS_EVENTv1                    = 0x17;
    case UPDATE_ROWS_EVENTv1                   = 0x18;
    case DELETE_ROWS_EVENTv1                   = 0x19;
    case INCIDENT_EVENT                        = 0x1a;
    case HEARTBEAT_EVENT                       = 0x1b;
    case IGNORABLE_EVENT                       = 0x1c;
    case ROWS_QUERY_EVENT                      = 0x1d;
    case WRITE_ROWS_EVENTv2                    = 0x1e;
    case UPDATE_ROWS_EVENTv2                   = 0x1f;
    case DELETE_ROWS_EVENTv2                   = 0x20;
    case GTID_EVENT                            = 0x21;
    case ANONYMOUS_GTID_EVENT                  = 0x22;
    case PREVIOUS_GTIDS_EVENT                  = 0x23;
    case MARIA_BINLOG_CHECKPOINT_EVENT         = 0xa1;
    case MARIA_GTID_EVENT                      = 0xa2;
    case MARIA_GTID_LIST_EVENT                 = 0xa3;
    case MARIA_START_ENCRYPTION_EVENT          = 0xa4;
    case MARIA_QUERY_COMPRESSED_EVENT          = 0xa5;
    case MARIA_WRITE_ROWS_COMPRESSED_EVENT_V1  = 0xa6;
    case MARIA_UPDATE_ROWS_COMPRESSED_EVENT_V1 = 0xa7;
    case MARIA_DELETE_ROWS_COMPRESSED_EVENT_V1 = 0xa8;
    case MARIA_WRITE_ROWS_COMPRESSED_EVENT     = 0xa9;
    case MARIA_UPDATE_ROWS_COMPRESSED_EVENT    = 0xaa;
    case MARIA_DELETE_ROWS_COMPRESSED_EVENT    = 0xab;

    private const CASES = [
        0x00 => 'UNKNOWN_EVENT',
        0x01 => 'START_EVENT_V3',
        0x02 => 'QUERY_EVENT',
        0x03 => 'STOP_EVENT',
        0x04 => 'ROTATE_EVENT',
        0x05 => 'INTVAR_EVENT',
        0x06 => 'LOAD_EVENT',
        0x07 => 'SLAVE_EVENT',
        0x08 => 'CREATE_FILE_EVENT',
        0x09 => 'APPEND_BLOCK_EVENT',
        0x0a => 'EXEC_LOAD_EVENT',
        0x0b => 'DELETE_FILE_EVENT',
        0x0c => 'NEW_LOAD_EVENT',
        0x0d => 'RAND_EVENT',
        0x0e => 'USER_VAR_EVENT',
        0x0f => 'FORMAT_DESCRIPTION_EVENT',
        0x10 => 'XID_EVENT',
        0x11 => 'BEGIN_LOAD_QUERY_EVENT',
        0x12 => 'EXECUTE_LOAD_QUERY_EVENT',
        0x13 => 'TABLE_MAP_EVENT',
        0x14 => 'WRITE_ROWS_EVENTv0',
        0x15 => 'UPDATE_ROWS_EVENTv0',
        0x16 => 'DELETE_ROWS_EVENTv0',
        0x17 => 'WRITE_ROWS_EVENTv1',
        0x18 => 'UPDATE_ROWS_EVENTv1',
        0x19 => 'DELETE_ROWS_EVENTv1',
        0x1a => 'INCIDENT_EVENT',
        0x1b => 'HEARTBEAT_EVENT',
        0x1c => 'IGNORABLE_EVENT',
        0x1d => 'ROWS_QUERY_EVENT',
        0x1e => 'WRITE_ROWS_EVENTv2',
        0x1f => 'UPDATE_ROWS_EVENTv2',
        0x20 => 'DELETE_ROWS_EVENTv2',
        0x21 => 'GTID_EVENT',
        0x22 => 'ANONYMOUS_GTID_EVENT',
        0x23 => 'PREVIOUS_GTIDS_EVENT',
        0xa1 => 'MARIA_BINLOG_CHECKPOINT_EVENT',
        0xa2 => 'MARIA_GTID_EVENT',
        0xa3 => 'MARIA_GTID_LIST_EVENT',
        0xa4 => 'MARIA_START_ENCRYPTION_EVENT',
        0xa5 => 'MARIA_QUERY_COMPRESSED_EVENT',
        0xa6 => 'MARIA_WRITE_ROWS_COMPRESSED_EVENT_V1',
        0xa7 => 'MARIA_UPDATE_ROWS_COMPRESSED_EVENT_V1',
        0xa8 => 'MARIA_DELETE_ROWS_COMPRESSED_EVENT_V1',
        0xa9 => 'MARIA_WRITE_ROWS_COMPRESSED_EVENT',
        0xaa => 'MARIA_UPDATE_ROWS_COMPRESSED_EVENT',
        0xab => 'MARIA_DELETE_ROWS_COMPRESSED_EVENT',
    ];

    public static function fromString(string $name): static
    {
        return static::tryFromString($name)
            ?? throw new \InvalidArgumentException(sprintf('Unknown event type: %s', $name));
    }

    public static function tryFromString(string $name): ?static
    {
        return $found = array_search($name, static::CASES, true)
            ? static::from($found)
            : null;
    }

    public function toString(): string
    {
        return self::CASES[$this->value];
    }

    public function jsonSerialize(): mixed
    {
        return $this->toString();
    }
}
