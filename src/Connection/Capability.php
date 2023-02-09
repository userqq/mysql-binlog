<?php

declare(strict_types=1);

namespace UserQQ\MySQL\Binlog\Connection;

enum Capability: int
{
    /**
     * Use the improved version of Old Password Authentication.
     */
    case LONG_PASSWORD = 0x00000001;

    /**
     * Send found rows instead of affected rows in EOF_Packet.
     */
    case FOUND_ROWS = 0x00000002;

    /**
     * Get all column flags
     */
    case LONG_FLAG = 0x00000004;

    /**
     * Database (schema) name can be specified on connect in Handshake Response Packet.
     */
    case CONNECT_WITH_DB = 0x00000008;

    /**
     * Don't allow database.table.column.
     */
    case NO_SCHEMA = 0x00000010;

    /**
     * Compression protocol supported.
     */
    case COMPRESS = 0x00000020;

    /**
     * Special handling of ODBC behavior.
     */
    case ODBC = 0x00000040;

    /**
     * Can use LOAD DATA LOCAL.
     */
    case LOCAL_FILES = 0x00000080;

    /**
     * Ignore spaces before '('.
     */
    case IGNORE_SPACE = 0x00000100;

    /**
     * New 4.1 protocol.
     */
    case PROTOCOL_41 = 0x00000200;

    /**
     * This is an interactive client.
     */
    case INTERACTIVE = 0x00000400;

    /**
     * Use SSL encryption for the session.
     */
    case SSL = 0x00000800;

    /**
     * Client only flag.
     */
    case IGNORE_SIGPIPE = 0x00001000;

    /**
     * Client knows about transactions.
     */
    case TRANSACTIONS = 0x00002000;

    /**
     * DEPRECATED: Old flag for 4.1 protocol
     */
    case RESERVED = 0x00004000;

    /**
     * DEPRECATED: Old flag for 4.1 authentication.
     */
    case SECURE_CONNECTION = 0x00008000;

    /**
     * Enable/disable multi-stmt support.
     */
    case MULTI_STATEMENTS = 0x00010000;

    /**
     * Enable/disable multi-results.
     */
    case MULTI_RESULTS = 0x00020000;

    /**
     * Multi-results and OUT parameters in PS-protocol.
     */
    case PS_MULTI_RESULTS = 0x00040000;

    /**
     * Client supports plugin authentication.
     */
    case PLUGIN_AUTH = 0x00080000;

    /**
     * Client supports connection attributes.
     */
    case CONNECT_ATTRS = 0x00100000;

    /**
     * Enable authentication response packet to be larger than 255 bytes.
     */
    case PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

    /**
     * Don't close the connection for a user account with expired password.
     */
    case CAN_HANDLE_EXPIRED_PASSWORDS = 0x00400000;

    /**
     * Capable of handling server state change information.
     */
    case SESSION_TRACK = 0x00800000;

    /**
     * Client no longer needs EOF_Packet and will use OK_Packet instead.
     */
    case DEPRECATE_EOF = 0x01000000;

    /**
     * The client can handle optional metadata information in the resultset.
     */
    case OPTIONAL_RESULTSET_METADATA = 0x02000000;

    /**
     * Compression protocol extended to support zstd compression method.
     */
    case ZSTD_COMPRESSION_ALGORITHM = 0x04000000;

    /**
     * Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets.
     */
    case QUERY_ATTRIBUTES = 0x08000000;

    /**
     * Support Multi factor authentication.
     */
    case MULTI_FACTOR_AUTHENTICATION = 0x10000000;

    /**
     * This flag will be reserved to extend the 32bit capabilities structure to 64bits.
     */
    case CAPABILITY_EXTENSION = 0x20000000;

    /**
     * Verify server certificate.
     */
    case SSL_VERIFY_SERVER_CERT = 0x40000000;

    /**
     * Don't reset the options after an unsuccessful connect.
     */
    case REMEMBER_OPTIONS = 0x80000000;

    public function in(int $capabilities): bool
    {
        return (bool) ($capabilities & $this->value);
    }
}
