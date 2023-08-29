<?php
declare(strict_types=1);

namespace Wwwision\DCBLibraryDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\MySqlPlatform;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Types;
use Wwwision\DCBEventStore\Types\SequenceNumber;
use Wwwision\DCBLibrary\CheckpointStorage;
use Wwwision\DCBLibrary\Exceptions\CheckpointException;
use Wwwision\DCBLibrary\ProvidesSetup;

final class DbalCheckpointStorage implements CheckpointStorage, ProvidesSetup
{
    private MySqlPlatform|PostgreSQLPlatform $platform;
    private int|null $lockedSequenceNumber = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly string $tableName,
        private readonly string $subscriberId,
    ) {
        $platform = $this->connection->getDatabasePlatform();
        if (!($platform instanceof MySqlPlatform || $platform instanceof PostgreSqlPlatform)) {
            throw new \InvalidArgumentException(sprintf('The %s only supports the platforms %s and %s currently. Given: %s', $this::class, MySqlPlatform::class, PostgreSqlPlatform::class, get_debug_type($platform)), 1691422911);
        }
        $this->platform = $platform;
    }

    public function acquireLock(): ?SequenceNumber
    {
        if ($this->connection->isTransactionActive()) {
            throw new CheckpointException(sprintf('Failed to acquire checkpoint lock for subscriber "%s" because a transaction is active already', $this->subscriberId), 1691422908);
        }
        $this->connection->beginTransaction();
        try {
            $highestAppliedSequenceNumber = $this->connection->fetchOne('SELECT sequence_number FROM ' . $this->connection->quoteIdentifier($this->tableName) . ' WHERE subscriber = :subscriberId ' . $this->platform->getForUpdateSQL() . ' NOWAIT', [
                'subscriberId' => $this->subscriberId
            ]);
        } catch (LockWaitTimeoutException $exception) {
            $this->connection->rollBack();
            throw new CheckpointException(sprintf('Failed to acquire checkpoint lock for subscriber "%s" because it is acquired already', $this->subscriberId), 1691422918);
        } catch (DBALException $exception) {
            $this->connection->rollBack();
            throw new \RuntimeException($exception->getMessage(), 1544207778, $exception);
        }
        if (!is_numeric($highestAppliedSequenceNumber)) {
            $this->connection->rollBack();
            throw new CheckpointException(sprintf('Failed to fetch highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1691422916);
        }
        $this->lockedSequenceNumber = (int)$highestAppliedSequenceNumber;
        if ($this->lockedSequenceNumber === 0) {
            return null;
        }
        return SequenceNumber::fromInteger($this->lockedSequenceNumber);
    }

    public function updateAndReleaseLock(SequenceNumber $sequenceNumber): void
    {
        if ($this->lockedSequenceNumber === null) {
            throw new CheckpointException(sprintf('Failed to update and commit checkpoint for subscriber "%s" because the lock has not been acquired successfully before', $this->subscriberId), 1691422923);
        }
        if (!$this->connection->isTransactionActive()) {
            throw new CheckpointException(sprintf('Failed to update and commit checkpoint for subscriber "%s" because no transaction is active', $this->subscriberId), 1691422925);
        }
        try {
            if ($sequenceNumber->value !== $this->lockedSequenceNumber) {
                $this->connection->update($this->tableName, ['sequence_number' => $sequenceNumber->value], ['subscriber' => $this->subscriberId]);
            }
            $this->connection->commit();
        } catch (DBALException $exception) {
            $this->connection->rollBack();
            throw new CheckpointException(sprintf('Failed to update and commit highest applied sequence number for subscriber "%s". Please run %s::setup()', $this->subscriberId, $this::class), 1691422928, $exception);
        } finally {
            $this->lockedSequenceNumber = null;
        }
    }

    public function setup(): void
    {
        $schemaManager = $this->connection->getSchemaManager();
        assert($schemaManager !== null);
        $schema = new Schema();
        $table = $schema->createTable($this->tableName);
        $table->addColumn('subscriber', Types::STRING, ['length' => 255]);
        $table->addColumn('sequence_number', Types::INTEGER);
        $table->setPrimaryKey(['subscriber']);

        $schemaDiff = (new Comparator())->compare($schemaManager->createSchema(), $schema);
        foreach ($schemaDiff->toSaveSql($this->platform) as $statement) {
            $this->connection->executeStatement($statement);
        }
        try {
            $this->connection->insert($this->tableName, ['subscriber' => $this->subscriberId, 'sequence_number' => 0]);
        } catch (UniqueConstraintViolationException $e) {
            // table and row already exists, ignore
        }
    }

    public function reset(): void
    {
        $this->connection->update($this->tableName, ['sequence_number' => 0], ['subscriber' => $this->subscriberId]);
    }
}
