<?php
declare(strict_types=1);

namespace Wwwision\DCBLibraryDoctrine;

use Closure;
use DateTimeImmutable;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Psr\Clock\ClockInterface;
use Wwwision\DCBEventStore\Types\SequenceNumber;
use Wwwision\DCBLibrary\ProvidesSetup;
use Wwwision\DCBLibrary\Subscription\RunMode;
use Wwwision\DCBLibrary\Subscription\Status;
use Wwwision\DCBLibrary\Subscription\Store\SubscriptionCriteria;
use Wwwision\DCBLibrary\Subscription\Store\SubscriptionStore;
use Wwwision\DCBLibrary\Subscription\Subscription;
use Wwwision\DCBLibrary\Subscription\SubscriptionError;
use Wwwision\DCBLibrary\Subscription\SubscriptionGroup;
use Wwwision\DCBLibrary\Subscription\SubscriptionId;
use Wwwision\DCBLibrary\Subscription\Subscriptions;
use Wwwision\Types\Parser;
use Wwwision\Types\Schema\StringSchema;
use function Wwwision\Types\instantiate;

final class DoctrineSubscriptionStore implements SubscriptionStore, ProvidesSetup
{
    public function __construct(
        private string $tableName,
        private readonly Connection $dbal,
        private readonly ClockInterface $clock,
    ) {
    }

    public function setup(): void
    {
        $schemaConfig = $this->dbal->getSchemaManager()->createSchemaConfig();
        assert($schemaConfig !== null);
        $schemaConfig->setDefaultTableOptions([
            'charset' => 'utf8mb4'
        ]);
        $isSqlite = $this->dbal->getDatabasePlatform() instanceof SqlitePlatform;
        $tableSchema = new Table($this->tableName, [
            (new Column('id', Type::getType(Types::STRING)))->setNotnull(true)->setLength(self::maxLength(SubscriptionId::class))->setCustomSchemaOption('charset', 'ascii')->setCustomSchemaOption('collation', $isSqlite ? null : 'ascii_general_ci'),
            (new Column('group_name', Type::getType(Types::STRING)))->setNotnull(true)->setLength(100)->setCustomSchemaOption('charset', 'ascii')->setCustomSchemaOption('collation', $isSqlite ? null : 'ascii_general_ci'),
            (new Column('run_mode', Type::getType(Types::STRING)))->setNotnull(true)->setLength(16)->setCustomSchemaOption('charset', 'ascii')->setCustomSchemaOption('collation', $isSqlite ? null : 'ascii_general_ci'),
            (new Column('position', Type::getType(Types::INTEGER)))->setNotnull(true),
            (new Column('locked', Type::getType(Types::BOOLEAN)))->setNotnull(true),
            (new Column('status', Type::getType(Types::STRING)))->setNotnull(true)->setLength(32)->setCustomSchemaOption('charset', 'ascii')->setCustomSchemaOption('collation', $isSqlite ? null : 'ascii_general_ci'),
            (new Column('error_message', Type::getType(Types::TEXT)))->setNotnull(false),
            (new Column('error_previous_status', Type::getType(Types::STRING)))->setNotnull(false)->setLength(32)->setCustomSchemaOption('charset', 'ascii')->setCustomSchemaOption('collation', $isSqlite ? null : 'ascii_general_ci'),
            (new Column('error_trace', Type::getType(Types::TEXT)))->setNotnull(false),
            (new Column('retry_attempt', Type::getType(Types::INTEGER)))->setNotnull(true),
            (new Column('last_saved_at', Type::getType(Types::DATETIME_IMMUTABLE)))->setNotnull(true),
        ]);
        $tableSchema->setPrimaryKey(['id']);
        $tableSchema->addIndex(['group_name']);
        $tableSchema->addIndex(['status']);
        $schema = new Schema(
            [$tableSchema],
            [],
            $schemaConfig,
        );
        foreach (DbalSchemaDiff::determineRequiredSqlStatements($this->dbal, $schema) as $statement) {
            $this->dbal->executeStatement($statement);
        }
    }

    public function findOneById(SubscriptionId $subscriptionId): ?Subscription
    {
        $row = $this->dbal->fetchAssociative('SELECT * FROM ' . $this->tableName . ' WHERE id = :subscriptionId', ['subscriptionId' => $subscriptionId->value]);
        if ($row === false) {
            return null;
        }
        return self::fromDatabase($row);
    }

    public function findByCriteria(SubscriptionCriteria $criteria): Subscriptions
    {
        $queryBuilder = $this->dbal->createQueryBuilder()
            ->select('*')
            ->from($this->tableName)
            ->orderBy('id');
        if ($criteria->ids !== null) {
            $queryBuilder->andWhere('id IN (:ids)')
                ->setParameter(
                    'ids',
                    $criteria->ids->toStringArray(),
                    Connection::PARAM_STR_ARRAY,
                );
        }
        if ($criteria->groups !== null) {
            $queryBuilder->andWhere('group_name IN (:groups)')
                ->setParameter(
                    'groups',
                    $criteria->groups->toStringArray(),
                    Connection::PARAM_STR_ARRAY,
                );
        }
        if ($criteria->status !== null) {
            $queryBuilder->andWhere('status IN (:status)')
                ->setParameter(
                    'status',
                    array_map(static fn (Status $status) => $status->name, $criteria->status),
                    Connection::PARAM_STR_ARRAY,
                );
        }
        $result = $queryBuilder->execute();
        assert($result instanceof Result);
        $rows = $result->fetchAllAssociative();
        if ($rows === []) {
            return Subscriptions::none();
        }
        return Subscriptions::fromArray(array_map(self::fromDatabase(...), $rows));
    }

    public function acquireLock(SubscriptionId $subscriptionId): bool
    {
        $data = [
            'locked' => 1,
            'last_saved_at' => $this->clock->now()->format('Y-m-d H:i:s'),
        ];
        $acquired = $this->dbal->update($this->tableName, $data, [
            'id' => $subscriptionId->value,
            'locked' => 0,
        ]);
        return $acquired >= 1;
    }

    public function releaseLock(SubscriptionId $subscriptionId): void
    {
        $data = [
            'locked' => 0,
            'last_saved_at' => $this->clock->now()->format('Y-m-d H:i:s'),
        ];
        $this->dbal->update($this->tableName, $data, ['id' => $subscriptionId->value]);
    }

    public function add(Subscription $subscription): void
    {
        $row = self::toDatabase($subscription);
        $row['id'] = $subscription->id->value;
        $row['locked'] = 0;
        $row['last_saved_at'] = $this->clock->now()->format('Y-m-d H:i:s');
        $this->dbal->insert(
            $this->tableName,
            $row,
        );
    }

    public function update(SubscriptionId $subscriptionId, Closure $updater): void
    {
        $subscription = $this->findOneById($subscriptionId);
        if ($subscription === null) {
            throw new \InvalidArgumentException(sprintf('Failed to update subscription with id "%s" because it does not exist', $subscriptionId->value), 1721672347);
        }
        /** @var Subscription $subscription */
        $subscription = $updater($subscription);
        $row = self::toDatabase($subscription);
        $row['last_saved_at'] = $this->clock->now()->format('Y-m-d H:i:s');
        $this->dbal->update(
            $this->tableName,
            $row,
            [
                'id' => $subscriptionId->value,
            ]
        );
    }

    /**
     * @param class-string $type
     */
    private static function maxLength(string $type): int
    {
        $schema = Parser::getSchema($type);
        if (!$schema instanceof StringSchema) {
            throw new \InvalidArgumentException(sprintf('Only %s types are supported, got: %s for type "%s"', StringSchema::class, get_debug_type($schema), $type), 1721671411);
        }
        if ($schema->maxLength === null) {
            throw new \RuntimeException(sprintf('maxLength restriction is not set for type %s', $type), 1723636955);
        }
        return $schema->maxLength;
    }

    /**
     * @return array<string, mixed>
     */
    private static function toDatabase(Subscription $subscription): array
    {
        return [
            'group_name' => $subscription->group->value,
            'run_mode' => $subscription->runMode->name,
            'status' => $subscription->status->name,
            'position' => $subscription->position->value,
            'error_message' => $subscription->error?->errorMessage,
            'error_previous_status' => $subscription->error?->previousStatus?->name,
            'error_trace' => $subscription->error?->errorTrace,
            'retry_attempt' => $subscription->retryAttempt,
        ];
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function fromDatabase(array $row): Subscription
    {
        if (isset($row['error_message'])) {
            assert(is_string($row['error_message']));
            assert(!isset($row['error_previous_status']) || is_string($row['error_previous_status']));
            assert(is_string($row['error_trace']));
            $subscriptionError = new SubscriptionError($row['error_message'], instantiate(Status::class, $row['error_previous_status']), $row['error_trace']);
        } else {
            $subscriptionError = null;
        }
        assert(is_string($row['id']));
        assert(is_string($row['group_name']));
        assert(is_string($row['run_mode']));
        assert(is_string($row['status']));
        assert(is_int($row['position']));
        assert(is_int($row['locked']));
        assert(is_int($row['retry_attempt']));
        assert(is_string($row['last_saved_at']));
        $lastSavedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['last_saved_at']);
        assert($lastSavedAt instanceof DateTimeImmutable);

        return new Subscription(
            SubscriptionId::fromString($row['id']),
            instantiate(SubscriptionGroup::class, $row['group_name']),
            instantiate(RunMode::class, $row['run_mode']),
            instantiate(Status::class, $row['status']),
            SequenceNumber::fromInteger($row['position']),
            (bool)$row['locked'],
            $subscriptionError,
            $row['retry_attempt'],
            $lastSavedAt,
        );
    }
}
