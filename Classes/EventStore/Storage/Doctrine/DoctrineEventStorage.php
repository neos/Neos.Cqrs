<?php
declare(strict_types=1);
namespace Neos\EventSourcing\EventStore\Storage\Doctrine;

/*
 * This file is part of the Neos.EventSourcing package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ConnectionException;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaConfig;
use Doctrine\DBAL\Types\Types;
use Neos\Error\Messages\Error;
use Neos\Error\Messages\Notice;
use Neos\Error\Messages\Result;
use Neos\Error\Messages\Warning;
use Neos\EventSourcing\EventStore\EventNormalizer;
use Neos\EventSourcing\EventStore\EventStream;
use Neos\EventSourcing\EventStore\Exception\ConcurrencyException;
use Neos\EventSourcing\EventStore\ExpectedVersion;
use Neos\EventSourcing\EventStore\Storage\Doctrine\Factory\ConnectionFactory;
use Neos\EventSourcing\EventStore\Storage\EventStorageInterface;
use Neos\EventSourcing\EventStore\StreamName;
use Neos\EventSourcing\EventStore\WritableEvent;
use Neos\EventSourcing\EventStore\WritableEvents;

/**
 * Database event storage adapter
 */
class DoctrineEventStorage implements EventStorageInterface
{
    private const DEFAULT_EVENT_TABLE_NAME = 'neos_eventsourcing_eventstore_events';

    /**
     * @var EventNormalizer
     */
    protected $eventNormalizer;

    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var string
     */
    private $eventTableName;

    /**
     * @param array $options
     * @param EventNormalizer $eventNormalizer
     * @param Connection $connection
     */
    public function __construct(array $options, EventNormalizer $eventNormalizer, Connection $connection)
    {
        $this->eventTableName = $options['eventTableName'] ?? self::DEFAULT_EVENT_TABLE_NAME;
        $this->eventNormalizer = $eventNormalizer;
        if (isset($options['backendOptions'])) {
            $factory = new ConnectionFactory();
            try {
                $this->connection = $factory->create($options);
            } catch (DBALException $e) {
                throw new \InvalidArgumentException('Failed to create DBAL connection for the given options', 1592381267, $e);
            }
        } else {
            $this->connection = $connection;
        }
    }

    /**
     * @inheritdoc
     */
    public function load(StreamName $streamName, int $minimumSequenceNumber = 0): EventStream
    {
        $this->reconnectDatabaseConnection();
        $query = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->eventTableName)
            ->orderBy('sequencenumber', 'ASC');

        if (!$streamName->isVirtualStream()) {
            $query->andWhere('stream = :streamName');
            $query->setParameter('streamName', (string)$streamName);
        } elseif ($streamName->isCategoryStream()) {
            $query->andWhere('stream LIKE :streamNamePrefix');
            $query->setParameter('streamNamePrefix', $streamName->getCategoryName() . '%');
        } elseif ($streamName->isCorrelationIdStream()) {
            $query->andWhere('correlationIdentifier LIKE :correlationId');
            $query->setParameter('correlationId', $streamName->getCorrelationId());
        } elseif (!$streamName->isAllStream()) {
            throw new \InvalidArgumentException(sprintf('Unsupported virtual stream name "%s"', $streamName), 1545155909);
        }
        if ($minimumSequenceNumber > 0) {
            $query->andWhere('sequencenumber >= :minimumSequenceNumber');
            $query->setParameter('minimumSequenceNumber', $minimumSequenceNumber);
        }

        $streamIterator = new DoctrineStreamIterator($query);
        return new EventStream($streamName, $streamIterator, $this->eventNormalizer);
    }

    /**
     * @inheritdoc
     * @throws DBALException | ConcurrencyException | \Throwable
     */
    public function commit(StreamName $streamName, WritableEvents $events, int $expectedVersion = ExpectedVersion::ANY): void
    {
        if ($streamName->isVirtualStream()) {
            throw new \InvalidArgumentException(sprintf('Can\'t commit to virtual stream "%s"', $streamName), 1540632984);
        }

        # Exponential backoff: initial interval = 5ms and 25 retry attempts = max 2360ms (= 2,36 seconds)
        # @see http://backoffcalculator.com/?attempts=25&interval=0.005&rate=1.2
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 25;
        $retryAttempt = 0;
        while (true) {
            $this->reconnectDatabaseConnection();
            if ($this->connection->getTransactionNestingLevel() > 0) {
                throw new \RuntimeException('A transaction is active already, can\'t commit events!', 1547829131);
            }
            $this->connection->beginTransaction();
            try {
                $actualVersion = $this->getStreamVersion($streamName);
                $this->verifyExpectedVersion($actualVersion, $expectedVersion);
                foreach ($events as $event) {
                    $actualVersion++;
                    $this->commitEvent($streamName, $event, $actualVersion);
                }
            } catch (UniqueConstraintViolationException $exception) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    $this->connection->rollBack();
                    throw new ConcurrencyException(sprintf('Failed after %d retry attempts', $retryAttempt), 1573817175, $exception);
                }
                usleep((int)($retryWaitInterval * 1E6));
                $retryAttempt++;
                $retryWaitInterval *= 1.2;
                $this->connection->rollBack();
                continue;
            } catch (DBALException | ConcurrencyException $exception) {
                $this->connection->rollBack();
                throw $exception;
            }
            $this->connection->commit();
            break;
        }
    }

    /**
     * @param StreamName $streamName
     * @param WritableEvent $event
     * @param int $version
     * @throws DBALException | UniqueConstraintViolationException
     */
    private function commitEvent(StreamName $streamName, WritableEvent $event, int $version): void
    {
        $metadata = $event->getMetadata();
        $this->connection->insert(
            $this->eventTableName,
            [
                'id' => $event->getIdentifier(),
                'stream' => (string)$streamName,
                'version' => $version,
                'type' => $event->getType(),
                'payload' => json_encode($event->getData(), JSON_PRETTY_PRINT),
                'metadata' => json_encode($metadata, JSON_PRETTY_PRINT),
                'correlationidentifier' => $metadata['correlationIdentifier'] ?? null,
                'causationidentifier' => $metadata['causationIdentifier'] ?? null,
                'recordedat' => new \DateTimeImmutable('now'),
            ],
            [
                'version' => \PDO::PARAM_INT,
                'recordedat' => Types::DATETIME_IMMUTABLE,
            ]
        );
    }

    /**
     * @param StreamName $streamName
     * @return int
     */
    private function getStreamVersion(StreamName $streamName): int
    {
        $version = $this->connection->createQueryBuilder()
            ->select('MAX(version)')
            ->from($this->eventTableName)
            ->where('stream = :streamName')
            ->setParameter('streamName', (string)$streamName)
            ->execute()
            ->fetchColumn();
        return $version !== null ? (int)$version : -1;
    }

    /**
     * @param int $actualVersion
     * @param int $expectedVersion
     * @throws ConcurrencyException | ConnectionException
     */
    private function verifyExpectedVersion(int $actualVersion, int $expectedVersion): void
    {
        if ($expectedVersion === ExpectedVersion::ANY) {
            return;
        }
        if ($expectedVersion === $actualVersion || ($expectedVersion === ExpectedVersion::STREAM_EXISTS && $actualVersion > -1)) {
            return;
        }
        throw new ConcurrencyException(sprintf('Expected version: %s, actual version: %s', $this->renderExpectedVersion($expectedVersion), $this->renderExpectedVersion($actualVersion)), 1477143473);
    }

    /**
     * @param int $expectedVersion
     * @return string
     */
    private function renderExpectedVersion(int $expectedVersion): string
    {
        $labels = [
            ExpectedVersion::ANY => 'ANY (-2)',
            ExpectedVersion::NO_STREAM => 'NO STREAM (-1)',
            ExpectedVersion::STREAM_EXISTS => 'STREAM EXISTS (-4)',
        ];
        return $labels[$expectedVersion] ?? (string)$expectedVersion;
    }

    /**
     * @inheritdoc
     */
    public function getStatus(): Result
    {
        $result = new Result();
        $schemaManager = $this->connection->getSchemaManager();
        if ($schemaManager === null) {
            $result->addError(new Error('Failed to retrieve Schema Manager', 1592381724, [], 'Connection failed'));
            return $result;
        }
        try {
            $tableExists = $schemaManager->tablesExist([$this->eventTableName]);
        } catch (ConnectionException $exception) {
            $result->addError(new Error($exception->getMessage(), $exception->getCode(), [], 'Connection failed'));
            return $result;
        }
        $result->addNotice(new Notice((string)$this->connection->getHost(), null, [], 'Host'));
        $result->addNotice(new Notice((string)$this->connection->getPort(), null, [], 'Port'));
        $result->addNotice(new Notice((string)$this->connection->getDatabase(), null, [], 'Database'));
        $result->addNotice(new Notice((string)$this->connection->getDriver()->getName(), null, [], 'Driver'));
        $result->addNotice(new Notice((string)$this->connection->getUsername(), null, [], 'Username'));
        if ($tableExists) {
            $result->addNotice(new Notice('%s (exists)', null, [$this->eventTableName], 'Table'));

            $fromSchema = $schemaManager->createSchema();
            $schemaDiff = (new Comparator())->compare($fromSchema, $this->createEventStoreSchema());
            /** @noinspection PhpUnhandledExceptionInspection */
            $statements = $schemaDiff->toSaveSql($this->connection->getDatabasePlatform());
            if ($statements !== []) {
                $result->addWarning(new Warning('The schema of table %s is not up-to-date', null, [$this->eventTableName], 'Table schema'));
                foreach ($statements as $statement) {
                    $result->addWarning(new Warning($statement, null, [], 'Required statement'));
                }
            }
        } else {
            $result->addWarning(new Warning('%s (missing)', null, [$this->eventTableName], 'Table'));
        }
        return $result;
    }

    /**
     * @inheritdoc
     * @throws DBALException | \Throwable
     */
    public function setup(): Result
    {
        $result = new Result();
        $schemaManager = $this->connection->getSchemaManager();
        if ($schemaManager === null) {
            $result->addError(new Error('Failed to retrieve Schema Manager', 1592381759, [], 'Connection failed'));
            return $result;
        }
        try {
            $tableExists = $schemaManager->tablesExist([$this->eventTableName]);
        } catch (ConnectionException $exception) {
            $result->addError(new Error($exception->getMessage(), $exception->getCode(), [], 'Connection failed'));
            return $result;
        }
        if ($tableExists) {
            $result->addNotice(new Notice('Table "%s" (already exists)', null, [$this->eventTableName]));
        } else {
            $result->addNotice(new Notice('Creating database table "%s" in database "%s" on host %s....', null, [$this->eventTableName, $this->connection->getDatabase(), $this->connection->getHost()]));
        }

        $fromSchema = $schemaManager->createSchema();
        $schemaDiff = (new Comparator())->compare($fromSchema, $this->createEventStoreSchema());

        $statements = $schemaDiff->toSaveSql($this->connection->getDatabasePlatform());
        if ($statements === []) {
            $result->addNotice(new Notice('Table schema is up to date, no migration required'));
            return $result;
        }
        $this->connection->beginTransaction();
        try {
            foreach ($statements as $statement) {
                $result->addNotice(new Notice('<info>++</info> %s', null, [$statement]));
                $this->connection->exec($statement);
            }
            $this->connection->commit();
        } catch (\Throwable $exception) {
            $this->connection->rollBack();
            throw $exception;
        }
        return $result;
    }

    /**
     * Creates the Doctrine schema to be compared with the current db schema for migration
     *
     * @return Schema
     */
    private function createEventStoreSchema(): Schema
    {
        $schemaConfiguration = new SchemaConfig();
        $connectionParameters = $this->connection->getParams();
        if (isset($connectionParameters['defaultTableOptions'])) {
            $schemaConfiguration->setDefaultTableOptions($connectionParameters['defaultTableOptions']);
        }
        $schema = new Schema([], [], $schemaConfiguration);
        $table = $schema->createTable($this->eventTableName);

        // The monotonic sequence number
        $table->addColumn('sequencenumber', Types::INTEGER, ['autoincrement' => true]);
        // The stream name, usually in the format "<BoundedContext>:<StreamName>"
        $table->addColumn('stream', Types::STRING, ['length' => 255]);
        // Version of the event in the respective stream
        $table->addColumn('version', Types::BIGINT, ['unsigned' => true]);
        // The event type in the format "<BoundedContext>:<EventType>"
        $table->addColumn('type', Types::STRING, ['length' => 255]);
        // The event payload as JSON
        $table->addColumn('payload', Types::TEXT);
        // The event metadata as JSON
        $table->addColumn('metadata', Types::TEXT);
        // The unique event id, usually a UUID
        $table->addColumn('id', Types::STRING, ['length' => 255]);
        // An optional correlation id, usually a UUID
        $table->addColumn('correlationidentifier', Types::STRING, ['length' => 255, 'notnull' => false]);
        // An optional causation id, usually a UUID
        $table->addColumn('causationidentifier', Types::STRING, ['length' => 255, 'notnull' => false]);
        // Timestamp of the the event publishing
        $table->addColumn('recordedat', Types::DATETIME_IMMUTABLE);

        $table->setPrimaryKey(['sequencenumber']);
        $table->addUniqueIndex(['id'], 'id_uniq');
        $table->addUniqueIndex(['stream', 'version'], 'stream_version_uniq');
        $table->addIndex(['correlationidentifier']);

        return $schema;
    }

    /**
     * Reconnects the database connection associated with this storage, if it doesn't respond to a ping
     *
     * @see \Neos\Flow\Persistence\Doctrine\PersistenceManager::persistAll()
     * @return void
     */
    private function reconnectDatabaseConnection(): void
    {
        if ($this->connection->ping() === false) {
            $this->connection->close();
            $this->connection->connect();
        }
    }
}
