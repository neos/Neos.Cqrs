<?php
declare(strict_types=1);
namespace Neos\EventSourcing\Projection;

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
use Doctrine\DBAL\DBALException;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\DoctrineAppliedEventsStorage;

trait DoctrineProjectorSnapshotTrait
{
    /**
     * @return Connection
     */
    abstract protected function getDatabaseConnection(): Connection;

    /**
     * Returns the database table names used by the concrete projection
     *
     * @return array
     */
    abstract protected static function getTableNames(): array;

    /**
     * @param SnapshotIdentifier $snapshotIdentifier
     * @return int The event sequence number at which the snapshot was created
     * @throws DBALException
     */
    public function createSnapshot(SnapshotIdentifier $snapshotIdentifier): int
    {
        $databaseConnection = $this->getDatabaseConnection();
        $doctrineAppliedEventsStorage = new DoctrineAppliedEventsStorage($databaseConnection, get_class($this));

        $eventSequenceNumber = $doctrineAppliedEventsStorage->reserveHighestAppliedEventSequenceNumber();

        foreach (static::getTableNames() as $tableName) {
            $snapshotTableName = $this->renderUtilityTableName('snap', $snapshotIdentifier, $tableName);

            $databaseConnection->exec(sprintf('DROP TABLE IF EXISTS %s', $snapshotTableName));
            $databaseConnection->exec(sprintf('CREATE TABLE %s AS SELECT * FROM %s', $snapshotTableName, $tableName));
        }

        $doctrineAppliedEventsStorage->releaseHighestAppliedSequenceNumber();
        return $eventSequenceNumber;
    }

    /**
     * @param Snapshot $snapshot
     * @throws DBALException
     */
    public function restoreSnapshot(Snapshot $snapshot): void
    {
        $databaseConnection = $this->getDatabaseConnection();
        $doctrineAppliedEventsStorage = new DoctrineAppliedEventsStorage($databaseConnection, get_class($this));

        foreach (static::getTableNames() as $originalTableName) {
            $snapshotTableName = $this->renderUtilityTableName('snap', $snapshot->getSnapshotIdentifier(), $originalTableName);
            $temporaryTableName = $this->renderUtilityTableName('temp', $snapshot->getSnapshotIdentifier(), $originalTableName);

            $databaseConnection->exec(sprintf('DROP TABLE IF EXISTS %s', $temporaryTableName));
            $databaseConnection->exec(sprintf('CREATE TABLE %s AS SELECT * FROM %s', $temporaryTableName, $snapshotTableName));
        }

        $doctrineAppliedEventsStorage->reserveHighestAppliedEventSequenceNumber();

        $tablesToTrash = [];
        foreach (static::getTableNames() as $originalTableName) {
            $temporaryTableName = $this->renderUtilityTableName('temp', $snapshot->getSnapshotIdentifier(), $originalTableName);
            $trashTableName = $this->renderUtilityTableName('trash', $snapshot->getSnapshotIdentifier(), $originalTableName);
            $tablesToTrash[] = $trashTableName;

            $databaseConnection->exec(sprintf('ALTER TABLE %s RENAME TO %s', $originalTableName, $trashTableName));
            $databaseConnection->exec(sprintf('ALTER TABLE %s RENAME TO %s', $temporaryTableName, $originalTableName));
        }

        $doctrineAppliedEventsStorage->saveHighestAppliedSequenceNumber($snapshot->getEventSequenceNumber());

        foreach ($tablesToTrash as $originalTableName) {
            $databaseConnection->exec(sprintf('DROP TABLE %s', $originalTableName));
        }
    }

    /**
     * Renders a unique, modestly telling and not too long table name for a snapshot
     *
     * @param string $prefix An up to 5 character long prefix: "snap", "trash" or "temp"
     * @param SnapshotIdentifier $snapshotIdentifier
     * @param string $originalTableName
     * @return string
     */
    protected function renderUtilityTableName(string $prefix, SnapshotIdentifier $snapshotIdentifier, string $originalTableName): string
    {
        $utilityTableName = $prefix . '_' . str_replace('-', '_', $snapshotIdentifier) . '_' . substr(str_replace('_', '', $originalTableName), -12) . '_' . substr(sha1($originalTableName), -8);
        assert(strlen($utilityTableName) <= 64);
        return $utilityTableName;
    }
}
