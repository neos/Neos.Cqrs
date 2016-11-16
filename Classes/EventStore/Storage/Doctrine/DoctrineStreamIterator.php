<?php
namespace Neos\Cqrs\EventStore\Storage\Doctrine;

/*
 * This file is part of the Neos.EventStore.DatabaseStorageAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Query\QueryBuilder;
use Neos\Cqrs\EventStore\RawEvent;

/**
 * Stream Iterator for the doctrine based EventStore
 */
final class DoctrineStreamIterator implements \Iterator
{

    /**
     * The number of records to fetch per batch
     *
     * @var int
     */
    const BATCH_SIZE = 100;

    /**
     * @var QueryBuilder
     */
    private $queryBuilder;

    /**
     * @var int
     */
    private $currentOffset = 0;

    /**
     * @var \ArrayIterator
     */
    private $innerIterator;

    /**
     * @param QueryBuilder $queryBuilder
     */
    public function __construct(QueryBuilder $queryBuilder)
    {
        $this->queryBuilder = clone $queryBuilder;
        $this->queryBuilder->setMaxResults(self::BATCH_SIZE);
        $this->fetchBatch();
    }

    /**
     * @return RawEvent
     */
    public function current()
    {
        $currentEventData = $this->innerIterator->current();
        $payload = json_decode($currentEventData['payload'], true);
        $metadata = json_decode($currentEventData['metadata'], true);
        $recordedAt = new \DateTimeImmutable($currentEventData['recordedat']);
        return new RawEvent(
            $currentEventData['sequencenumber'],
            $currentEventData['type'],
            $payload,
            $metadata,
            (int)$currentEventData['version'],
            $currentEventData['id'],
            $recordedAt
        );
    }

    /**
     * @return void
     */
    public function next()
    {
        $this->currentOffset ++;
        $this->innerIterator->next();
        if ($this->innerIterator->valid()) {
            return;
        }
        $this->fetchBatch();
    }

    /**
     * @return bool|int
     */
    public function key()
    {
        return $this->innerIterator->valid() ? $this->innerIterator->current()['sequencenumber'] : null;
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return $this->innerIterator->valid();
    }

    /**
     * @return void
     */
    public function rewind()
    {
        if ($this->currentOffset === 0) {
            return;
        }
        $this->currentOffset = 0;
        $this->fetchBatch();
    }

    /**
     * Fetches a batch of maximum BATCH_SIZE records
     *
     * @return void
     */
    private function fetchBatch()
    {
        $this->queryBuilder->setFirstResult($this->currentOffset);
        $rawResult = $this->queryBuilder->execute()->fetchAll();
        $this->innerIterator = new \ArrayIterator($rawResult);
    }
}
