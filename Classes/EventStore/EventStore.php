<?php
namespace Neos\EventSourcing\EventStore;

/*
 * This file is part of the Neos.EventSourcing package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Neos\Error\Messages\Result;
use Neos\EventSourcing\EventStore\Exception\EventNotFoundException;
use Neos\EventSourcing\EventStore\Exception\EventStreamNotFoundException;
use Neos\EventSourcing\EventStore\Storage\EventStorageInterface;

/**
 * Main API to store and fetch events.
 *
 * NOTE: Do not instantiate this class directly but use the EventStoreManager
 */
final class EventStore
{
    /**
     * @var EventStorageInterface
     */
    private $storage;

    /**
     * @internal Do not instantiate this class directly but use the EventStoreManager
     * @param EventStorageInterface $storage
     */
    public function __construct(EventStorageInterface $storage)
    {
        $this->storage = $storage;
    }

    /**
     * Returns an EventStream that matches the given $filter - or throws an EventStreamNotFoundException if no matching events could be found
     *
     * @param EventStreamFilterInterface $filter
     * @return EventStream Can be empty stream
     * @throws EventStreamNotFoundException
     * @todo improve exception message, log the current filter type and configuration
     */
    public function get(EventStreamFilterInterface $filter): EventStream
    {
        $eventStream = $this->storage->load($filter);
        if (!$eventStream->valid()) {
            $streamName = $filter->getFilterValue(EventStreamFilterInterface::FILTER_STREAM_NAME) ?? 'unknown stream';
            throw new EventStreamNotFoundException(sprintf('The event stream "%s" does not appear to be valid.', $streamName), 1477497156);
        }
        return $eventStream;
    }

    /**
     * Returns the first EventAndRawEvent instance that matches the given $filter - or throws an EventNotFoundException if no matching events could be found
     *
     * @param EventStreamFilterInterface $filter
     * @return EventAndRawEvent
     * @throws EventNotFoundException
     */
    public function getOne(EventStreamFilterInterface $filter): EventAndRawEvent
    {
        $rawEvent = $this->storage->loadOne($filter);
        if ($rawEvent === null) {
            throw new EventNotFoundException('No event found for the given filter', 1513170518);
        }
        return (new EventStream(new \ArrayIterator([$rawEvent])))->current();
    }

    /**
     * @param string $streamName
     * @param WritableEvents $events
     * @param int $expectedVersion
     * @return RawEvent[]
     */
    public function commit(string $streamName, WritableEvents $events, int $expectedVersion = ExpectedVersion::ANY): array
    {
        return $this->storage->commit($streamName, $events, $expectedVersion);
    }

    /**
     * Returns the (connection) status of this Event Store, @see EventStorageInterface::getStatus()
     *
     * @return Result
     */
    public function getStatus()
    {
        return $this->storage->getStatus();
    }

    /**
     * Sets up this Event Store and returns a status, @see EventStorageInterface::setup()
     *
     * @return Result
     */
    public function setup()
    {
        return $this->storage->setup();
    }
}
