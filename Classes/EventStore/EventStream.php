<?php
declare(strict_types=1);
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

use Neos\Flow\Annotations as Flow;

/**
 * EventStream
 */
final class EventStream implements \Iterator
{

    /**
     * @Flow\Inject
     * @var EventNormalizer
     */
    protected $eventNormalizer;

    /**
     * @var \Iterator
     */
    private $streamIterator;

    /**
     * @var StreamName
     */
    private $streamName;

    /**
     * @param StreamName $streamName
     * @param EventStreamIteratorInterface $streamIterator
     */
    public function __construct(StreamName $streamName, EventStreamIteratorInterface $streamIterator)
    {
        $this->streamName = $streamName;
        $this->streamIterator = $streamIterator;
    }

    public function getName(): StreamName
    {
        return $this->streamName;
    }

    /**
     * @return EventEnvelope
     */
    public function current(): EventEnvelope
    {
        /** @var RawEvent $rawEvent */
        $rawEvent = $this->streamIterator->current();
        return new EventEnvelope(
            function () use ($rawEvent) {
                return $this->eventNormalizer->denormalize($rawEvent->getPayload(), $rawEvent->getType());
            },
            $rawEvent
        );
    }

    public function next(): void
    {
        $this->streamIterator->next();
    }

    public function key()
    {
        return $this->streamIterator->key();
    }

    public function valid(): bool
    {
        return $this->streamIterator->valid();
    }

    public function rewind(): void
    {
        $this->streamIterator->rewind();
    }
}
