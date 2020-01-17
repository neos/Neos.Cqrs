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

use Neos\EventSourcing\Event\DomainEventInterface;

/**
 * Wrapper for a event that was loaded from the Event Store.
 * It contains the original event data including raw payload, metadata and technical meta information
 * and the converted EventInterface instance
 */
final class EventEnvelope
{
    /**
     * @var \Closure<DomainEventInterface>
     */
    private $eventGenerator;

    /**
     * @var DomainEventInterface
     */
    private $event;

    /**
     * @var RawEvent
     */
    private $rawEvent;

    public function __construct(\Closure $eventGenerator, RawEvent $rawEvent)
    {
        $this->eventGenerator = $eventGenerator;
        $this->rawEvent = $rawEvent;
    }

    /**
     * The converted event instance
     *
     * @return DomainEventInterface
     */
    public function getDomainEvent(): DomainEventInterface
    {
        if (!$this->event) {
            $this->event = call_user_func($this->eventGenerator);
        }
        return $this->event;
    }

    /**
     * The raw event including version and metadata, as stored in the Event Store
     *
     * @return RawEvent
     */
    public function getRawEvent(): RawEvent
    {
        return $this->rawEvent;
    }
}
