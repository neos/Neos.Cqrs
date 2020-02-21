<?php
declare(strict_types=1);
namespace Neos\EventSourcing\EventPublisher;

/*
 * This file is part of the Neos.EventSourcing package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\JobQueue\Common\Job\JobManager;
use Neos\EventSourcing\Event\DecoratedEvent;
use Neos\EventSourcing\Event\DomainEvents;
use Neos\EventSourcing\EventListener\CatchAllEventListenerInterface;
use Neos\EventSourcing\EventListener\Mapping\EventToListenerMappings;
use Neos\EventSourcing\EventPublisher\JobQueue\CatchUpEventListenerJob;
use Neos\Flow\Annotations as Flow;

/**
 * An Event Publisher that uses a Job Queue from the Flowpack.JobQueue package to notify Event Listeners of new Events.
 *
 * It sends a CatchUpEventListenerJob to the configured queue for each individual Event Listener class that is affected (i.e. that is registered
 * for at least one of the published Events).
 * This job then makes sure that the corresponding Event Listener fetches all new Events from the Event Store until it is caught up.
 * This is done in order to reduce the risk of lost events and to move deduplication logic to the framework to achieve "Exactly-once Delivery".
 *
 * The queue name is "neos-eventsourcing" by default, but that can be changed with the "queueName" option.
 * Other options can be specified via ("queueOptions") – see "Submit options" documentation of the corresponding JobQueue implementation.
 *
 * Example configuration:
 *
 * Neos:
 *   EventSourcing:
 *     EventStore:
 *       stores:
 *         'Some.Package:SomeStore':
 *           // ...
 *           listeners:
 *             'Some.Package\.*': true
 *               queueName: 'custom-queue'
 *               queueOptions:
 *                 priority: 2048
 */
final class JobQueueEventPublisher implements EventPublisherInterface
{
    /**
     * @const string
     */
    private const DEFAULT_QUEUE_NAME = 'neos-eventsourcing';

    /**
     * @Flow\Inject
     * @var JobManager
     */
    protected $jobManager;

    /**
     * @var string
     */
    private $eventStoreIdentifier;

    /**
     * @var EventToListenerMappings
     */
    private $mappings;

    public function __construct(string $eventStoreIdentifier, EventToListenerMappings $mappings)
    {
        $this->eventStoreIdentifier = $eventStoreIdentifier;
        $this->mappings = $mappings;
    }

    /**
     * Iterate through EventToListenerMappings and queue a CatchUpEventListenerJob for every affected Event Listener
     *
     * @param DomainEvents $events
     */
    public function publish(DomainEvents $events): void
    {
        $queuedEventListenerClassNames = [];
        $processedEventClassNames = [];
        foreach ($events as $event) {
            $eventClassName = \get_class($event instanceof DecoratedEvent ? $event->getWrappedEvent() : $event);
            // only process every Event type once
            if (isset($processedEventClassNames[$eventClassName])) {
                continue;
            }
            foreach ($this->mappings as $mapping) {
                if (!$mapping->matchesEventClassName($eventClassName)) {
                    continue;
                }
                // only process every Event Listener once
                if (isset($queuedEventListenerClassNames[$mapping->getListenerClassName()])) {
                    continue;
                }
                $queueName = $mapping->getOption('queueName', self::DEFAULT_QUEUE_NAME);
                $options = $mapping->getOption('queueOptions', []);
                $this->jobManager->queue($queueName, new CatchUpEventListenerJob($mapping->getListenerClassName(), $this->eventStoreIdentifier), $options);
                $queuedEventListenerClassNames[$mapping->getListenerClassName()] = true;
            }
        }
    }
}
