<?php
declare(strict_types=1);
namespace Neos\EventSourcing\EventListener;

/*
 * This file is part of the Neos.EventSourcing package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\ORM\EntityManagerInterface;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\AppliedEventsStorageInterface;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\DoctrineAppliedEventsStorage;
use Neos\EventSourcing\EventListener\Exception\EventCouldNotBeAppliedException;
use Neos\EventSourcing\EventStore\EventEnvelope;
use Neos\EventSourcing\EventStore\EventStore;
use Neos\EventSourcing\EventStore\StreamAwareEventListenerInterface;
use Neos\EventSourcing\EventStore\StreamName;
use Neos\Flow\Annotations as Flow;

final class EventListenerInvoker
{

    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @Flow\Inject
     * @var EntityManagerInterface
     */
    protected $entityManager;

    public function __construct(EventStore $eventStore)
    {
        $this->eventStore = $eventStore;
    }

    /**
     * @param EventListenerInterface $listener
     * @param \Closure $progressCallback
     * @throws EventCouldNotBeAppliedException
     */
    public function replay(EventListenerInterface $listener, \Closure $progressCallback = null): void
    {
        $appliedEventsStorage = $this->getAppliedEventsStorageForListener($listener);
        $highestAppliedSequenceNumber = -1;
        $appliedEventsStorage->saveHighestAppliedSequenceNumber($highestAppliedSequenceNumber);
        $highestAppliedSequenceNumber = $appliedEventsStorage->reserveHighestAppliedEventSequenceNumber();

        $streamName = $listener instanceof StreamAwareEventListenerInterface ? $listener::listensToStream() : StreamName::all();
        $eventStream = $this->eventStore->load($streamName);
        foreach ($eventStream as $eventEnvelope) {
            try {
                $this->applyEvent($listener, $eventEnvelope);
                $highestAppliedSequenceNumber = $eventEnvelope->getRawEvent()->getSequenceNumber();
            } catch (EventCouldNotBeAppliedException $exception) {
                $appliedEventsStorage->saveHighestAppliedSequenceNumber($highestAppliedSequenceNumber);
                $appliedEventsStorage->releaseHighestAppliedSequenceNumber();
                throw $exception;
            }
            if ($progressCallback !== null) {
                $progressCallback($eventEnvelope);
            }
        }
        $appliedEventsStorage->saveHighestAppliedSequenceNumber($highestAppliedSequenceNumber);
        $appliedEventsStorage->releaseHighestAppliedSequenceNumber();
    }

    /**
     * @param EventListenerInterface $listener
     * @param \Closure $progressCallback
     * @throws EventCouldNotBeAppliedException
     */
    public function catchUp(EventListenerInterface $listener, \Closure $progressCallback = null): void
    {
        $appliedEventsStorage = $this->getAppliedEventsStorageForListener($listener);
        $highestAppliedSequenceNumber = $appliedEventsStorage->reserveHighestAppliedEventSequenceNumber();
        $streamName = $listener instanceof StreamAwareEventListenerInterface ? $listener::listensToStream() : StreamName::all();
        $eventStream = $this->eventStore->load($streamName, $highestAppliedSequenceNumber + 1);
        foreach ($eventStream as $eventEnvelope) {
            try {
                $this->applyEvent($listener, $eventEnvelope);
            } catch (EventCouldNotBeAppliedException $exception) {
                $appliedEventsStorage->releaseHighestAppliedSequenceNumber();
                throw $exception;
            }
            $appliedEventsStorage->saveHighestAppliedSequenceNumber($eventEnvelope->getRawEvent()->getSequenceNumber());
            if ($progressCallback !== null) {
                $progressCallback($eventEnvelope);
            }
        }
        $appliedEventsStorage->releaseHighestAppliedSequenceNumber();
    }

    /**
     * @param EventListenerInterface $listener
     * @param EventEnvelope $eventEnvelope
     * @throws EventCouldNotBeAppliedException
     */
    private function applyEvent(EventListenerInterface $listener, EventEnvelope $eventEnvelope): void
    {
        $event = $eventEnvelope->getDomainEvent();
        $rawEvent = $eventEnvelope->getRawEvent();
        try {
            $listenerMethodName = 'when' . (new \ReflectionClass($event))->getShortName();
        } catch (\ReflectionException $exception) {
            throw new \RuntimeException(sprintf('Could not extract listener method name for listener %s and event %s', get_class($listener), get_class($event)), 1541003718, $exception);
        }
        if ($listener instanceof BeforeInvokeInterface) {
            $listener->beforeInvoke($eventEnvelope);
        }
        if (method_exists($listener, $listenerMethodName)) {
            try {
                $listener->$listenerMethodName($event, $rawEvent);
            } catch (\Throwable $exception) {
                throw new EventCouldNotBeAppliedException(sprintf('Event "%s" (%s) could not be applied to %s. Sequence number (%d) is not updated', $rawEvent->getIdentifier(), $rawEvent->getType(), get_class($listener),
                    $rawEvent->getSequenceNumber()), 1544207001, $exception, $eventEnvelope, $listener);
            }
        }
        if ($listener instanceof AfterInvokeInterface) {
            $listener->afterInvoke($eventEnvelope);
        }
    }

    private function getAppliedEventsStorageForListener(EventListenerInterface $listener): AppliedEventsStorageInterface
    {
        if ($listener instanceof ProvidesAppliedEventsStorageInterface) {
            $appliedEventsStorage = $listener->getAppliedEventsStorage();
        } elseif ($listener instanceof AppliedEventsStorageInterface) {
            $appliedEventsStorage = $listener;
        } else {
            $appliedEventsStorage = new DoctrineAppliedEventsStorage($this->entityManager->getConnection(), \get_class($listener));
        }
        return $appliedEventsStorage;
    }
}
