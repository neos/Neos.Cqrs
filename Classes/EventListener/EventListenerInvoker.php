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
use Neos\EventSourcing\Event\EventTypeResolver;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\AppliedEventsStorageInterface;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\DoctrineAppliedEventsStorage;
use Neos\EventSourcing\EventListener\Exception\EventCouldNotBeAppliedException;
use Neos\EventSourcing\EventStore\EventEnvelope;
use Neos\EventSourcing\EventStore\EventStore;
use Neos\EventSourcing\EventStore\EventStoreFactory;
use Neos\EventSourcing\EventStore\StreamAwareEventListenerInterface;
use Neos\EventSourcing\EventStore\StreamName;
use Neos\Flow\Annotations as Flow;

/**
 * @Flow\Scope("singleton")
 */
final class EventListenerInvoker
{

    /**
     * @Flow\Inject
     * @var EventStoreFactory
     */
    protected $eventStoreFactory;

    /**
     * @Flow\Inject
     * @var EntityManagerInterface
     */
    protected $entityManager;

    /**
     * @Flow\Inject
     * @var EventTypeResolver
     */
    protected $eventTypeResolver;

    /**
     * @Flow\InjectConfiguration(path="EventListener.listeners")
     * @var array
     */
    protected $eventListenersConfiguration;

    /**
     * @param EventListenerInterface $listener
     * @param \Closure $progressCallback
     * @throws EventCouldNotBeAppliedException
     */
    public function catchUp(EventListenerInterface $listener, \Closure $progressCallback = null): void
    {
        if ($listener instanceof ProvidesAppliedEventsStorageInterface) {
            $appliedEventsStorage = $listener->getAppliedEventsStorage();
        } elseif ($listener instanceof AppliedEventsStorageInterface) {
            $appliedEventsStorage = $listener;
        } else {
            $appliedEventsStorage = new DoctrineAppliedEventsStorage($this->entityManager->getConnection(), \get_class($listener));
        }
        $highestAppliedSequenceNumber = $appliedEventsStorage->reserveHighestAppliedEventSequenceNumber();
        $streamName = $listener instanceof StreamAwareEventListenerInterface ? $listener::listensToStream() : StreamName::all();
        $eventStore = $this->getEventStoreForEventListener($listener);
        $eventStream = $eventStore->load($streamName, $highestAppliedSequenceNumber + 1);
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
     * @return EventStore
     */
    public function getEventStoreForEventListener(EventListenerInterface $listener): EventStore
    {
        $listenerClassName = \get_class($listener);
        $eventStoreIdentifier = $this->eventListenersConfiguration[$listenerClassName]['eventStore'] ?? 'default';
        try {
            return $this->eventStoreFactory->create($eventStoreIdentifier);
        } catch (\InvalidArgumentException $exception) {
            throw new \RuntimeException(sprintf('Failed to build Event Store for listener "%s": %s', $listenerClassName, $exception->getMessage()), 1570191582, $exception);
        }
    }

    /**
     * @param EventListenerInterface $listener
     * @param EventEnvelope $eventEnvelope
     * @throws EventCouldNotBeAppliedException
     */
    private function applyEvent(EventListenerInterface $listener, EventEnvelope $eventEnvelope): void
    {
        $rawEvent = $eventEnvelope->getRawEvent();
        try {
            $eventClassName = $this->eventTypeResolver->getEventClassNameByType($rawEvent->getType());
            $listenerMethodName = 'when' . (new \ReflectionClass($eventClassName))->getShortName();
        } catch (\ReflectionException $exception) {
            throw new \RuntimeException(sprintf('Could not extract listener method name for listener %s and event %s', get_class($listener), $eventClassName), 1541003718, $exception);
        }
        if (!method_exists($listener, $listenerMethodName)) {
            return;
        }
        if ($listener instanceof BeforeInvokeInterface) {
            $listener->beforeInvoke($eventEnvelope);
        }
        try {
            $listener->$listenerMethodName($eventEnvelope->getDomainEvent(), $rawEvent);
        } catch (\Throwable $exception) {
            throw new EventCouldNotBeAppliedException(sprintf('Event "%s" (%s) could not be applied to %s. Sequence number (%d) is not updated', $rawEvent->getIdentifier(), $rawEvent->getType(), get_class($listener), $rawEvent->getSequenceNumber()), 1544207001, $exception, $eventEnvelope, $listener);
        }
        if ($listener instanceof AfterInvokeInterface) {
            $listener->afterInvoke($eventEnvelope);
        }
    }
}
