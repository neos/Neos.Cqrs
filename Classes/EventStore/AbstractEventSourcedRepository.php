<?php
namespace Neos\Cqrs\EventStore;

/*
 * This file is part of the Neos.EventStore package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Neos\Cqrs\Domain\EventSourcedAggregateRootInterface;
use Neos\Cqrs\Domain\Exception\AggregateRootNotFoundException;
use Neos\Cqrs\Domain\RepositoryInterface;
use Neos\Cqrs\Event\EventPublisher;
use Neos\Cqrs\EventStore\Exception\EventStreamNotFoundException;
use TYPO3\Flow\Annotations as Flow;

/**
 * Base implementation for an event-sourced repository
 */
abstract class AbstractEventSourcedRepository implements RepositoryInterface
{
    /**
     * @Flow\Inject
     * @var EventStore
     */
    protected $eventStore;

    /**
     * @Flow\Inject
     * @var EventPublisher
     */
    protected $eventPublisher;

    /**
     * @Flow\Inject
     * @var StreamNameResolver
     */
    protected $streamNameResolver;

    /**
     * @var string
     */
    protected $aggregateClassName;

    /**
     * Initializes a new Repository.
     */
    public function __construct()
    {
        $this->aggregateClassName = preg_replace(['/Repository$/'], [''], get_class($this));
    }

    /**
     * @param string $identifier
     * @return EventSourcedAggregateRootInterface
     * @throws AggregateRootNotFoundException
     */
    final public function findByIdentifier($identifier)
    {
        $streamName = $this->streamNameResolver->getStreamNameForAggregateTypeAndIdentifier($this->aggregateClassName, $identifier);
        try {
            $eventStream = $this->eventStore->get(new StreamNameFilter($streamName));
        } catch (EventStreamNotFoundException $exception) {
            return null;
        }
        if (!class_exists($this->aggregateClassName)) {
            throw new AggregateRootNotFoundException(sprintf("Could not reconstitute the aggregate root %s because its class '%s' does not exist.", $identifier, $this->aggregateClassName), 1474454928115);
        }
        if (!is_subclass_of($this->aggregateClassName, EventSourcedAggregateRootInterface::class)) {
            throw new AggregateRootNotFoundException(sprintf("Could not reconstitute the aggregate root '%s' with id '%s' because it does not implement the EventSourcedAggregateRootInterface.", $this->aggregateClassName, $identifier, $this->aggregateClassName), 1474464335530);
        }

        return call_user_func($this->aggregateClassName . '::reconstituteFromEventStream', $identifier, $eventStream);
    }

    /**
     * @param EventSourcedAggregateRootInterface $aggregate
     * @param int $expectedVersion
     * @return void
     */
    final public function save(EventSourcedAggregateRootInterface $aggregate, int $expectedVersion = null)
    {
        $streamName = $this->streamNameResolver->getStreamNameForAggregate($aggregate);
        if ($expectedVersion === null) {
            $expectedVersion = $aggregate->getReconstitutionVersion();
        }
        $this->eventPublisher->publishMany($streamName, $aggregate->pullUncommittedEvents(), $expectedVersion);
    }
}
