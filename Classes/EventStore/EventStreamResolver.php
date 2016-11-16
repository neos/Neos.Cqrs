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

use Neos\Cqrs\Domain\AggregateRootInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Object\ObjectManagerInterface;
use TYPO3\Flow\Package\PackageManagerInterface;
use TYPO3\Flow\Reflection\ClassReflection;
use TYPO3\Flow\Utility\TypeHandling;

/**
 * @Flow\Scope("singleton")
 */
class EventStreamResolver
{
    /**
     * @Flow\Inject
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * @param AggregateRootInterface $aggregate
     * @return string
     */
    public function getStreamNameForAggregate(AggregateRootInterface $aggregate)
    {
        return $this->getStreamNameForAggregateTypeAndIdentifier(TypeHandling::getTypeForValue($aggregate), $aggregate->getIdentifier());
    }

    /**
     * @param string $aggregateClassName
     * @param string $aggregateIdentifier
     * @return string
     */
    public function getStreamNameForAggregateTypeAndIdentifier(string $aggregateClassName, string $aggregateIdentifier)
    {
        $packageKey = $this->objectManager->getPackageKeyByObjectName($aggregateClassName);
        $aggregateShortClassName = (new ClassReflection($aggregateClassName))->getShortName();
        return $packageKey . ':' . $aggregateShortClassName . ':' . $aggregateIdentifier;
    }
}
