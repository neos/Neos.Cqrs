<?php
namespace Neos\Cqrs\Projection;

/*
 * This file is part of the Neos.Cqrs package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Neos\Cqrs\Event\EventInterface;
use TYPO3\Flow\Log\SystemLoggerInterface;
use TYPO3\Flow\Reflection\ObjectAccess;
use TYPO3\Flow\Annotations as Flow;

/**
 * A base class for projections
 *
 * Specialized projections may extend this class in order to use the convenience methods included. Alternatively, they
 * can as well just implement the ProjectionInterface and refrain from extending this base class.
 *
 * @api
 */
abstract class AbstractBaseProjection implements ProjectionInterface
{

    /**
     * @Flow\Inject
     * @var SystemLoggerInterface
     * @api
     */
    protected $systemLogger;

    /**
     * Concrete projectors may override this property for setting the class name of the Read Model to a non-conventional name
     *
     * @var string
     * @api
     */
    protected $readModelClassName;

    /**
     * Initialize the Read Model class name
     * Make sure to call this method as parent when overriding it in a concrete projector.
     *
     * @return void
     */
    protected function initializeObject()
    {
        if ($this->readModelClassName === null && substr(get_class($this), -10, 10) === 'Projection') {
            $this->readModelClassName = substr(get_class($this), 0, -10);
        }
    }

    /**
     * Sets the properties in a Read Model with corresponding properties of an event according to the given map of
     * event and read model property names.
     *
     * You can pass the property names in four different ways:
     *
     * 1. [ "eventPropertyName" => "readModelPropertyName", ... ]
     * 2. [ "propertyName", ...]
     *
     * In the second case "propertyName" will be used both for determining the event property as well as the read model property.
     * Combinations of both are also possible:
     *
     * 3. [ "eventSomeFoo" => "readModelSomeFoo", "bar", "baz", "eventSomeQuux" => "readModelSomeQuux" ]
     *
     * 4. If the property map array is empty, this method will try to map all accessible properties of the event to the same name properties in the read model.
     *
     * For use in the concrete projection.
     *
     * @param EventInterface $event An event
     * @param object $readModel A read model
     * @param array $propertyNamesMap Property names of the event (key) and of the read model (value). Alternatively just the property name as value.
     * @return void
     * @api
     */
    protected function mapEventToReadModel(EventInterface $event, $readModel, array $propertyNamesMap = [])
    {
        if ($propertyNamesMap === []) {
            $propertyNamesMap = ObjectAccess::getGettablePropertyNames($event);
        }

        foreach ($propertyNamesMap as $eventPropertyName => $readModelPropertyName) {
            if (is_numeric($eventPropertyName)) {
                $eventPropertyName = $readModelPropertyName;
            }
            if (ObjectAccess::isPropertyGettable($event, $eventPropertyName) && ObjectAccess::isPropertySettable($readModel, $readModelPropertyName)) {
                ObjectAccess::setProperty($readModel, $readModelPropertyName, ObjectAccess::getProperty($event, $eventPropertyName));
            }
        }
    }

}
