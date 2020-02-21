<?php
declare(strict_types=1);
namespace Neos\EventSourcing\EventListener\Mapping;

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
use Neos\EventSourcing\EventListener\CatchAllEventListenerInterface;
use Neos\EventSourcing\EventListener\EventListenerInterface;
use Neos\EventSourcing\EventListener\Exception\InvalidConfigurationException;
use Neos\EventSourcing\EventListener\Exception\InvalidEventListenerException;
use Neos\EventSourcing\EventStore\RawEvent;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\Configuration\Exception\InvalidConfigurationTypeException;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Reflection\ReflectionService;

/**
 * This factory for mappings will create Event Publisher mappings based on the configured EventStore.<store>.listeners
 *
 * @Flow\Scope("singleton")
 */
class DefaultEventToListenerMappingProvider
{

    /**
     * @var EventToListenerMappings[] indexed by the corresponding EventStore identifier
     */
    private $mappings;

    /**
     * This class is usually not instantiated manually but injected like other singletons
     *
     * @param ObjectManagerInterface $objectManager
     * @throws InvalidConfigurationException | InvalidEventListenerException
     */
    public function __construct(ObjectManagerInterface $objectManager)
    {
        $mappingsAndOptions = static::prepareMappings($objectManager);
        foreach ($mappingsAndOptions['mappings'] as $eventStoreIdentifier => $mappings) {
            $eventStoreMappings = [];
            foreach ($mappings as $mapping) {
                $eventStoreMappings[] = EventToListenerMapping::create($mapping['eventClassName'], $mapping['listenerClassName'], $mapping['presetId'] ? $mappingsAndOptions['presets'][$mapping['presetId']] : []);
            }
            $this->mappings[$eventStoreIdentifier] = EventToListenerMappings::fromArray($eventStoreMappings);
        }
    }

    /**
     * @param string $eventStoreIdentifier
     * @return EventToListenerMappings
     */
    public function getMappingsForEventStore(string $eventStoreIdentifier): EventToListenerMappings
    {
        if (!isset($this->mappings[$eventStoreIdentifier])) {
            throw new \InvalidArgumentException(sprintf('No mappings found for Event Store "%s". Configured stores are: "%s"', $eventStoreIdentifier, implode('", "', array_keys($this->mappings))), 1578656948);
        }
        return $this->mappings[$eventStoreIdentifier];
    }

    /**
     * @param string $listenerClassName
     * @return string
     */
    public function getEventStoreIdentifierForListenerClassName(string $listenerClassName): string
    {
        foreach ($this->mappings as $eventStoreIdentifier => $mappings) {
            if ($mappings->hasMappingForListenerClassName($listenerClassName)) {
                return $eventStoreIdentifier;
            }
        }
        throw new \InvalidArgumentException('No mappings found for Event Listener "%s"', 1579187905);
    }

    /**
     * @param ObjectManagerInterface $objectManager
     * @return array
     * @throws InvalidConfigurationException | InvalidEventListenerException
     * @Flow\CompileStatic
     */
    protected static function prepareMappings(ObjectManagerInterface $objectManager): array
    {
        /** @var ReflectionService $reflectionService */
        $reflectionService = $objectManager->get(ReflectionService::class);
        $listeners = self::detectListeners($reflectionService);

        /** @var ConfigurationManager $configurationManager */
        $configurationManager = $objectManager->get(ConfigurationManager::class);
        $eventStoresConfiguration = self::getEventStoresConfiguration($configurationManager);

        if ($eventStoresConfiguration === []) {
            throw new InvalidConfigurationException('No configured event stores. At least one event store should be configured via Neos.EventSourcing.EventStore.stores.*', 1578658050);
        }

        $matchedListeners = [];
        $mappings = [];
        $presets = [];
        foreach ($eventStoresConfiguration as $eventStoreIdentifier => $eventStoreConfiguration) {
            $presetsForThisStore = array_filter($eventStoreConfiguration['listeners'] ?? [], static function ($presetOptions) {
                return $presetOptions !== false;
            });
            if ($presetsForThisStore === []) {
                throw new InvalidConfigurationException(sprintf('No Event Listeners are configured for Event Store "%s". Please register at least one listener via Neos.EventSourcing.EventStore.stores.*.listeners or disable this Event Store', $eventStoreIdentifier), 1577534654);
            }
            foreach ($presetsForThisStore as $pattern => $presetOptions) {
                $presetId = $eventStoreIdentifier . '.' . $pattern;
                $presets[$presetId] = is_array($presetOptions) ? $presetOptions : [];
                $presetMatchesAnyListeners = false;
                foreach ($listeners as $listenerClassName => $events) {
                    if (preg_match('/^' . str_replace('\\', '\\\\', $pattern) . '$/', $listenerClassName) !== 1) {
                        continue;
                    }
                    if (isset($matchedListeners[$listenerClassName])) {
                        if ($eventStoreIdentifier === $matchedListeners[$listenerClassName]['eventStoreIdentifier']) {
                            $message = 'Listener "%s" matches presets "%s" and "%4$s" of Event Store "%5$s". One of the presets need to be adjusted or removed.';
                        } else {
                            $message = 'Listener "%s" matches preset "%s" of Event Store "%s" and preset "%s" of Event Store "%s". One of the presets need to be adjusted or removed.';
                        }
                        throw new InvalidConfigurationException(sprintf($message, $listenerClassName, $matchedListeners[$listenerClassName]['pattern'], $matchedListeners[$listenerClassName]['eventStoreIdentifier'], $pattern, $eventStoreIdentifier), 1577532711);
                    }
                    $presetMatchesAnyListeners = true;
                    $matchedListeners[$listenerClassName] = compact('eventStoreIdentifier', 'pattern');
                    foreach ($events as $eventClassName => $handlerMethodName) {
                        $mappings[$eventStoreIdentifier][] = compact('eventClassName', 'listenerClassName', 'presetId');
                    }
                    if (is_subclass_of($listenerClassName, CatchAllEventListenerInterface::class)) {
                        $eventClassName = '.*';
                        $mappings[$eventStoreIdentifier][] = compact('eventClassName', 'listenerClassName', 'presetId');
                    }
                }
                if (!$presetMatchesAnyListeners) {
                    throw new InvalidConfigurationException(sprintf('The pattern "%s" for Event Store "%s" does not match any listeners. Please adjust the pattern or remove it', $pattern, $eventStoreIdentifier), 1577533005);
                }
            }
        }
        $unmatchedListeners = array_diff_key($listeners, $matchedListeners);
        if ($unmatchedListeners !== []) {
            if (count($unmatchedListeners) === 1) {
                $errorMessage = 'The Event Listener "%s" is not registered to any of the configured Event Stores. Please add a new listener pattern at Neos.EventSourcing.EventStore.stores.*.listeners or remove the unused listener class';
            } else {
                $errorMessage = 'The following Event Listeners are not registered to any of the configured Event Stores: "%s". Please add new listener patterns at Neos.EventSourcing.EventStore.stores.*.listeners or remove the unused listener classes';
            }
            throw new InvalidConfigurationException(sprintf($errorMessage, implode('", "', array_keys($unmatchedListeners))), 1577532358);
        }
        return compact('mappings', 'presets');
    }

    /**
     * @param ConfigurationManager $configurationManager
     * @return array
     */
    private static function getEventStoresConfiguration(ConfigurationManager $configurationManager): array
    {
        try {
            $stores = $configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'Neos.EventSourcing.EventStore.stores');
        } catch (InvalidConfigurationTypeException $e) {
            throw new \RuntimeException('Failed to load Event Store configuration', 1578579711, $e);
        }
        return array_filter($stores, static function ($storeConfiguration) {
            return $storeConfiguration !== false;
        });
    }

    /**
     * @param ReflectionService $reflectionService
     * @return array
     * @throws InvalidEventListenerException
     */
    private static function detectListeners(ReflectionService $reflectionService): array
    {
        $listeners = [];
        foreach ($reflectionService->getAllImplementationClassNamesForInterface(EventListenerInterface::class) as $listenerClassName) {
            $listenersFoundInClass = false;
            foreach (get_class_methods($listenerClassName) as $listenerMethodName) {
                preg_match('/^when[A-Z].*$/', $listenerMethodName, $matches);
                if (!isset($matches[0])) {
                    continue;
                }
                $parameters = array_values($reflectionService->getMethodParameters($listenerClassName, $listenerMethodName));

                if (!isset($parameters[0])) {
                    throw new InvalidEventListenerException(sprintf('Invalid listener in %s::%s the method signature is wrong, must accept an EventInterface and optionally a RawEvent', $listenerClassName, $listenerMethodName), 1472500228);
                }
                $eventClassName = $parameters[0]['class'];
                if (!$reflectionService->isClassImplementationOf($eventClassName, DomainEventInterface::class)) {
                    throw new InvalidEventListenerException(sprintf('Invalid listener in %s::%s the method signature is wrong, the first parameter should be an implementation of EventInterface but it expects an instance of "%s"', $listenerClassName,
                        $listenerMethodName, $eventClassName), 1472504443);
                }

                if (isset($parameters[1])) {
                    $rawEventDataType = $parameters[1]['class'];
                    if ($rawEventDataType !== RawEvent::class) {
                        throw new InvalidEventListenerException(sprintf('Invalid listener in %s::%s the method signature is wrong. If the second parameter is present, it has to be a RawEvent but it expects an instance of "%s"', $listenerClassName,
                            $listenerMethodName, $rawEventDataType), 1472504303);
                    }
                }
                try {
                    $expectedMethodName = 'when' . (new \ReflectionClass($eventClassName))->getShortName();
                } catch (\ReflectionException $exception) {
                    throw new \RuntimeException(sprintf('Failed to determine short name for class %s: %s', $eventClassName, $exception->getMessage()), 1576498725, $exception);
                }
                if ($expectedMethodName !== $listenerMethodName) {
                    throw new InvalidEventListenerException(sprintf('Invalid listener in %s::%s the method name is expected to be "%s"', $listenerClassName, $listenerMethodName, $expectedMethodName), 1476442394);
                }

                $listeners[$listenerClassName][$eventClassName] = $listenerMethodName;
                $listenersFoundInClass = true;
            }
            if (!$listenersFoundInClass) {
                if (!is_subclass_of($listenerClassName, CatchAllEventListenerInterface::class)) {
                    throw new InvalidEventListenerException(sprintf('No listener methods have been detected in listener class %s. A listener has the signature "public function when<EventClass>(<EventClass> $event) {}" and every EventListener class has to implement at least one listener or implement the %s interface!', $listenerClassName, CatchAllEventListenerInterface::class), 1498123537);
                }
                $listeners[$listenerClassName] = [];
            }
        }

        return $listeners;
    }
}
