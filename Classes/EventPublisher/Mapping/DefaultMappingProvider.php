<?php
declare(strict_types=1);
namespace Neos\EventSourcing\EventPublisher\Mapping;

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
use Neos\EventSourcing\EventListener\EventListenerInterface;
use Neos\EventSourcing\EventPublisher\Exception\EventPublisherException;
use Neos\EventSourcing\EventPublisher\Exception\InvalidConfigurationException;
use Neos\EventSourcing\EventPublisher\Exception\InvalidEventListenerException;
use Neos\EventSourcing\EventStore\RawEvent;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\Configuration\Exception\InvalidConfigurationTypeException;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Reflection\ReflectionService;

/**
 * TODO
 */
class DefaultMappingProvider
{

    /**
     * @var Mappings[] indexed by the corresponding EventStore identifier
     */
    private $mappings;

    /**
     * This class is usually not instantiated manually but injected like other singletons
     *
     * @param ObjectManagerInterface $objectManager
     * @throws EventPublisherException
     */
    public function __construct(ObjectManagerInterface $objectManager)
    {
        $mappingsAndOptions = static::prepareMappings($objectManager);
        foreach ($mappingsAndOptions['mappings'] as $eventStoreIdentifier => $mappings) {
            $eventStoreMappings = [];
            foreach ($mappings as $mapping) {
                $eventStoreMappings[] = Mapping::create($mapping['eventClassName'], $mapping['listenerClassName'], $mapping['presetId'] ? $mappingsAndOptions['presets'][$mapping['presetId']] : []);
            }
            $this->mappings[$eventStoreIdentifier] = Mappings::fromArray($eventStoreMappings);
        }
    }

    public function getMappingsForEventStore(string $eventStoreIdentifier): Mappings
    {
        if (!isset($this->mappings[$eventStoreIdentifier])) {
            throw new \InvalidArgumentException(sprintf('No mappings found for Event Store "%s". Configured stores are: "%s"', $eventStoreIdentifier, implode('", "', array_keys($this->mappings))), 1578656948);
        }
        return $this->mappings[$eventStoreIdentifier];
    }

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
     * @throws EventPublisherException
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
                throw new InvalidConfigurationException(sprintf('Unmatched event store: "%s"', $eventStoreIdentifier), 1577534654);
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
                }
                if (!$presetMatchesAnyListeners) {
                    throw new InvalidConfigurationException(sprintf('The pattern %s.%s does not match any listeners', $eventStoreIdentifier, $pattern), 1577533005);
                }
            }
        }
        $unmatchedListeners = array_diff_key($listeners, $matchedListeners);
        if ($unmatchedListeners !== []) {
            throw new InvalidConfigurationException(sprintf('Unmatched listener(s): "%s"', implode('", "', array_keys($unmatchedListeners))), 1577532358);
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
                throw new InvalidEventListenerException(sprintf('No listener methods have been detected in listener class %s. A listener has the signature "public function when<EventClass>(<EventClass> $event) {}" and every EventListener class has to implement at least one listener!',
                    $listenerClassName), 1498123537);
            }
        }

        return $listeners;
    }
}