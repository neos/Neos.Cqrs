<?php
namespace Neos\EventSourcing\Tests\Unit\EventStore;

use Neos\EventSourcing\EventStore\EventStore;
use Neos\EventSourcing\EventStore\EventStoreManager;
use Neos\EventSourcing\EventStore\Storage\EventStorageInterface;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Tests\UnitTestCase;
use Neos\Utility\ObjectAccess;

class EventStoreManagerTest extends UnitTestCase
{

    /**
     * @var EventStoreManager
     */
    private $eventStoreManager;

    /**
     * @var EventStorageInterface|\PHPUnit_Framework_MockObject_MockObject
     */
    private $mockEventStorage;

    /**
     * @var EventStore|\PHPUnit_Framework_MockObject_MockObject
     */
    private $mockEventStore;

    /**
     * @var ObjectManagerInterface|\PHPUnit_Framework_MockObject_MockObject
     */
    private $mockObjectManager;

    public function setUp()
    {
        $this->mockObjectManager = $this->getMockBuilder(ObjectManagerInterface::class)->getMock();

        $this->mockEventStorage = $this->getMockBuilder(EventStorageInterface::class)->getMock();
        $this->mockEventStore = new EventStore($this->mockEventStorage);

        $mockConfiguration = [
            'fallbackStore' => [
                'storage' => 'fallbackStoreStorage',
                'boundedContexts' => [
                    '*' => true,
                    'Bounded.Context1' => true,
                ],
            ],
            'eventStore2' => [
                'storage' => 'eventStore2Storage',
                'boundedContexts' => [
                    'Bounded.Context2' => true,
                ],
            ],
            'eventStore3' => [
                'storage' => 'eventStore3Storage',
                'boundedContexts' => [
                    'Bounded.Context3' => true,
                    'Bounded.Context2.Sub' => true,
                    'Bounded.Context23' => true,
                    'Inactive' => false,
                ],
            ],
        ];
        $this->eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
    }

    /**
     * @test
     * @expectedException \Neos\EventSourcing\EventStore\Exception\StorageConfigurationException
     * @expectedExceptionCode 1479213813
     */
    public function getEventStoreThrowsExceptionForEventStoreConfigurationsWithoutBoundedContextTarget()
    {
        $mockConfiguration = [
            'someStore' => [
                'storage' => 'Foo',
                'boundedContexts' => [],
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
        $eventStoreManager->getEventStore('someStore');
    }

    /**
     * @test
     * @expectedException \Neos\EventSourcing\EventStore\Exception\StorageConfigurationException
     * @expectedExceptionCode 1479214520
     */
    public function getEventStoreThrowsExceptionIfNoFallbackStoreIsConfigured()
    {
        $mockConfiguration = [];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
        $eventStoreManager->getEventStore('someStore');
    }

    /**
     * @test
     * @expectedException \Neos\EventSourcing\EventStore\Exception\StorageConfigurationException
     * @expectedExceptionCode 1492434176
     */
    public function getEventStoreThrowsExceptionForEventStoreConfigurationsWithOverlappingBoundedContexts()
    {
        $mockConfiguration = [
            'someStore' => [
                'storage' => 'Foo',
                'boundedContexts' => [
                    'Bounded:Context1' => true,
                    'Bounded:Context2' => true,
                    '*' => true,
                ],
            ],
            'someOtherStore' => [
                'storage' => 'Bar',
                'boundedContexts' => [
                    'Bounded:Context2' => true,
                    'Bounded:Context3' => true,
                ],
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
        $eventStoreManager->getEventStore('someStore');
    }

    /**
     * @test
     * @expectedException \RuntimeException
     * @expectedExceptionCode 1492610857
     */
    public function getEventStoreThrowsExceptionIfTheRequestedEventStoreIsNotConfigured()
    {
        $mockConfiguration = [
            'someStore' => [
                'storage' => 'Foo',
                'boundedContexts' => [
                    '*' => true,
                ],
            ],
            'someOtherStore' => [
                'storage' => 'Bar',
                'boundedContexts' => [
                    'SomeBounded:Context' => true,
                ],
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
        $eventStoreManager->getEventStore('someNonExistingStore');
    }

    /**
     * @test
     * @expectedException \Neos\EventSourcing\EventStore\Exception\StorageConfigurationException
     * @expectedExceptionCode 1492610902
     */
    public function getEventStoreThrowsExceptionIfNoStorageIsConfigured()
    {
        $mockConfiguration = [
            'someStore' => [
                'boundedContexts' => [
                    '*' => true,
                ],
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
        $eventStoreManager->getEventStore('someStore');
    }

    /**
     * @test
     * @expectedException \Neos\EventSourcing\EventStore\Exception\StorageConfigurationException
     * @expectedExceptionCode 1492610908
     */
    public function getEventStoreThrowsExceptionIfConfiguredStorageIsNoEventStorageInterface()
    {
        $mockConfiguration = [
            'someStore' => [
                'storage' => 'EventStorageClassName',
                'boundedContexts' => [
                    '*' => true,
                ],
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);

        $this->mockObjectManager->expects($this->once())->method('get')->with('EventStorageClassName')->will($this->returnValue(new \stdClass()));
        $eventStoreManager->getEventStore('someStore');
    }

    /**
     * @test
     */
    public function getEventStoreReturnsAnInstanceOfEventStoreWithTheConfiguredStorage()
    {
        $mockStorageOptions = ['foo' => 'Bar'];
        $mockConfiguration = [
            'someStore' => [
                'storage' => get_class($this->mockEventStorage),
                'storageOptions' => $mockStorageOptions,
                'boundedContexts' => [
                    '*' => true,
                ],
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);

        $this->mockObjectManager->expects($this->at(0))->method('get')->with(get_class($this->mockEventStorage), $mockStorageOptions)->will($this->returnValue($this->mockEventStorage));
        $this->mockObjectManager->expects($this->at(1))->method('get')->with(EventStore::class, $this->mockEventStorage)->will($this->returnValue($this->mockEventStore));

        $eventStoreManager->getEventStore('someStore');
        $eventStore = $eventStoreManager->getEventStore('someStore');

        $this->mockEventStorage->expects($this->once())->method('setup');
        $eventStore->setup();
    }

    /**
     * @test
     */
    public function eventStoresAreInstantiatedLazily()
    {
        $mockConfiguration = [
            'eventStore1' => [
                'storage' => 'EventStorage1',
                'boundedContexts' => [
                    '*' => true,
                ],
            ],
            'eventStore2' => [
                'storage' => 'EventStorage2',
                'boundedContexts' => [
                    'Foo' => true,
                ]
            ],
        ];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);

        $this->mockObjectManager->expects($this->atLeastOnce())->method('get')->will($this->returnCallback(function($className) {
            switch ($className) {
                case EventStore::class:
                    return $this->mockEventStore;
                case 'EventStorage2':
                    return $this->mockEventStorage;
                default:
                    $this->fail(sprintf('Class "%s" were not expected to be instantiated!', $className));
            }
            return '';
        }));

        $eventStoreManager->getEventStore('eventStore2');
    }

    public function getEventStoreForStreamNameDataProvider()
    {
        return [
            ['streamName' => '', 'expectedEventStore' => 'fallbackStore'],
            ['streamName' => 'Inactive', 'expectedEventStore' => 'fallbackStore'],
            ['streamName' => 'NoMatch', 'expectedEventStore' => 'fallbackStore'],
            ['streamName' => 'NoMatch:Foo', 'expectedEventStore' => 'fallbackStore'],
            ['streamName' => 'Bounded.Context1:Foo', 'expectedEventStore' => 'fallbackStore'],
            ['streamName' => 'Bounded.Context234:Foo', 'expectedEventStore' => 'fallbackStore'],
            ['streamName' => 'Bounded.Context2', 'expectedEventStore' => 'eventStore2'],
            ['streamName' => 'Bounded.Context2:Foo', 'expectedEventStore' => 'eventStore2'],
            ['streamName' => 'Bounded.Context23', 'expectedEventStore' => 'eventStore3'],
            ['streamName' => 'Bounded.Context23:Foo', 'expectedEventStore' => 'eventStore3'],
            ['streamName' => 'Bounded.Context2.Sub', 'expectedEventStore' => 'eventStore3'],
            ['streamName' => 'Bounded.Context2.Sub:Foo', 'expectedEventStore' => 'eventStore3'],
            ['streamName' => 'Bounded.Context3:Xyz', 'expectedEventStore' => 'eventStore3'],
        ];
    }

    /**
     * @param string $streamName
     * @param string $expectedEventStore
     * @test
     * @dataProvider getEventStoreForStreamNameDataProvider
     */
    public function getEventStoreForStreamNameTests(string $streamName, string $expectedEventStore)
    {
        $this->mockObjectManager->expects($this->atLeastOnce())->method('get')->will($this->returnCallback(function($className) use ($expectedEventStore) {
            $storageClassName = $expectedEventStore . 'Storage';
            switch ($className) {
                case EventStore::class:
                    return $this->mockEventStore;
                case $storageClassName;
                    return $this->mockEventStorage;
                default:
                    $this->fail(sprintf('Class "%s" were not expected to be instantiated, expected "%s" or "%s"', $className, $storageClassName, EventStore::class));
            }
            return '';
        }));

        $this->eventStoreManager->getEventStoreForStreamName($streamName);
    }

    /**
     * @test
     */
    public function getEventStoreForEventTypesReturnsFallbackEventStoreIfNoEventTypeIsGiven()
    {
        $this->mockObjectManager->expects($this->at(0))->method('get')->with('fallbackStoreStorage')->will($this->returnValue($this->mockEventStorage));
        $this->mockObjectManager->expects($this->at(1))->method('get')->with(EventStore::class, $this->mockEventStorage)->will($this->returnValue($this->mockEventStore));
        $eventStore = $this->eventStoreManager->getEventStoreForEventTypes([]);
        $this->assertSame($this->mockEventStore, $eventStore);
    }

    /**
     * @test
     * @expectedException \RuntimeException
     * @expectedExceptionCode 1492434719
     */
    public function getEventStoreForEventTypesThrowsExceptionIfGivenEventTypesResolveDifferentEventStores()
    {
        $this->mockObjectManager->expects($this->atLeastOnce())->method('get')->will($this->returnCallback(function($className) {
            switch ($className) {
                case EventStore::class:
                    return new EventStore($this->mockEventStorage);
                default:
                    return $this->mockEventStorage;
            }
        }));
        $this->eventStoreManager->getEventStoreForEventTypes(['Boundend.Context1:Foo', 'Bounded.Context2:Bar']);
    }

    public function getEventStoreForEventTypesDataProvider()
    {
        return [
            ['eventTypes' => [], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['NoMatch'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['Inactive'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['NoMatch:Foo'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['Bounded.Context1:Foo'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['NoMatch', 'Bounded.Context1:Foo'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['Bounded.Context1:Foo', 'NoMatch'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['Bounded.Context234:Foo'], 'expectedEventStore' => 'fallbackStore'],
            ['eventTypes' => ['Bounded.Context2'], 'expectedEventStore' => 'eventStore2'],
            ['eventTypes' => ['Bounded.Context2:Foo'], 'expectedEventStore' => 'eventStore2'],
            ['eventTypes' => ['Bounded.Context23'], 'expectedEventStore' => 'eventStore3'],
            ['eventTypes' => ['Bounded.Context23:Foo'], 'expectedEventStore' => 'eventStore3'],
            ['eventTypes' => ['Bounded.Context2.Sub'], 'expectedEventStore' => 'eventStore3'],
            ['eventTypes' => ['Bounded.Context2.Sub:Foo'], 'expectedEventStore' => 'eventStore3'],
            ['eventTypes' => ['Bounded.Context3:Xyz'], 'expectedEventStore' => 'eventStore3'],
        ];
    }

    /**
     * @param array $eventTypes
     * @param string $expectedEventStore
     * @test
     * @dataProvider getEventStoreForEventTypesDataProvider
     */
    public function getEventStoreForEventTypesTests(array $eventTypes, string $expectedEventStore)
    {
        $this->mockObjectManager->expects($this->atLeastOnce())->method('get')->will($this->returnCallback(function($className) use ($expectedEventStore) {
            $storageClassName = $expectedEventStore . 'Storage';
            switch ($className) {
                case EventStore::class:
                    return $this->mockEventStore;
                case $storageClassName;
                    return $this->mockEventStorage;
                default:
                    $this->fail(sprintf('Class "%s" were not expected to be instantiated, expected "%s" or "%s"', $className, $storageClassName, EventStore::class));
            }
            return '';
        }));

        $this->eventStoreManager->getEventStoreForEventTypes($eventTypes);
    }

    public function getEventStoreForBoundedContextDataProvider()
    {
        return [
            ['boundedContext' => '', 'expectedEventStore' => 'fallbackStore'],
            ['boundedContext' => 'NoMatch', 'expectedEventStore' => 'fallbackStore'],
            ['boundedContext' => 'Inactive', 'expectedEventStore' => 'fallbackStore'],
            ['boundedContext' => 'NoMatch:Foo', 'expectedEventStore' => 'fallbackStore'],
            ['boundedContext' => 'Bounded.Context1', 'expectedEventStore' => 'fallbackStore'],
            ['boundedContext' => 'Bounded.Context234', 'expectedEventStore' => 'fallbackStore'],
            ['boundedContext' => 'Bounded.Context2', 'expectedEventStore' => 'eventStore2'],
            ['boundedContext' => 'Bounded.Context23', 'expectedEventStore' => 'eventStore3'],
            ['boundedContext' => 'Bounded.Context2.Sub', 'expectedEventStore' => 'eventStore3'],
            ['boundedContext' => 'Bounded.Context3', 'expectedEventStore' => 'eventStore3'],
        ];
    }

    /**
     * @param string $boundedContext
     * @param string $expectedEventStore
     * @test
     * @dataProvider getEventStoreForBoundedContextDataProvider
     */
    public function getEventStoreForBoundedContextTests(string $boundedContext, string $expectedEventStore)
    {
        $this->mockObjectManager->expects($this->atLeastOnce())->method('get')->will($this->returnCallback(function($className) use ($expectedEventStore) {
            $storageClassName = $expectedEventStore . 'Storage';
            switch ($className) {
                case EventStore::class:
                    return $this->mockEventStore;
                case $storageClassName;
                    return $this->mockEventStorage;
                default:
                    $this->fail(sprintf('Class "%s" were not expected to be instantiated, expected "%s" or "%s"', $className, $storageClassName, EventStore::class));
            }
            return '';
        }));

        $this->eventStoreManager->getEventStoreForBoundedContext($boundedContext);
    }

    /**
     * @test
     * @expectedException \Neos\EventSourcing\EventStore\Exception\StorageConfigurationException
     */
    public function getAllEventStoresThrowsExceptionIfNoEventStoreIsConfigured()
    {
        $mockConfiguration = [];
        $eventStoreManager = new EventStoreManager($this->mockObjectManager, $mockConfiguration);
        $eventStoreManager->getAllEventStores();
    }

    /**
     * @test
     */
    public function getAllEventStoresInstantiatesAndReturnsAllConfiguredEventStores()
    {
        $mockEventStores = [
            'fallbackStore' => new EventStore($this->mockEventStorage),
            'eventStore2' => new EventStore($this->mockEventStorage),
            'eventStore3' => new EventStore($this->mockEventStorage),
        ];

        $this->mockObjectManager->expects($this->at(0))->method('get')->with('fallbackStoreStorage')->will($this->returnValue($this->mockEventStorage));
        $this->mockObjectManager->expects($this->at(1))->method('get')->with(EventStore::class)->will($this->returnValue($mockEventStores['fallbackStore']));
        $this->mockObjectManager->expects($this->at(2))->method('get')->with('eventStore2Storage')->will($this->returnValue($this->mockEventStorage));
        $this->mockObjectManager->expects($this->at(3))->method('get')->with(EventStore::class)->will($this->returnValue($mockEventStores['eventStore2']));
        $this->mockObjectManager->expects($this->at(4))->method('get')->with('eventStore3Storage')->will($this->returnValue($this->mockEventStorage));
        $this->mockObjectManager->expects($this->at(5))->method('get')->with(EventStore::class)->will($this->returnValue($mockEventStores['eventStore3']));

        $actualResult = $this->eventStoreManager->getAllEventStores();
        $this->assertSame($mockEventStores, $actualResult);
    }
}