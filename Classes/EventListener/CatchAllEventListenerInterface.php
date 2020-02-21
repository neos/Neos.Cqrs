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

/**
 * Marker interface for Event Listeners (including Projectors) that handle all event types
 *
 * Usage:
 *
 *  final class SomeLogProjector implements ProjectorInterface, CatchAllEventListener, BeforeInvokeInterface
 *  {
 *
 *    public function beforeInvoke(EventEnvelope $eventEnvelope): void
 *    {
 *      // process $eventEnvelope->getRawEvent() or $eventEnvelope->getDomainEvent()
 *    }
 *
 *    public function whenSomeEventType(SomeEventType $event): void
 *    {
 *      // this method CAN still be implemented and will be invoked for the corresponding event type
 *    }
 *
 *    // ...
 *  }
 */
interface CatchAllEventListenerInterface
{
}
