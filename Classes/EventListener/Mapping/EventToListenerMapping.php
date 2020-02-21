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

use Neos\Flow\Annotations as Flow;

/**
 * A Mapping from an Event Class name to the corresponding Event listener, potentially with custom options
 *
 * @Flow\Proxy(false)
 */
final class EventToListenerMapping implements \JsonSerializable
{
    /**
     * @var string
     */
    private $eventClassNamePattern;

    /**
     * @var string
     */
    private $listenerClassName;

    /**
     * @var array
     */
    private $options;

    private function __construct(string $eventClassNamePattern, string $listenerClassName, array $options)
    {
        $this->eventClassNamePattern = $eventClassNamePattern;
        $this->listenerClassName = $listenerClassName;
        $this->options = $options;
    }

    public static function create(string $eventClassNamePattern, string $listenerClassName, array $options): self
    {
        return new static($eventClassNamePattern, $listenerClassName, $options);
    }

    public function getEventClassNamePattern(): string
    {
        return $this->eventClassNamePattern;
    }

    public function matchesEventClassName(string $eventClassName): bool
    {
        $pattern = '/^' . str_replace('\\', '\\\\', $this->eventClassNamePattern) . '$/';
        return preg_match($pattern, $eventClassName) === 1;
    }

    public function getListenerClassName(): string
    {
        return $this->listenerClassName;
    }

    public function getOptions(): array
    {
        return $this->options;
    }

    public function getOption(string $optionName, $defaultValue)
    {
        return $this->options[$optionName] ?? $defaultValue;
    }

    /**
     * @inheritDoc
     */
    public function jsonSerialize()
    {
        return [
            'eventClassNamePattern' => $this->eventClassNamePattern,
            'listenerClassName' => $this->listenerClassName,
            'options' => $this->options,
        ];
    }
}
