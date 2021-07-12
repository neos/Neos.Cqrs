<?php
declare(strict_types=1);
namespace Neos\EventSourcing\Projection;

/*
 * This file is part of the Neos.EventSourcing package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Ramsey\Uuid\Uuid;

/**
 * SnapshotIdentifier
 */
class SnapshotIdentifier
{

    /**
     * @var string
     */
    private $snapshotIdentifier;

    /**
     * @param string $snapshotIdentifier
     */
    public function __construct(string $snapshotIdentifier)
    {
        $this->setSnapshotIdentifier($snapshotIdentifier);
    }

    /**
     * @param string $snapshotIdentifier
     */
    private function setSnapshotIdentifier(string $snapshotIdentifier): void
    {
        if (preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/', $snapshotIdentifier) !== 1) {
            throw new \InvalidArgumentException(sprintf('Invalid snapshot identifier "%s"', $snapshotIdentifier), 1598000542);
        }
        $this->snapshotIdentifier = $snapshotIdentifier;
    }

    /**
     * Create a SnapshotIdentifier object from the given string
     *
     * @param string $snapshotIdentifier
     * @return static
     */
    public static function fromString(string $snapshotIdentifier): self
    {
        return new static($snapshotIdentifier);
    }

    /**
     * @return static
     * @throws
     */
    public static function fromRandom(): self
    {
        return self::fromString(Uuid::uuid4()->toString());
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->snapshotIdentifier;
    }
}
