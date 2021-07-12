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

/**
 * SnapshotAwareProjectorInterface
 */
class Snapshot
{
    /**
     * @var SnapshotIdentifier
     */
    private $snapshotIdentifier;

    /**
     * @var string
     */
    private $projectionIdentifier;

    /**
     * @var int
     */
    private $eventSequenceNumber;

    /**
     * @var \DateTimeImmutable
     */
    private $createdAt;

    /**
     * @param SnapshotIdentifier $snapshotIdentifier
     * @param string $projectionIdentifier
     * @param int $eventSequenceNumber
     * @param \DateTimeImmutable $createdAt
     */
    public function __construct(SnapshotIdentifier $snapshotIdentifier, string $projectionIdentifier, int $eventSequenceNumber, \DateTimeImmutable $createdAt)
    {
        $this->snapshotIdentifier = $snapshotIdentifier;
        $this->projectionIdentifier = $projectionIdentifier;
        $this->eventSequenceNumber = $eventSequenceNumber;
        $this->createdAt = $createdAt;
    }

    /**
     * @return SnapshotIdentifier
     */
    public function getSnapshotIdentifier(): SnapshotIdentifier
    {
        return $this->snapshotIdentifier;
    }

    /**
     * @return string
     */
    public function getProjectionIdentifier(): string
    {
        return $this->projectionIdentifier;
    }

    /**
     * @return int
     */
    public function getEventSequenceNumber(): int
    {
        return $this->eventSequenceNumber;
    }

    /**
     * @return \DateTimeImmutable
     */
    public function getCreatedAt(): \DateTimeImmutable
    {
        return $this->createdAt;
    }
}
