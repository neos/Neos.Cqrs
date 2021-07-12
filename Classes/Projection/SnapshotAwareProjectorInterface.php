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
interface SnapshotAwareProjectorInterface extends ProjectorInterface
{
    /**
     * @param SnapshotIdentifier $snapshotIdentifier
     * @return int The event sequence number at which the snapshot was created
     */
    public function createSnapshot(SnapshotIdentifier $snapshotIdentifier): int;
}
