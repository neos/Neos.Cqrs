<?php
namespace Neos\Cqrs\Message;

/*
 * This file is part of the Neos.Cqrs package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

/**
 * MessageIdentifierAwareInterface
 */
interface MessageIdentifierAwareInterface
{
    /**
     * Returns the message identifier
     *
     * @return string
     */
    public function getMessageIdentifier();
}
