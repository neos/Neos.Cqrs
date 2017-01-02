<?php
namespace Neos\EventSourcing\Command;

/*
 * This file is part of the Neos.EventSourcing package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Neos\EventSourcing\Projection\InvalidProjectionIdentifierException;
use Neos\EventSourcing\Projection\Projection;
use Neos\EventSourcing\Projection\ProjectionManager;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Core\Booting\Scripts;

/**
 * CLI Command Controller for projection related commands
 *
 * @Flow\Scope("singleton")
 */
class ProjectionCommandController extends CommandController
{
    /**
     * @Flow\Inject
     * @var ProjectionManager
     */
    protected $projectionManager;

    /**
     * @Flow\InjectConfiguration(package="Neos.Flow")
     * @var array
     */
    protected $flowSettings;

    /**
     * @var array in the format ['<shortIdentifier>' => '<fullIdentifier>', ...]
     */
    private $shortProjectionIdentifiers;

    /**
     * List all projections
     *
     * This command displays a list of all projections and their respective short projection identifier which can
     * be used in the other projection commands.
     *
     * @return void
     */
    public function listCommand()
    {
        $lastPackageKey = null;
        foreach ($this->projectionManager->getProjections() as $projection) {
            $packageKey = $this->objectManager->getPackageKeyByObjectName($projection->getProjectorClassName());
            if ($packageKey !== $lastPackageKey) {
                $lastPackageKey = $packageKey;
                $this->outputLine();
                $this->outputLine('PACKAGE "%s":', array(strtoupper($packageKey)));
                $this->outputLine(str_repeat('-', $this->output->getMaximumLineLength()));
            }
            $this->outputLine('%-2s%-40s %s', array('', $this->getShortProjectionIdentifier($projection->getIdentifier()), $this->shortenText($projection->getProjectorClassName())));
        }
        $this->outputLine();
    }

    /**
     * Describe a projection
     *
     * This command displays detailed information about a specific projection, including the projector class name
     * and the event types which are processed by this projector.
     *
     * @param string $projection The projection identifier; see projection:list for possible options
     * @return void
     * @see neos.eventsourcing:projection:list
     */
    public function describeCommand($projection)
    {
        $projectionDto = $this->resolveProjectionOrQuit($projection);

        $this->outputLine('<b>PROJECTION:</b>');
        $this->outputLine('  <i>%s</i>', [$projectionDto->getIdentifier()]);
        $this->outputLine();
        $this->outputLine('<b>REPLAY:</b>');
        $this->outputLine('  %s projection:replay %s', [$this->getFlowInvocationString(), $this->getShortProjectionIdentifier($projectionDto->getIdentifier())]);
        $this->outputLine();
        $this->outputLine('<b>PROJECTOR:</b>');
        $this->outputLine('  %s', [$projectionDto->getProjectorClassName()]);
        $this->outputLine();

        $this->outputLine('<b>HANDLED EVENT TYPES:</b>');
        foreach ($projectionDto->getEventTypes() as $eventType) {
            $this->outputLine('  * %s', [$eventType]);
        }
    }

    /**
     * Replay a projection
     *
     * This command allows you to replay all relevant events for one specific projection.
     *
     * @param string $projection The projection identifier; see projection:list for possible options
     * @param bool $quiet If specified, this command won't produce any output apart from errors (useful for automation)
     * @return void
     * @see neos.eventsourcing:projection:list
     * @see neos.eventsourcing:projection:replayall
     */
    public function replayCommand($projection, $quiet = false)
    {
        $projectionDto = $this->resolveProjectionOrQuit($projection);

        if (!$quiet) {
            $this->outputLine('Replaying events for projection "%s" ...', [$projectionDto->getIdentifier()]);
            $this->output->progressStart();
        }
        $eventsCount = 0;
        $this->projectionManager->replay($projectionDto->getIdentifier(), function () use (&$eventsCount, $quiet) {
            $eventsCount ++;
            if (!$quiet) {
                $this->output->progressAdvance();
            }
        });
        if (!$quiet) {
            $this->output->progressFinish();
            $this->outputLine('Replayed %s events.', [$eventsCount]);
        }
    }

    /**
     * Replay all projections
     *
     * This command allows you to replay all relevant events for all known projections.
     *
     * @param bool $onlyEmpty If specified, only projections which are currently empty will be considered
     * @param bool $quiet If specified, this command won't produce any output apart from errors (useful for automation)
     * @return void
     * @see neos.eventsourcing:projection:replay
     * @see neos.eventsourcing:projection:list
     */
    public function replayAllCommand($onlyEmpty = false, $quiet = false)
    {
        if (!$quiet) {
            $this->outputLine('Replaying all%s projections', [$onlyEmpty ? ' empty' : '']);
        }
        $eventsCount = 0;
        foreach ($this->projectionManager->getProjections() as $projection) {
            if ($onlyEmpty && !$this->projectionManager->isProjectionEmpty($projection->getIdentifier())) {
                if (!$quiet) {
                    $this->outputLine('Skipping non-empty projection "%s" ...', [$projection->getIdentifier()]);
                }
                continue;
            }
            if (!$quiet) {
                $this->outputLine('Replaying events for projection "%s" ...', [$projection->getIdentifier()]);
                $this->output->progressStart();
            }
            $this->projectionManager->replay($projection->getIdentifier(), function () use (&$eventsCount, $quiet) {
                $eventsCount++;
                if (!$quiet) {
                    $this->output->progressAdvance();
                }
            });
            if (!$quiet) {
                $this->output->progressFinish();
            }
        }
        if (!$quiet) {
            $this->outputLine('Replayed %d events.', [$eventsCount]);
        }
    }

    /**
     * Forward new events to a projection
     *
     * This command allows you to play all relevant unseen events for one specific projection.
     *
     * @param string $projection The projection identifier; see projection:list for possible options
     * @param bool $quiet If specified, this command won't produce any output apart from errors (useful for automation)
     * @return void
     * @see neos.eventsourcing:projection:list
     * @see neos.eventsourcing:projection:replay
     */
    public function catchUpCommand($projection, $quiet = false)
    {
        $projectionDto = $this->resolveProjectionOrQuit($projection);
        if (!$quiet) {
            $this->outputLine('Catching up projection "%s" ...', [$projectionDto->getIdentifier()]);
            $this->output->progressStart();
        }
        $eventsCount = 0;
        $this->projectionManager->catchUp($projectionDto->getIdentifier(), function () use (&$eventsCount, $quiet) {
            $eventsCount ++;
            if (!$quiet) {
                $this->output->progressAdvance();
            }
        });
        if (!$quiet) {
            $this->output->progressFinish();
            $this->outputLine('Applied %d events.', [$eventsCount]);
        }
    }

    /**
     * Listen to new events for a given (asynchronous) projection
     *
     * @param string $projection The projection identifier; see projection:list for possible options
     * @param int $lookupInterval Pause between lookups (in seconds)
     * @param bool $quiet If specified, this command won't produce any output apart from errors (useful for automation)
     * @return void
     * @see neos.eventsourcing:projection:list
     * @see neos.eventsourcing:projection:catchup
     */
    public function watchCommand($projection, $lookupInterval = 10, $quiet = false)
    {
        $projectionDto = $this->resolveProjectionOrQuit($projection);

        if (!$quiet) {
            $this->outputLine('Watching events for projection "%s" ...', [$projectionDto->getIdentifier()]);
        }
        do {
            $catchupCommandArguments = ['projection' => $projectionDto->getIdentifier()];
            if ($quiet) {
                $catchupCommandArguments['quiet'] = true;
            }
            Scripts::executeCommand('neos.eventsourcing:projection:catchup', $this->flowSettings, !$quiet, $catchupCommandArguments);
            if (!$quiet) {
                $this->outputLine();
            }
            sleep($lookupInterval);
        } while (true);
    }

    /**
     * Returns the shortest unambiguous projection identifier for a given $fullProjectionIdentifier
     *
     * @param string $fullProjectionIdentifier
     * @return string
     */
    private function getShortProjectionIdentifier(string $fullProjectionIdentifier)
    {
        if ($this->shortProjectionIdentifiers === null) {
            $projectionsByName = $projectionIdentifiers = [];
            foreach ($this->projectionManager->getProjections() as $projection) {
                $projectionIdentifiers[] = $projection->getIdentifier();
                list($packageKey, $projectionName) = explode(':', $projection->getIdentifier());
                if (!isset($projectionsByName[$projectionName])) {
                    $projectionsByName[$projectionName] = [];
                }
                $projectionsByName[$projectionName][] = $packageKey;
            }
            $this->shortProjectionIdentifiers = [];
            foreach ($projectionIdentifiers as $projectionIdentifier) {
                list($packageKey, $projectionName) = explode(':', $projectionIdentifier);
                if (count($projectionsByName[$projectionName]) === 1) {
                    $this->shortProjectionIdentifiers[$projectionIdentifier] = $projectionName;
                    continue;
                }
                $prefix = null;
                foreach (array_reverse(explode('.', $packageKey)) as $packageKeyPart) {
                    $prefix = $prefix === null ? $packageKeyPart : $packageKeyPart . '.' . $prefix;
                    $matchingPackageKeys = array_filter($projectionsByName[$projectionName], function ($searchedPackageKey) use ($packageKey) {
                        return $searchedPackageKey === $packageKey || substr($packageKey, -(strlen($searchedPackageKey) + 1)) === '.' . $searchedPackageKey;
                    });
                    if (count($matchingPackageKeys) === 1) {
                        $this->shortProjectionIdentifiers[$projectionIdentifier] = $prefix . ':' . $projectionName;
                        break;
                    }
                }
            }
        }
        return isset($this->shortProjectionIdentifiers[$fullProjectionIdentifier]) ? $this->shortProjectionIdentifiers[$fullProjectionIdentifier] : $fullProjectionIdentifier;
    }

    /**
     * Wrapper around ProjectionManager::getProjection() to render nicer error messages in case the $projectionIdentifier
     * is not valid.
     *
     * @param string $projectionIdentifier
     * @return Projection
     */
    private function resolveProjectionOrQuit($projectionIdentifier): Projection
    {
        try {
            return $this->projectionManager->getProjection($projectionIdentifier);
        } catch (InvalidProjectionIdentifierException $exception) {
            $this->outputLine('<error>%s</error>', [$exception->getMessage()]);
            $this->quit(1);
            return null;
        }
    }

    /**
     * Shortens the given text by removing characters from the middle
     *
     * @param string $text Text to shorten
     * @param int $maximumCharacters Maximum of characters
     * @return string
     */
    private function shortenText($text, $maximumCharacters = 36)
    {
        $length = strlen($text);
        if ($length <= $maximumCharacters) {
            return $text;
        }
        return substr_replace($text, '...', ($maximumCharacters - 3) / 2, $length - $maximumCharacters + 3);
    }
}
