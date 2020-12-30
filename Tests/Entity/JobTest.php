<?php

/*
 * Copyright 2012 Johannes M. Schmitt <schmittjoh@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace JMS\JobQueueBundle\Tests\Entity;

use JMS\JobQueueBundle\Entity\Job;
use PHPUnit\Framework\TestCase;

class JobTest extends TestCase
{
    public function testConstruct(): Job
    {
        $job = new Job('a:b', array('a', 'b', 'c'));

        self::assertEquals('a:b', $job->getCommand());
        self::assertEquals(array('a', 'b', 'c'), $job->getArgs());
        self::assertNotNull($job->getCreatedAt());
        self::assertEquals('pending', $job->getState());
        self::assertNull($job->getStartedAt());

        return $job;
    }

    /**
     * @depends testConstruct
     *
     */
    public function testInvalidTransition(Job $job): void
    {
        $this->expectException(\JMS\JobQueueBundle\Exception\InvalidStateTransitionException::class);
        $job->setState('failed');
    }

    /**
     * @depends testConstruct
     */
    public function testStateToRunning(Job $job): Job
    {
        $job->setState('running');
        self::assertEquals('running', $job->getState());
        self::assertNotNull($startedAt = $job->getStartedAt());
        $job->setState('running');
        self::assertSame($startedAt, $job->getStartedAt());

        return $job;
    }

    /**
     * @depends testStateToRunning
     */
    public function testStateToFailed(Job $job): void
    {
        $job = clone $job;
        $job->setState('running');
        $job->setState('failed');
        self::assertEquals('failed', $job->getState());
    }

    /**
     * @depends testStateToRunning
     */
    public function testStateToTerminated(Job $job): void
    {
        $job = clone $job;
        $job->setState('running');
        $job->setState('terminated');
        self::assertEquals('terminated', $job->getState());
    }

    /**
     * @depends testStateToRunning
     */
    public function testStateToFinished(Job $job): void
    {
        $job = clone $job;
        $job->setState('running');
        $job->setState('finished');
        self::assertEquals('finished', $job->getState());
    }

    public function testAddOutput(): void
    {
        $job = new Job('foo');
        self::assertNull($job->getOutput());
        $job->addOutput('foo');
        self::assertEquals('foo', $job->getOutput());
        $job->addOutput('bar');
        self::assertEquals('foobar', $job->getOutput());
    }

    public function testAddErrorOutput(): void
    {
        $job = new Job('foo');
        self::assertNull($job->getErrorOutput());
        $job->addErrorOutput('foo');
        self::assertEquals('foo', $job->getErrorOutput());
        $job->addErrorOutput('bar');
        self::assertEquals('foobar', $job->getErrorOutput());
    }

    public function testSetOutput(): void
    {
        $job = new Job('foo');
        self::assertNull($job->getOutput());
        $job->setOutput('foo');
        self::assertEquals('foo', $job->getOutput());
        $job->setOutput('bar');
        self::assertEquals('bar', $job->getOutput());
    }

    public function testSetErrorOutput(): void
    {
        $job = new Job('foo');
        self::assertNull($job->getErrorOutput());
        $job->setErrorOutput('foo');
        self::assertEquals('foo', $job->getErrorOutput());
        $job->setErrorOutput('bar');
        self::assertEquals('bar', $job->getErrorOutput());
    }

    public function testAddDependency(): void
    {
        $a = new Job('a');
        $b = new Job('b');
        self::assertCount(0, $a->getDependencies());
        self::assertCount(0, $b->getDependencies());

        $a->addDependency($b);
        self::assertCount(1, $a->getDependencies());
        self::assertCount(0, $b->getDependencies());
        self::assertSame($b, $a->getDependencies()->first());
    }

    public function testAddDependencyToRunningJob(): void
    {
        $this->expectExceptionMessage("You cannot add dependencies to a job which might have been started already.");
        $this->expectException(\LogicException::class);
        $job = new Job('a');
        $job->setState(Job::STATE_RUNNING);
        $this->setField($job, 'id', 1);
        $job->addDependency(new Job('b'));
    }

    public function testAddRetryJob(): Job
    {
        $a = new Job('a');
        $a->setState(Job::STATE_RUNNING);
        $b = new Job('b');
        $a->addRetryJob($b);

        self::assertCount(1, $a->getRetryJobs());
        self::assertSame($b, $a->getRetryJobs()->get(0));

        return $a;
    }

    /**
     * @depends testAddRetryJob
     */
    public function testIsRetryJob(Job $a): void
    {
        self::assertFalse($a->isRetryJob());
        self::assertTrue($a->getRetryJobs()->get(0)->isRetryJob());
    }

    /**
     * @depends testAddRetryJob
     */
    public function testGetOriginalJob(Job $a): void
    {
        self::assertSame($a, $a->getOriginalJob());
        self::assertSame($a, $a->getRetryJobs()->get(0)->getOriginalJob());
    }

    public function testCheckedAt(): void
    {
        $job = new Job('a');
        self::assertNull($job->getCheckedAt());

        $job->checked();
        self::assertInstanceOf('DateTime', $checkedAtA = $job->getCheckedAt());

        $job->checked();
        self::assertInstanceOf('DateTime', $checkedAtB = $job->getCheckedAt());
        self::assertNotSame($checkedAtA, $checkedAtB);
    }

    public function testSameDependencyIsNotAddedTwice(): void
    {
        $a = new Job('a');
        $b = new Job('b');

        self::assertCount(0, $a->getDependencies());
        $a->addDependency($b);
        self::assertCount(1, $a->getDependencies());
        $a->addDependency($b);
        self::assertCount(1, $a->getDependencies());
    }

    public function testHasDependency(): void
    {
        $a = new Job('a');
        $b = new Job('b');

        self::assertFalse($a->hasDependency($b));
        $a->addDependency($b);
        self::assertTrue($a->hasDependency($b));
    }

    public function testIsRetryAllowed(): void
    {
        $job = new Job('a');
        self::assertFalse($job->isRetryAllowed());

        $job->setMaxRetries(1);
        self::assertTrue($job->isRetryAllowed());

        $job->setState('running');
        $retry = new Job('a');
        $job->addRetryJob($retry);
        self::assertFalse($job->isRetryAllowed());
    }

    public function testCloneDoesNotChangeQueue(): void
    {
        $job = new Job('a', array(), true, 'foo');
        $clonedJob = clone $job;

        self::assertEquals('foo', $clonedJob->getQueue());
    }

    private function setField($obj, $field, $value): void
    {
        $ref = new \ReflectionProperty($obj, $field);
        $ref->setAccessible(true);
        $ref->setValue($obj, $value);
    }
}