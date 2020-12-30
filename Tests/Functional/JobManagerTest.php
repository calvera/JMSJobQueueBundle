<?php

namespace JMS\JobQueueBundle\Tests\Functional;

use JMS\JobQueueBundle\Retry\ExponentialRetryScheduler;
use JMS\JobQueueBundle\Retry\RetryScheduler;
use JMS\JobQueueBundle\Tests\Functional\TestBundle\Entity\Train;

use JMS\JobQueueBundle\Tests\Functional\TestBundle\Entity\Wagon;

use PHPUnit\Framework\Constraint\LogicalNot;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Doctrine\ORM\EntityManager;
use JMS\JobQueueBundle\Entity\Repository\JobManager;
use JMS\JobQueueBundle\Event\StateChangeEvent;
use JMS\JobQueueBundle\Entity\Job;

class JobManagerTest extends BaseTestCase
{
    /** @var EntityManager */
    private $em;

    /** @var JobManager */
    private $jobManager;

    /** @var EventDispatcher */
    private $dispatcher;

    public function testGetOne(): void
    {
        $a = new Job('a', array('foo'));
        $a2 = new Job('a');
        $this->em->persist($a);
        $this->em->persist($a2);
        $this->em->flush();

        self::assertSame($a, $this->jobManager->getJob('a', array('foo')));
        self::assertSame($a2, $this->jobManager->getJob('a'));
    }

    public function testGetOneThrowsWhenNotFound(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage("Found no job for command");
        $this->jobManager->getJob('foo');
    }

    public function getOrCreateIfNotExists(): void
    {
        $a = $this->jobManager->getOrCreateIfNotExists('a');
        self::assertSame($a, $this->jobManager->getOrCreateIfNotExists('a'));
        self::assertNotSame($a, $this->jobManager->getOrCreateIfNotExists('a', array('foo')));
    }

    public function testFindPendingJobReturnsAllDependencies(): void
    {
        $a = new Job('a');
        $b = new Job('b');

        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        $c = new Job('c');
        $c->addDependency($a);
        $c->addDependency($b);
        $this->em->persist($c);
        $this->em->flush();
        $this->em->clear();

        $cReloaded = $this->jobManager->findPendingJob(array($a->getId(), $b->getId()));
        self::assertNotNull($cReloaded);
        self::assertEquals($c->getId(), $cReloaded->getId());
        self::assertCount(2, $cReloaded->getDependencies());
    }

    public function testFindPendingJob(): void
    {
        self::assertNull($this->jobManager->findPendingJob());

        $a = new Job('a');
        $a->setState('running');
        $b = new Job('b');
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        self::assertSame($b, $this->jobManager->findPendingJob());
        self::assertNull($this->jobManager->findPendingJob(array($b->getId())));
    }

    public function testFindPendingJobInRestrictedQueue(): void
    {
        self::assertNull($this->jobManager->findPendingJob());

        $a = new Job('a');
        $b = new Job('b', array(), true, 'other_queue');
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        self::assertSame($a, $this->jobManager->findPendingJob());
        self::assertSame($b, $this->jobManager->findPendingJob(array(), array(), array('other_queue')));
    }

    public function testFindStartableJob(): void
    {
        self::assertNull($this->jobManager->findStartableJob('my-name'));

        $a = new Job('a');
        $a->setState('running');
        $b = new Job('b');
        $c = new Job('c');
        $b->addDependency($c);
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->persist($c);
        $this->em->flush();

        $excludedIds = array();

        self::assertSame($c, $this->jobManager->findStartableJob('my-name', $excludedIds));
        self::assertEquals(array($b->getId()), $excludedIds);
    }

    public function testFindJobByRelatedEntity(): void
    {
        $a = new Job('a');
        $b = new Job('b');
        $b->addRelatedEntity($a);
        $b2 = new Job('b');
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->persist($b2);
        $this->em->flush();
        $this->em->clear();

        self::assertFalse($this->em->contains($b));

        $reloadedB = $this->jobManager->findJobForRelatedEntity('b', $a);
        self::assertNotNull($reloadedB);
        self::assertEquals($b->getId(), $reloadedB->getId());
        self::assertCount(1, $reloadedB->getRelatedEntities());
        self::assertEquals($a->getId(), $reloadedB->getRelatedEntities()->first()->getId());
    }

    public function testFindStartableJobDetachesNonStartableJobs(): void
    {
        $a = new Job('a');
        $b = new Job('b');
        $a->addDependency($b);
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        self::assertTrue($this->em->contains($a));
        self::assertTrue($this->em->contains($b));

        $excludedIds = array();
        $startableJob = $this->jobManager->findStartableJob('my-name', $excludedIds);
        self::assertNotNull($startableJob);
        self::assertEquals($b->getId(), $startableJob->getId());
        self::assertEquals(array($a->getId()), $excludedIds);
        self::assertFalse($this->em->contains($a));
        self::assertTrue($this->em->contains($b));
    }

    public function testCloseJob(): void
    {
        $a = new Job('a');
        $a->setState('running');
        $b = new Job('b');
        $b->addDependency($a);
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        $this->dispatcher->expects(self::at(0))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new StateChangeEvent($a, 'terminated'));
        $this->dispatcher->expects(self::at(1))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new StateChangeEvent($b, 'canceled'));

        self::assertEquals('running', $a->getState());
        self::assertEquals('pending', $b->getState());
        $this->jobManager->closeJob($a, 'terminated');
        self::assertEquals('terminated', $a->getState());
        self::assertEquals('canceled', $b->getState());
    }

    public function testCloseJobDoesNotCreateRetryJobsWhenCanceled(): void
    {
        $a = new Job('a');
        $a->setMaxRetries(4);
        $b = new Job('b');
        $b->setMaxRetries(4);
        $b->addDependency($a);
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        $this->dispatcher->expects(self::at(0))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new StateChangeEvent($a, 'canceled'));

        $this->dispatcher->expects(self::at(1))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new StateChangeEvent($b, 'canceled'));

        $this->jobManager->closeJob($a, 'canceled');
        self::assertEquals('canceled', $a->getState());
        self::assertCount(0, $a->getRetryJobs());
        self::assertEquals('canceled', $b->getState());
        self::assertCount(0, $b->getRetryJobs());
    }

    public function testCloseJobDoesNotCreateMoreThanAllowedRetries(): void
    {
        $a = new Job('a');
        $a->setMaxRetries(2);
        $a->setState('running');
        $this->em->persist($a);
        $this->em->flush();

        $this->dispatcher->expects(self::at(0))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new StateChangeEvent($a, 'failed'));
        $this->dispatcher->expects(self::at(1))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new LogicalNot(self::equalTo(new StateChangeEvent($a, 'failed'))));
        $this->dispatcher->expects(self::at(2))
            ->method('dispatch')
            ->with('jms_job_queue.job_state_change', new LogicalNot(self::equalTo(new StateChangeEvent($a, 'failed'))));

        self::assertCount(0, $a->getRetryJobs());
        $this->jobManager->closeJob($a, 'failed');
        self::assertEquals('running', $a->getState());
        self::assertCount(1, $a->getRetryJobs());

        $a->getRetryJobs()->first()->setState('running');
        $this->jobManager->closeJob($a->getRetryJobs()->first(), 'failed');
        self::assertCount(2, $a->getRetryJobs());
        self::assertEquals('failed', $a->getRetryJobs()->first()->getState());
        self::assertEquals('running', $a->getState());

        $a->getRetryJobs()->last()->setState('running');
        $this->jobManager->closeJob($a->getRetryJobs()->last(), 'terminated');
        self::assertCount(2, $a->getRetryJobs());
        self::assertEquals('terminated', $a->getRetryJobs()->last()->getState());
        self::assertEquals('terminated', $a->getState());

        $this->em->clear();
        $reloadedA = $this->em->find('JMSJobQueueBundle:Job', $a->getId());
        self::assertCount(2, $reloadedA->getRetryJobs());
    }

    public function testModifyingRelatedEntity(): void
    {
        $wagon = new Wagon();
        $train = new Train();
        $wagon->train = $train;

        $defEm = self::$kernel->getContainer()->get('doctrine')->getManager('default');
        $defEm->persist($wagon);
        $defEm->persist($train);
        $defEm->flush();

        $j = new Job('j');
        $j->addRelatedEntity($wagon);
        $this->em->persist($j);
        $this->em->flush();

        $defEm->clear();
        $this->em->clear();
        self::assertNotSame($defEm, $this->em);

        $reloadedJ = $this->em->find('JMSJobQueueBundle:Job', $j->getId());

        $reloadedWagon = $reloadedJ->findRelatedEntity('JMS\JobQueueBundle\Tests\Functional\TestBundle\Entity\Wagon');
        $reloadedWagon->state = 'broken';
        $defEm->persist($reloadedWagon);
        $defEm->flush();

        self::assertTrue($defEm->contains($reloadedWagon->train));
    }

    protected function setUp(): void
    {
        self::createClient();
        $this->importDatabaseSchema();

        $this->dispatcher = $this->createMock('Symfony\Component\EventDispatcher\EventDispatcherInterface');
        $this->em = self::$kernel->getContainer()->get('doctrine')->getManagerForClass(Job::class);
        $this->jobManager = new JobManager(
            self::$kernel->getContainer()->get('doctrine'),
            $this->dispatcher,
            new ExponentialRetryScheduler()
        );
    }
}