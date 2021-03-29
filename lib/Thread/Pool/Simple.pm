package Thread::Pool::Simple;
use 5.16.1;
use constant {
	END_OF_QUEUE 		=> 'END_OF_QUEUE',
	WAIT_BTW_FIN_ITER 	=>  0.08,
};
use threads;
use threads::shared;
use Carp qw(confess);
use List::Util qw(any all);
use AnyEvent::Loop;
use AnyEvent;
use Thread::Queue;
use Log::Any qw($logger);

sub new {
	my ($class, %pars) = @_;
	my $self = bless {}, ref($class) || $class;
	ref($self->{'work_sub'} = $pars{'work_sub'} // $pars{'do'}) eq 'CODE'
		or confess sprintf 'you must specify "work_sub" or "do" as a %s constructor parameter and it must be coderef', __PACKAGE__;

	my $n_threads = $self->{'n_threads'} = $pars{'workers'} // $ENV{'NUMBER_OF_PROCESSORS'};
	$self->{'tq_jobs'} = $pars{'tq_jobs'} // $self->__create_jobs_queue;
	$self->{'tq_res'}  = $pars{'tq_res'} // $self->__create_results_queue;
	if ( exists $pars{'jobs'} ) {
		my $jobs = $pars{'jobs'};
		$self->{'tq_jobs'}->enqueue( ref $jobs eq 'ARRAY' ? @{$jobs} : ($jobs) )
	}
	$self->{'threads_pool'} = [ map {
		threads->create(\&__thread_work, $self)
	} 1..$n_threads ];
	$self->{'on_finish'} = $pars{'on_finish'} // ( 
								$pars{'finalize'} eq 'CODE' ? $pars{'finalize'} : undef
							);
	$self->finalize if $pars{'finalize'};
	$self
}

sub results_queue {
	$_[0]{'tq_res'}
}

sub jobs_queue {
	$_[0]{'tq_jobs'}
}

sub __create_jobs_queue {
	Thread::Queue->new
}

sub __create_results_queue {
	Thread::Queue->new
}

sub results_queue {
	$_[0]->{'tq_res'}
}

sub __thread_work {
	my $self = $_[0];
	my $fl_exit_if_no_jobs = $self->{'finalize'};
	$logger->debug('i am thread #' . threads->tid);
	my $tq_jobs = $self->{'tq_jobs'};
	while (defined $tq_jobs->pending) {
		# We dont expect that dequeue will be non-blocked (that's why we don't check for $tq_jobs->pending > 0).
		# Blocking here is a normal operation!
		my $job = $tq_jobs->dequeue;
		defined $job and ! ref($job) and $job eq END_OF_QUEUE
			and last;
		my @job_results = eval { $self->{'work_sub'}->($job) };
		if ( my $err = $@ ) {
			$logger->error(sprintf 'Error in thread #%d: %s', threads->tid, $err)
		} elsif ( @job_results ) {
			$self->results_queue->enqueue( @job_results )
		} else {
			$logger->warn(sprintf 'No results for job [%s] returned', $job)
		}
	}
}

sub post_jobs {
	my $self = shift;
	$self->{'finalizing'} and return('Thread pool finalization in progress', undef);
	$self->{'finalized'}  and return('Thread pool was finalized', undef);
	@_ or return ('Where is your jobs?', undef);
	$self->{'tq_jobs'}->enqueue(@_);
	(undef, scalar @_)
}

sub finalize {
	my $self = $_[0];
	return if $self->{'finalizing'}++ or $self->{'finalized'};

	$self->{'tq_jobs'}->end;

	my $cb = $self->{'on_finish'};
	my $fl_dont_recv = ref($cb) eq 'CODE';

	my $thr_pool = $self->{'threads_pool'};
	my $cv = AE::cv;
	$self->{'aeh'}{'wait4thr_fin'} = AE::timer(
		0, WAIT_BTW_FIN_ITER 
		=> sub {
			all { $_->is_joinable } @{$thr_pool} or return;
			undef $self->{'aeh'}{'wait4thr_fin'};
			$self->{'finalized'} = 1;  undef $self->{'finalizing'};
			$self->{'tq_res'}->end;
			$_->join for @{$thr_pool};
			if ( $fl_dont_recv ) {
				$cb->( $self )
			} else {
				$cv->send
			}
		});
	$fl_dont_recv ? (1) : $cv->recv
}

sub results {
	my $self = $_[0];
	my $tq_res = $self->{'tq_res'};
	my @results;
	while (my $res = $tq_res->dequeue_nb) {
		push @results, $res
	}
	return @results
}

sub DESTROY {
	my $self = $_[0];
	return if $self->{'destroyed'}++;
	defined $_ and defined $_->pending and $_->end for @{$self->{qw[tq_jobs tq_res]}};
	$_->is_joinable ? $_->join : $_->detach for @{$_[0]->{'threads_pool'}};
}

1;
