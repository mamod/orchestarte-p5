package Orchestrate;
use strict;
use warnings;
use Carp;
use HTTP::Tiny;
use JSON::MaybeXS;
use URI::Escape;
use Data::Dumper;

our $VERSION = '0.0.1';

my $ApiEndPoint = 'api.orchestrate.io';
my $ApiProtocol = 'https:';
my $ApiVersion  = 'v0';

sub assert {
	my $test = shift;
	my $error = shift;
	croak $error if !$test;
}


sub new {
	my ($class, $token, $apiEndpoint) = @_;
	my $self = bless {}, $class;
	assert($token, 'API key required.');

	$self->{contentType}  = 'application/json';
	$self->{_token}       = $token;
	$self->{_apiEndPoint} = $apiEndpoint || $ApiEndPoint;
	$self->{_userAgent}   = 'orchestrate.pm/' . $VERSION;
	$self->{http}         = HTTP::Tiny->new( keep_alive => 1 );
	$self->{json}         = JSON::MaybeXS->new({ utf8 => 1 });
	return $self;
}


sub _request {
	my ($self, $method, $url, $data, $headers) = @_;

	$headers = $headers || {};

	if (!$headers->{'Content-Type'}) {
		$headers->{'Content-Type'} = $self->{contentType}
	}

	$headers->{'User-Agent'} = $self->{_userAgent};

	# $headers->{'WWW-Authenticate'}
	my $res = $self->{http}->request($method, $url, {
		auth   => { user => $self->{_token} },
		headers => $headers,
		content => defined $data ? $self->{json}->encode($data) : ''
	});

	if ($res->{content} && $res->{content} ne ''){
		$res->{body} = $self->{json}->decode($res->{content});
	}
	return $res;
}


sub _head {
	my ($self, $url) = @_;
	return $self->_request('HEAD', $url);
}


sub _post {
	my ($self, $url, $data, $header) = @_;
	return $self->_request('POST', $url, $data, $header);
}


sub _get {
	my ($self, $url) = @_;
	return $self->_request('GET', $url);
}


sub ping {
	my $self = shift;
	return $self->_head($self->generateApiUrl());
}


sub post {
	my ($self, $collection, $data) = @_;
	assert($collection && ref $data, 'Collection and ref object data required.');
	return $self->_post($self->generateApiUrl([$collection]), $data);
}


sub get {
	my ($self, $collection, $key, $ref, $params) = @_;
	assert($collection && $key, 'Collection and key required.');
	if (!$ref) {
		return $self->_get($self->generateApiUrl([$collection, $key], $params));
	} else {
		return $self->_get($self->generateApiUrl([$collection, $key, 'refs', $ref], $params));
	}
}


sub generateApiUrl {
	my ($self, $path, $query) = @_;

	my $pathname = '';

	if (!$path) { $path = []; }

	foreach my $p (@{$path}){
		$pathname .= '/' . uri_escape($p);
	}

	#Remove undefined key-value pairs.
	my $querystring = '';
	if ($query) {
		foreach my $key (keys %{$query}) {
			if (!defined $query->{$key}) {
				delete $query->{$key}; next;
			} elsif (ref $query->{$key} eq 'ARRAY'){
				$query->{$key} = join ',', @{$query->{$key}};
			}
			$querystring .=  ($querystring eq '') ?  '?' : '&';
			$querystring .= uri_escape($key) . '='. uri_escape($query->{$key});
		}
	}

	my $reqURL = $ApiProtocol . '//' . $self->{_token} . ':@' . $self->{_apiEndPoint} . '/' . $ApiVersion
			. $pathname . (defined $query ? $querystring : '');

	# print Dumper $reqURL;
	return $reqURL;
}

sub newSearchBuilder {
	my $self = shift;
	return Orchestrate::SearchBuilder->new($self, @_);
}


package Orchestrate::SearchBuilder; {
	use strict;
	use warnings;
	use Carp;
	use Data::Dumper;
	use Scalar::Util 'looks_like_number';
	sub BB {
		print Dumper \@_;
	}

	sub new {
		my $class  = shift;
		my $client  = shift;
		my $options = shift;

		my $self = bless {}, $class;
		$self->{_sortClauses} = [];
		$self->{client}       = $client;
		$self->{write}        = 1;

		if ($options){
			croak "SearchBuilder options must be a hash ref" if ref $options ne 'HASH';
			foreach my $key (keys %{$options}) {
				my $val = $options->{$key};
				my @args = ref $val eq 'ARRAY' ? @{$val} : ($val);
				$self->$key(@args);
			}
		}
		return $self;
	}

	sub collection {
		my ($self, $collection) = @_;
		croak('Collection required.') if !$collection;
		$self->{_collection} = $collection;
		return $self;
	}

	sub limit {
		my ($self, $limit) = @_;
		croak('Limit required.') if !$limit;
		$self->{_limit} = $limit;
		return $self;
	}

	sub offset {
		my ($self, $offset) = @_;
		croak('Offset must be a number.') if !looks_like_number($offset);
		$self->{_offset} = $offset + 0;
		return $self;
	}

	sub sort {
		my ($self, $field, $order) = @_;
		croak 'field required' if !$field;
		croak 'order required' if !$order;

		if ( $field !~ m/^\@path\./ ) {
			# TODO we should NOT be doing this default prefixing!
			# removing will be a breaking change.
			$field = 'value.' . $field;
		}

		my $sortClause = $field . ':' . $order;
		return $self->sortBy($sortClause);
	}

	sub sortBy {
		my $self = shift;
		foreach my $sort (@_) {
			push @{$self->{_sortClauses}}, $sort;
		}
		return $self;
	}

	sub withFields {
		my $self = shift;
		$self->{filterWithFields} = $self->{filterWithFields} || [];
		push @{$self->{filterWithFields}}, @_;
		return $self;
	}

	sub withoutFields {
		my $self = shift;
		$self->{filterWithoutFields} = $self->{filterWithoutFields} || [];
		push @{$self->{filterWithoutFields}}, @_;
		return $self;
	}

	sub kinds {
		my $self  = shift;
		my $kinds = [];
		croak 'At least one kind required.' if @_ < 1;
		foreach my $kind (@_) {
			croak "kinds must be of type 'item', 'event', or 'relationship'"
				unless $kind eq 'event' || $kind eq 'item' || $kind eq 'releationship';
			push @{$kinds}, $kind;
		}

		$self->{_kinds} = $kinds;
		return $self;
	}

	sub query {
		my ($self, $query) = @_;
		croak 'Query required.' if !$query;
		$self->{_query} = $query;
		return $self->_execute('get');
	}


	sub _execute {
		my ($self, $method) = @_;
		my $query = $self->{_query};
		die 'Query required.' if !$query;

		my $pathArgs = [];

		if ( $self->{_collection} ) {
			push @{$pathArgs}, $self->{_collection};
		}

		if ( $self->{_kinds} ) {
			$query = '@path.kind:('
				. join ' ', @{$self->{_kinds}}
				. ') AND ('
				. $self->{_query}
				. ')';
		}

		my $client = $self->{client};

		my $url = $client && $client->generateApiUrl($pathArgs, {
			query          => $query,
			limit          => $self->{_limit},
			offset         => $self->{_offset},
			sort           => $self->_generateSort(),
			aggregate      => $self->{_aggregate},
			with_fields    => $self->{filterWithFields},
			without_fields => $self->{filterWithoutFields}
		});

		$method = '_' . $method;
		return $client->$method($url);
	}

	sub _generateSort {
		my $self = shift;
		if (scalar @{$self->{_sortClauses}} > 0) {
			return join ",", @{$self->{_sortClauses}};
		}
	}
}

1;
