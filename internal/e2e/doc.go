// Package e2e holds end-to-end tests. A notification goes in through the HTTP API and out to a scripted
// provider, passing through PostgreSQL, the relay, RabbitMQ and the workers, all assembled in the test
// process against PostgreSQL, RabbitMQ and Redis containers. The tests use the API only, as a client
// would; they reach into the database only to make time pass (an expired lease).
package e2e
