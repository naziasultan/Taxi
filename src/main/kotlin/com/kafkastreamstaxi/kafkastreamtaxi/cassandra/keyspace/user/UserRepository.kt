package com.kafkastreamtaxi.kafkastreamtaxi.cassandra.keyspace.user


import com.kafkastreamstaxi.kafkastreamtaxi.models.UserEvent
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository


@Repository
interface UserRepository: CassandraRepository<UserEvent, String>