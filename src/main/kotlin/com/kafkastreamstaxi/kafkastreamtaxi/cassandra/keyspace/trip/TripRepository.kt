package com.kafkastreamtaxi.kafkastreamtaxi.cassandra.keyspace.trip


import com.kafkastreamstaxi.kafkastreamtaxi.models.TripEvent
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository

@Repository
interface TripRepository: CassandraRepository<TripEvent, String>
