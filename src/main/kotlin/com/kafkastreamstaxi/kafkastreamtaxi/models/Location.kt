package com.kafkastreamstaxi.kafkastreamtaxi.models


import org.springframework.data.cassandra.core.mapping.UserDefinedType


@UserDefinedType("geo_point")
data class Location(
    val lat: Double,
    val lon: Double
)