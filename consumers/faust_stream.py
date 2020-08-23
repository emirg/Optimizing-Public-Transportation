"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("cta.stations", value_type=Station)
out_topic = app.topic("cta.stations.transformed", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "cta.stations.transformed",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


def transform_station(station):
    if station.red:
        line = 'red'
    elif station.blue:
        line = 'blue'
    else:
        line = 'green'
    transformed_station = TransformedStation(station.station_id, station.station_name, station.order, line)
    return transformed_station


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station(stations):
    async for st in stations:
        transformed_st = transform_station(st)
        table[transformed_st.station_id] = transformed_st


if __name__ == "__main__":
    app.main()
