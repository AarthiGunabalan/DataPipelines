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


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
 topic = app.topic("org.chicago.cta.stations", value_type=Station)
# TODO: Define the output Kafka Topic
 out_topic = app.topic("org.chicago.cta.stations.transformed", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "TransformedStation",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def stationevent(stations):
    #
    # TODO: Group By URI
    #       See: https://faust.readthedocs.io/en/latest/userguide/streams.html#group-by-repartition-the-stream
    #
    async for s in stations:
        #
        # TODO: Use the URI as key, and add the number for each click event. Print the updated
        #       entry for each key so you can see how the table is changing.
        #       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
        #
        table[s.] += ce.number
        print(f"{ce.uri}:{uri_summary_table[ce.uri]}")

if __name__ == "__main__":
    app.main()