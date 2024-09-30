import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.security.Key;
import java.util.Arrays;
import java.util.Properties;

// When student topic or classroom topic modified, check if room is below capacity
public class A4Application 
{

	public static void main(String[] args) throws Exception 
	{
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);
		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		StreamsBuilder builder = new StreamsBuilder();
		
		// Represent a stream of key,value pairs
		KStream<String, String> studentRoomStream = builder.stream(studentTopic);
		KStream<String, String> roomCapacityStream = builder.stream(classroomTopic);
		
		KTable<String, String> studentRoomTable = studentRoomStream.groupByKey().reduce((oldRoom, newRoom) -> newRoom);
		
		KTable<String, Long> roomOccupancyTable = studentRoomTable.groupBy((student, room) -> KeyValue.pair(room, student)).count();
		KTable<String, String> roomCapacityTable = roomCapacityStream.groupByKey().reduce((oldCapacity, newCapacity) -> newCapacity);
		KTable<String, String> roomStateTable = roomOccupancyTable.join(roomCapacityTable, 
			(roomOccupancy, roomCapacity) -> {
				// OVERLIMIT
				if (roomOccupancy > Integer.parseInt(roomCapacity)) {
					return roomOccupancy.toString();
				}
				return "WITHIN";
			}
		);

		roomStateTable.toStream().groupByKey().aggregate(
			// Previous state
			() -> "WITHIN",
			(room, newState, oldState) -> {
				// WITHIN/OK -> WITHIN = WITHIN
				if ((oldState.equals("WITHIN") || oldState.equals("OK")) && newState.equals("WITHIN")) {
					return "WITHIN";
				}
				// OVERLIMIT -> WITHIN = OK
				if (!oldState.equals("WITHIN") && newState.equals("WITHIN")) {
					return "OK";
				}
				// OVERLIMIT
				return newState;
			}
		)
		// Only print OK and OVERLIMIT
		.toStream().filter((room, state) -> !state.equals("WITHIN") && !state.equals(null)).to(outputTopic);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
