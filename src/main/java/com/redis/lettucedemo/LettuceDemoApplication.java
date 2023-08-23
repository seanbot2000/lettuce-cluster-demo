package com.redis.lettucedemo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class LettuceDemoApplication implements ApplicationRunner{

	@Value("${keycount}")
	private int keyCount;
	@Value("${uri}")
	private String uri;
	@Value("${mgetsize}")
	private int mgetSize;
	@Value("${batchsize}")
	private int batchSize;

	public static void main(String[] args) {
		SpringApplication.run(LettuceDemoApplication.class, args);

	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.info("Using slot count: {}", SlotHash.SLOT_COUNT);
		runTest();
	}

	private void runTest() {
		RedisClusterClient client = RedisClusterClient.create(uri);
		StatefulRedisClusterConnection<String, String> connection = client.connect();
		log.info("Connected to {}", connection.sync().clusterInfo());
		log.info("Pipelining SETs with {} keys", keyCount);
		for (int batchIndex = 0; batchIndex < (keyCount / batchSize); batchIndex++) {
			int start = batchIndex * batchSize;
			List<String> batch = new ArrayList<>();
			for (int index = 0; index < batchSize; index++) {
				int current = index + start;
				batch.add(key(current));
			}
			writePipeline(connection, batch);
		}
		connection.close();
		String[] keys = keys();
		log.info("Calling MGET with {} keys", keys.length);
		List<KeyValue<String, String>> results = client.connect().sync().mget(keys);
		log.info("MGET successful - {} results", results.size());

		for(KeyValue<String, String> pair: results)
		{
			log.info("K/V Pair - {}, {}", pair.getKey(), pair.getValue());
		}
		connection.close();
		client.shutdown();
	}

	private String[] keys() {
		String[] keys = new String[mgetSize];
		for (int index = 0; index < keys.length; index++) {
			keys[index] = key(index);
		}
		return keys;
	}

	private String key(int index) {
		return "key" + index;
	}

	private void writePipeline(StatefulRedisClusterConnection<String, String> connection, List<String> batch) {
		RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();
		connection.setAutoFlushCommands(false);
		List<RedisFuture<String>> futures = new ArrayList<>();
		for (String key : batch) {
			futures.add(commands.set(key, value(key)));
		}
		connection.flushCommands();
		try {
			boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
					futures.toArray(new RedisFuture[futures.size()]));
			if (result) {
				log.debug("Wrote {} records", batch.size());
			} else {
				for (RedisFuture<?> future : futures) {
					if (future.getError() != null) {
						log.error(future.getError());
					}
				}
			}
		} catch (RedisCommandExecutionException e) {
			log.error("Could not execute commands", e);
		}
	}

	private String value(String key) {
		return key + "-value";
	}
}
